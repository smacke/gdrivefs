package com.gdrivefs.cache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.IOUtils;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;
import com.jimsproch.sql.DatabaseRow;

/**
 * File represents a particular remote file (as represented by Google's file ID), but provides a clean interface for performing reads and writes
 * through the localhost cache layer.
 */
public class File
{
	String id;
	Drive drive;
	
	// Cache (null indicates field must be fetched from db)
	String title;
	Boolean isDirectory;
	Long size;
	Timestamp modifiedTime;
	List<File> children;
	List<File> parents;
	
	/** Returns a new file representing the root of the drive **/
	File(Drive drive) throws IOException
	{
		this(drive, getRootId(drive));
	}
	
	/** Returns a new file representing the specified remote file **/
	File(Drive drive, com.google.api.services.drive.model.File file) throws IOException
	{
		this.drive = drive;
		this.id = file.getId();
		
		observeFile(drive, file);
	}
	
	File(Drive drive, String id) throws IOException
	{
		this.drive = drive;
		this.id = id;
	}
	
	/** Reads basic metadata from the cache, throwing an exception if the file metadata isn't in our database **/
	private void readBasicMetadata() throws IOException
	{
		List<DatabaseRow> rows = drive.getDatabase().getRows("SELECT * FROM FILES WHERE ID=?", id);
		if(rows.size() == 0)
		{
			refresh();
			rows = drive.getDatabase().getRows("SELECT * FROM FILES WHERE ID=?", id);
		}
		readBasicMetadata(rows.get(0));
	}
	
	private void readBasicMetadata(DatabaseRow row)
	{
		title = row.getString("TITLE");
		isDirectory = row.getString("MD5HEX") == null;
		size = row.getLong("SIZE");
		modifiedTime = row.getTimestamp("MTIME");
	}
	
	public void refresh() throws IOException
	{
		observeFile(drive, drive.getRemote().files().get(id).execute());
	}
	
	String getId()
	{
		return id;
	}
	
	private static String getRootId(Drive drive) throws IOException
	{
		try
		{
			return drive.getDatabase().getString("SELECT ID FROM FILES WHERE PARENT IS NULL");
		}
		catch(NoSuchElementException e)
		{
			String id = drive.getRemote().about().get().execute().getRootFolderId();
			com.google.api.services.drive.model.File file = drive.getRemote().files().get(id).execute();
			observeFile(drive, file);
			return id;
		}
	}
	
	private void clearChildrenCache()
	{
		drive.getDatabase().execute("DELETE FROM FILES WHERE PARENT=?", id);
		children = null;
	}
	
	public List<File> getChildren() throws IOException
	{
		if(children != null) return children;
		
		children = new ArrayList<File>();
		
		if(!isDirectory()) return children;
		
		try
		{
			List<String> files = drive.getDatabase().getStrings("SELECT ID FROM FILES WHERE PARENT=?", id);
			for(String file : files) children.add(drive.getFile(file));
			if(!children.isEmpty()) return children;
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
		
		try
		{
			System.out.println("Fetchild child ren from google: "+id+" "+getTitle());
			com.google.api.services.drive.Drive.Files.List lst = drive.getRemote().files().list().setQ("'"+id+"' in parents and trashed=false");

			do
			{
				try
				{
					FileList files = lst.execute();
					for(com.google.api.services.drive.model.File child : files.getItems()) children.add(drive.getFile(child));
					lst.setPageToken(files.getNextPageToken());
				}
				catch(IOException e)
				{
					System.out.println("An error occurred: " + e);
					lst.setPageToken(null);
				}
			} while(lst.getPageToken() != null && lst.getPageToken().length() > 0);
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
		return children;
	}
	
	static void observeFile(Drive drive, com.google.api.services.drive.model.File file)
	{
		drive.getDatabase().execute("DELETE FROM FILES WHERE ID=?", file.getId());
		if(file.getParents().isEmpty())
			drive.getDatabase().execute("INSERT INTO FILES(ID, PARENT, TITLE, MD5HEX, SIZE, MTIME, DOWNLOADURL) VALUES(?,?,?,?,?,?,?)", file.getId(), null, file.getTitle(), file.getMd5Checksum(), file.getQuotaBytesUsed(), new Date(file.getModifiedDate().getValue()), file.getDownloadUrl());
		for(ParentReference parent : file.getParents())
			drive.getDatabase().execute("INSERT INTO FILES(ID, PARENT, TITLE, MD5HEX, SIZE, MTIME, DOWNLOADURL) VALUES(?,?,?,?,?,?,?)", file.getId(), parent.getId(), file.getTitle(), file.getMd5Checksum(), file.getQuotaBytesUsed(), new Date(file.getModifiedDate().getValue()), file.getDownloadUrl());
	}
	
	public String getTitle() throws IOException
	{
		if(title == null) readBasicMetadata();
		return title;
	}
	
	public boolean isDirectory() throws IOException
	{
		if(isDirectory == null) readBasicMetadata();
		return isDirectory;
	}
	
	public long getSize() throws IOException
	{
		if(size == null) readBasicMetadata();
		return size;
	}
	
	public Timestamp getModified() throws IOException
	{
		if(modifiedTime == null) readBasicMetadata();
		return modifiedTime;
	}
	
	private static final String MIME_FOLDER = "application/vnd.google-apps.folder";
	public File mkdir(String name) throws IOException
	{
		System.out.println(name);
		com.google.api.services.drive.model.File directory = new com.google.api.services.drive.model.File();
		directory.setTitle(name);
		directory.setMimeType(MIME_FOLDER);
		directory.setParents(Arrays.asList(new ParentReference().setId(id)));
		
		directory = drive.getRemote().files().insert(directory).execute();
		children = null;

	    return drive.getFile(directory);
	}
		
	
	
	
    public byte[] read(final long size, final long offset)
    {
            com.google.api.services.drive.model.File remoteFile;
            try
            {
                    remoteFile = drive.getRemote().files().get(id).execute();
                    String fileMd5 = remoteFile.getMd5Checksum();
                    
                    byte[] data = downloadFragment(fileMd5, (int)offset, (int)(offset+size));
                    if(data.length != size) throw new Error("expected: "+size+" actual: "+data.length+" "+new String(data));
                    return data;
                    /*
                     * 
                    int fragmentPointer = (int)(offset%FRAGMENT_SIZE);
                    int outputPointer = 0;
                    
                    while(outputPointer != size)
                    {
                            byte[] fragmentData = getFragment(fileMd5, (int)((offset+outputPointer)/FRAGMENT_SIZE));
                            System.arraycopy(fragmentData, fragmentPointer, output, outputPointer, (int)Math.min(size-outputPointer, FRAGMENT_SIZE)-fragmentPointer);
                            outputPointer = outputPointer+(int)(Math.min(size-outputPointer, FRAGMENT_SIZE)-fragmentPointer);
                            fragmentPointer = 0;
                    }
                    return output;
                    */
            }
            catch(IOException e)
            {
                    e.printStackTrace();
                    throw new RuntimeException(e);
            }
    }

    
    
    
    
    
    
    protected byte[] downloadFragment(String fileMd5, int startPosition, int endPosition) throws IOException
    {
            if(startPosition > endPosition) throw new IllegalArgumentException("startPosition ("+startPosition+") must not be greater than endPosition ("+endPosition+")");
            if(startPosition > endPosition) throw new IllegalArgumentException("startPosition ("+startPosition+") must not be greater than endPosition ("+endPosition+")");
            if(startPosition == endPosition) return new byte[0];
            
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        HttpRequestFactory requestFactory = drive.transport.createRequestFactory(drive.getRemote().getRequestFactory().getInitializer());

                // prepare the GET request
                HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(drive.getDatabase().getStrings("SELECT DOWNLOADURL FROM FILES WHERE MD5HEX=?", fileMd5).get(0)));
                // set Range header (if necessary)
                  request.getHeaders().setRange("bytes="+(startPosition)+"-"+(endPosition-1));
                HttpResponse response = request.execute();
                try {
                  IOUtils.copy(response.getContent(), out);
                        System.out.println(response);
                } finally {
                  response.disconnect();
                }
        
        byte[] bytes = out.toByteArray();
        
        storeFragment(fileMd5, startPosition, bytes);
        
        return bytes;
    }

    
    
    
    
    
    protected byte[] readFragment(String fileMd5, long startPosition, long endPosition, String chunkMd5) throws IOException
    {
            if(startPosition >= endPosition) throw new Error("EndPosition ("+endPosition+") should not be greater than StartPosition ("+startPosition+")");
            RandomAccessFile file = new RandomAccessFile(new java.io.File("/home/kyle/.googlefs/cache/"+fileMd5), "r");
            try
            {
                    file.seek(startPosition);
                    byte[] data = new byte[(int)(endPosition-startPosition)];
                    file.read(data);
                    
                    // If the fragment was found and is correct, return it
                    if(DigestUtils.md5Hex(data).equals(chunkMd5)) return data;
                    
                    // Something went horribly wrong, delete the fragment record and return not-found
                    drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE fileMd5=? AND chunkMd5=?", fileMd5, chunkMd5);
                    throw new NoSuchElementException("Could not load chunk "+fileMd5+"."+chunkMd5+"("+startPosition+"-"+endPosition+")");
            }
            finally
            {
                    file.close();
            }
    }
    

    
    protected void storeFragment(String fileMd5, long position, byte[] data) throws IOException
    {
    	drive.getDatabase().execute("INSERT INTO FRAGMENTS(FILEMD5, CHUNKMD5, STARTBYTE, ENDBYTE) VALUES(?,?,?,?)", fileMd5, DigestUtils.md5Hex(data), position, position+data.length);
            RandomAccessFile file = new RandomAccessFile(new java.io.File(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "cache"), fileMd5), "rw");
            file.seek(position);
            file.write(data);
            file.close();
    }

    public List<File> getParents() throws IOException
    {
    	if(parents == null) parents = new ArrayList<File>();
    	for(ParentReference parent : drive.getRemote().files().get(id).execute().getParents())
    		parents.add(drive.getFile(parent.getId()));
    	return parents;
    }
    
    public void delete() throws IOException
    {
    	List<File> parents = getParents();
    	drive.getRemote().files().delete(id).execute();
    	
    	// Clear the parent's cache of children
    	for(File parent : parents) parent.clearChildrenCache();
    }
    
    @Override
    public String toString()
    {
    	return "File("+id+")";
    }
    
}
