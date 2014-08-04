package com.gdrivefs.cache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.IOUtils;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;
import com.jimsproch.sql.DatabaseConnectionException;
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
	String fileMd5;
	URL downloadUrl;
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
	
	private void readBasicMetadata(DatabaseRow row) throws MalformedURLException
	{
		title = row.getString("TITLE");
		isDirectory = row.getString("MD5HEX") == null;
		size = row.getLong("SIZE");
		modifiedTime = row.getTimestamp("MTIME");
		fileMd5 = row.getString("MD5HEX");
		downloadUrl = row.getString("DOWNLOADURL") != null ? new URL(row.getString("DOWNLOADURL")) : null;
	}
	
	public void refresh() throws IOException
	{
		observeFile(drive, drive.getRemote().files().get(id).execute());
		readBasicMetadata();
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
	
	public URL getDownloadUrl() throws IOException
	{
		if(downloadUrl == null) readBasicMetadata();
		return downloadUrl;
	}
	
	public String getMd5Checksum() throws IOException
	{
		if(isDirectory()) return null;
		if(fileMd5 == null) readBasicMetadata();
		return fileMd5;
	}
	
	private static final String MIME_FOLDER = "application/vnd.google-apps.folder";
	public File mkdir(String name) throws IOException
	{
		System.out.println(name);
		com.google.api.services.drive.model.File newRemoteDirectory = new com.google.api.services.drive.model.File();
		newRemoteDirectory.setTitle(name);
		newRemoteDirectory.setMimeType(MIME_FOLDER);
		newRemoteDirectory.setParents(Arrays.asList(new ParentReference().setId(id)));
		
		newRemoteDirectory = drive.getRemote().files().insert(newRemoteDirectory).execute();
		File newDirectory = drive.getFile(newRemoteDirectory);
		
		clearChildrenCache();

	    return newDirectory;
	}
		
	
	
	
    public byte[] read(final long size, final long offset) throws DatabaseConnectionException, IOException
    {
		byte[] data = getBytesByAnyMeans(offset, offset + size);
		if(data.length != size) throw new Error("expected: " + size + " actual: " + data.length + " " + new String(data));
		return data;
	}
    
    
    
    protected byte[] downloadFragment(long startPosition, long endPosition) throws IOException
	{
    	System.out.println("Downloading "+fileMd5+" "+startPosition+" "+endPosition);
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition == endPosition) return new byte[0];
            
        ByteArrayOutputStream out = new ByteArrayOutputStream();

		HttpRequestFactory requestFactory = drive.transport.createRequestFactory(drive.getRemote().getRequestFactory().getInitializer());

		HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(getDownloadUrl()));
		request.getHeaders().setRange("bytes=" + (startPosition) + "-" + (endPosition - 1));
		HttpResponse response = request.execute();
		try
		{
			IOUtils.copy(response.getContent(), out);
		}
		finally
		{
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
    	String chunkMd5 = DigestUtils.md5Hex(data);
		drive.getDatabase().execute("INSERT INTO FRAGMENTS(FILEMD5, CHUNKMD5, STARTBYTE, ENDBYTE) VALUES(?,?,?,?)", fileMd5, chunkMd5, position, position + data.length);
		FileUtils.writeByteArrayToFile(getCacheFile(chunkMd5), data);
    }
    
	static java.io.File getCacheFile(String chunkMd5)
	{
		java.io.File cacheFile = new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "cache");
		for(byte c : chunkMd5.getBytes())
			cacheFile = new java.io.File(cacheFile, Character.toString((char) c));
		cacheFile = new java.io.File(cacheFile, chunkMd5);
		return cacheFile;
	}
	
	public byte[] getBytesByAnyMeans(long start, long end) throws DatabaseConnectionException, IOException
	{
		byte[] output = new byte[(int)(end-start)];
		List<DatabaseRow> fragments = drive.getDatabase().getRows("SELECT * FROM FRAGMENTS WHERE FILEMD5=? AND STARTBYTE < ? AND ENDBYTE > ? ORDER BY STARTBYTE ASC", getMd5Checksum(), end, start);

		long currentPosition = start;
		for(DatabaseRow fragment : fragments)
		{
			int startbyte = fragment.getInteger("STARTBYTE");
			int endbyte = fragment.getInteger("ENDBYTE");
			String chunkMd5 = fragment.getString("CHUNKMD5");
			java.io.File cachedChunkFile = File.getCacheFile(chunkMd5);
			
			if(!cachedChunkFile.exists() || cachedChunkFile.length() != endbyte-startbyte)
			{
				drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE CHUNKMD5=?", chunkMd5);
				continue;
			}
			
			// If the fragment starts after the byte we need, download the piece we still need
			if(startbyte > currentPosition)
			{
				byte[] data = downloadFragment(currentPosition, Math.min(startbyte, end));
				System.arraycopy(data, 0, output, (int)(currentPosition-start), data.length);
				currentPosition += data.length;
			}

			// Consume the fragment
			int copyStart = (int)(currentPosition-startbyte);
			System.out.println("endbyte"+endbyte+" "+"currentPosition"+currentPosition+" "+"startbyte"+startbyte+" "+"readend"+end);
			int copyEnd = Math.min((int)(endbyte-startbyte), (int)(end-startbyte));
			System.out.println("readstart"+start+" "+"readend"+end+" "+"copyStart"+copyStart+" "+"copyEnd"+copyEnd+" "+"destpos"+(currentPosition-start)+" "+"length"+(copyEnd-copyStart)+" ");
			System.out.println("outputlength"+output.length+" "+"fragmentlength"+FileUtils.readFileToByteArray(cachedChunkFile).length);
			System.arraycopy(FileUtils.readFileToByteArray(cachedChunkFile), copyStart, output, (int)(currentPosition-start), copyEnd-copyStart);
			currentPosition += copyEnd-copyStart;
		}
		
		byte[] data = downloadFragment(currentPosition, end);
		System.arraycopy(data, 0, output, (int)(currentPosition-start), data.length);
		currentPosition += data.length;
		
		return output;
	}
    
    public void setTitle(String title) throws IOException
    {
        // First retrieve the file from the API.
    	com.google.api.services.drive.model.File file = drive.getRemote().files().get(id).execute();
    	
    	if(file.getTitle().equals(title)) return;

        // File's new metadata.
        file.setTitle(title);

        // Send the request to the API.
        drive.getRemote().files().update(id, file).execute();
        
        refresh();
        
        for(ParentReference ref : file.getParents())
        	drive.getFile(ref.getId()).clearChildrenCache();
    }

    public List<File> getParents() throws IOException
    {
    	if(parents == null) parents = new ArrayList<File>();
    	for(ParentReference parent : drive.getRemote().files().get(id).execute().getParents())
    		parents.add(drive.getFile(parent.getId()));
    	return parents;
    }
    
    public void addParent(File parent) throws IOException
	{
		ParentReference newParent = new ParentReference();
		newParent.setId(parent.getId());
		drive.getRemote().parents().insert(getId(), newParent).execute();
		parent.clearChildrenCache();
    }
    
    public void removeParent(File parent) throws IOException
    {
    	drive.getRemote().parents().delete(getId(), parent.getId()).execute();
    	parent.clearChildrenCache();
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
