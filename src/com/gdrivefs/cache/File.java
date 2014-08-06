package com.gdrivefs.cache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
	Date metadataAsOfDate;
	Date childrenAsOfDate;
	String title;
	String fileMd5;
	URL downloadUrl;
	String mimeType;
	Long size;
	Timestamp modifiedTime;
	List<File> children;
	List<File> parents;
	
	static final Executor worker = Executors.newSingleThreadExecutor();
	
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
		mimeType = row.getString("MIMETYPE");
		size = row.getLong("SIZE");
		modifiedTime = row.getTimestamp("MTIME");
		metadataAsOfDate = row.getTimestamp("METADATAREFRESHED");
		childrenAsOfDate = row.getTimestamp("CHILDRENREFRESHED");
		fileMd5 = row.getString("MD5HEX");
		downloadUrl = row.getString("DOWNLOADURL") != null ? new URL(row.getString("DOWNLOADURL")) : null;
	}
	
	public void refresh() throws IOException
	{
		Date asof = new Date();
		com.google.api.services.drive.model.File metadata = drive.getRemote().files().get(id).execute();
		refresh(metadata, asof);
	}
	
	String getId()
	{
		return id;
	}
	
	private void clearChildrenCache()
	{
		drive.getDatabase().execute("DELETE FROM RELATIONSHIPS WHERE PARENT=?", id);
		children = null;
	}
	
	/**
	 * Refresh this file if the file hasn't been refreshed recently (within threshold).
	 */
	public void considerSynchronousDirectoryRefresh(long threshold, TimeUnit units) throws IOException
	{
		if(!isDirectory()) throw new Error("This method must not be called on non-directories; called on "+id+"; consider calling method on this file's parent");

		Date updateThreshold = new Date(System.currentTimeMillis()-units.toMillis(threshold));
		if(getMetadataDate().before(updateThreshold)) refresh();
		if(getChildrenDate().before(updateThreshold)) updateChildrenFromRemote();
	}
	
	/**
	 * Asynchronously refresh this file if the file hasn't been refreshed recently (within threshold).
	 */
	public void considerAsyncDirectoryRefresh(final long threshold, final TimeUnit units)
	{
		// Sanity check that we are in fact calling this method on a directory.
		// We do the mimecheck ourselves instead of calling isDirectory() to avoid potentially doing I/O on an asynchronous code path
		if(Boolean.FALSE.equals(isDirectoryNoIO()))
			throw new Error("This method must not be called on non-directories; called on "+id+"; consider calling method on this file's parent");
		
		worker.execute(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					considerSynchronousDirectoryRefresh(threshold, units);
				}
				catch(IOException e)
				{
					throw new RuntimeException();
				}
			}
		});
	}

	public List<File> getChildren(String title) throws IOException
	{
		List<File> children = getChildrenInternal(title);
		
		if(!children.isEmpty()) return children;
		
		// If we get to this point, it means the user was looking for a specific child, but that child was not found.
		// Sometimes this implies the user has some knowledge about an update that we're as-of-yet unaware-of, and are trying to access that file.
		// We can use this information to be more aggressive about doing a fetch, but since 404's are a perfectly valid FS outcome, we don't want to block every time.
		// If the user was correct, and there was an update available, it means this heuristic is working, so we continue to use the heuristic.
		// If the user was wrong, we back off exponentially, thereby avoiding a heuristic that isn't working for this user.
		
		// Do not retry if we're backing off, and do not retry if the last attempt was less than a second ago.
		if(drive.nextFileNotFoundAttempt > System.currentTimeMillis()) return children;
		if(getChildrenDate().after(new Date(System.currentTimeMillis()-1000))) return children;

		// Perform refresh
		considerSynchronousDirectoryRefresh(1, TimeUnit.SECONDS);
		
		// Get a new list of children
		children = getChildrenInternal(title);
		
		// If the children changed, we were successful.  Else, heuristic failed so we backoff.
		if(children.size() > 0){ System.out.println("resetting counter"); drive.fileNotFoundBackoff.reset(); }
		else {long timeout = drive.fileNotFoundBackoff.nextBackOffMillis(); System.out.println("new timeout: "+timeout); drive.nextFileNotFoundAttempt = System.currentTimeMillis()+timeout; }

		return children;
	}
	
	private List<File> getChildrenInternal(String title) throws IOException
	{
		List<File> children = new ArrayList<File>();
		for(File child : getChildren())
			if(title.equals(child.getTitle()))
				children.add(child);
		return children;
	}
	
	public List<File> getChildren() throws IOException
	{
		if(children != null)
		{
			considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
			for(File child : children)
				if(child.isDirectory())
					child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
			return children;
		}
		
		children = new ArrayList<File>();
		if(!isDirectory()) return children;
		
		try
		{
			List<String> files = drive.getDatabase().getStrings("SELECT CHILD FROM RELATIONSHIPS WHERE PARENT=?", id);
			for(String file : files) children.add(drive.getFile(file));
			if(!children.isEmpty())
			{
				considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
				for(File child : children)
					if(child.isDirectory())
						child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
				return children;
			}
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
		
		updateChildrenFromRemote();
		considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
		for(File child : children)
			if(child.isDirectory())
				child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
		
		return children;
	}
	
	void updateChildrenFromRemote()
	{
		List<File> children = new ArrayList<File>();
		try
		{
			Date childrenUpdateDate = new Date();
			com.google.api.services.drive.Drive.Files.List lst = drive.getRemote().files().list().setQ("'"+id+"' in parents and trashed=false");
			do
			{
				FileList files = lst.execute();
				drive.getDatabase().execute("DELETE FROM RELATIONSHIPS WHERE PARENT=?", id);
				for(com.google.api.services.drive.model.File child : files.getItems())
				{
					File f = drive.getFile(child.getId());
					f.refresh(child, childrenUpdateDate);
					children.add(f);
				}
				lst.setPageToken(files.getNextPageToken());
			} while(lst.getPageToken() != null && lst.getPageToken().length() > 0);
			
			this.children = children;
			drive.getDatabase().execute("UPDATE FILES SET CHILDRENREFRESHED = ? WHERE ID = ?", childrenUpdateDate, id);
			this.childrenAsOfDate = childrenUpdateDate;
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}
	
	synchronized void refresh(com.google.api.services.drive.model.File file, Date asof) throws IOException
	{
		if(!id.equals(file.getId())) throw new Error("Attempting to refresh "+id+" using remote file "+file.getId());
		
		GregorianCalendar nextUpdateTimestamp = new GregorianCalendar();
		nextUpdateTimestamp.add(Calendar.DAY_OF_YEAR, 1);
		
		drive.getDatabase().execute("DELETE FROM FILES WHERE ID=?", file.getId());
		drive.getDatabase().execute("INSERT INTO FILES(ID, TITLE, MIMETYPE, MD5HEX, SIZE, MTIME, DOWNLOADURL, METADATAREFRESHED, CHILDRENREFRESHED) VALUES(?,?,?,?,?,?,?,?,?)", file.getId(), file.getTitle(), file.getMimeType(), file.getMd5Checksum(), file.getQuotaBytesUsed(), new Date(file.getModifiedDate().getValue()), file.getDownloadUrl(), asof, childrenAsOfDate != null ? childrenAsOfDate : new Date(1));
		
		drive.getDatabase().execute("DELETE FROM RELATIONSHIPS WHERE CHILD=?", file.getId());
		if(file.getParents().isEmpty())
			drive.getDatabase().execute("INSERT INTO RELATIONSHIPS(PARENT, CHILD) VALUES(?,?)", null, file.getId());
		for(ParentReference parent : file.getParents())
			drive.getDatabase().execute("INSERT INTO RELATIONSHIPS(PARENT, CHILD) VALUES(?,?)", parent.getId(), file.getId());

		readBasicMetadata();
	}
	
	public String getTitle() throws IOException
	{
		if(title == null) readBasicMetadata();
		
		// Somebody appears to be watching this file; let's keep it up-to-date
		if(isDirectory()) considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
		else if(parents != null) for(File file : parents) file.considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
		
		return title;
	}
	
	public String getMimeType() throws IOException
	{
		if(mimeType == null) readBasicMetadata();
		return mimeType;
	}
	
	Boolean isDirectoryNoIO()
	{
		if(mimeType == null) return null;
		return MIME_FOLDER.equals(mimeType);
	}
	
	public boolean isDirectory() throws IOException
	{
		if(mimeType == null) readBasicMetadata();
		return isDirectoryNoIO();
	}
	
	public long getSize() throws IOException
	{
		if(size == null) readBasicMetadata();
		return size;
	}
	
	public Date getMetadataDate() throws IOException
	{
		if(metadataAsOfDate == null) readBasicMetadata();
		return metadataAsOfDate;
	}
	
	public Date getChildrenDate() throws IOException
	{
		if(childrenAsOfDate == null) updateChildrenFromRemote();
		return childrenAsOfDate;
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
		
		// Somebody appears to be watching this file; let's keep it up-to-date
		for(File file : getParents()) file.considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
		
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
		File newDirectory = drive.getFile(newRemoteDirectory.getId());
		
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

	public void addChild(File child) throws IOException
	{
		if(!isDirectory()) throw new UnsupportedOperationException("Can not add child to non-directory");

		ParentReference newParent = new ParentReference();
		newParent.setId(getId());
		drive.getRemote().parents().insert(child.getId(), newParent).execute();

		clearChildrenCache();
	}

	public void removeChild(File child) throws IOException
	{
		if(child.getParents().size() <= 1) throw new UnsupportedOperationException("Child must have at least one parent");
		drive.getRemote().parents().delete(child.getId(), getId()).execute();
		clearChildrenCache();
	}

	public void trash() throws IOException
	{
		if(this.equals(drive.getRoot())) throw new UnsupportedOperationException("Can not trash the root node");
		List<File> parents = getParents();
		drive.getRemote().files().trash(id).execute();

		// Clear the parent's cache of children
		for(File parent : parents)
			parent.clearChildrenCache();
	}
    
    @Override
    public String toString()
    {
    	return "File("+id+")";
    }
    
}
