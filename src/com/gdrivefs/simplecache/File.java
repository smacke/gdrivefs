package com.gdrivefs.simplecache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.IOUtils;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;
import com.google.common.collect.ImmutableList;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.DatabaseConnectionException;
import com.jimsproch.sql.DatabaseRow;
import com.jimsproch.sql.Transaction;
import com.thoughtworks.xstream.XStream;

/**
 * File represents a particular remote file (as represented by Google's file ID), but provides a clean interface for performing reads and writes
 * through the localhost cache layer.
 */
public class File
{
	String googleFileId;
	UUID localFileId;
	Drive drive;
	
	ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	// Cache (null indicates field must be fetched from db)
	Date metadataAsOfDate;
	Date childrenAsOfDate;
	Date parentsAsOfDate;
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
		this.googleFileId = id;
	}
	
	File(Drive drive, UUID id) throws IOException
	{
		this.drive = drive;
		this.localFileId = id;
	}
	
	/** Reads basic metadata from the cache, throwing an exception if the file metadata isn't in our database **/
	private void readBasicMetadata() throws IOException
	{
		if(lock.getReadLockCount() == 0 && !lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
		List<DatabaseRow> rows = drive.getDatabase().getRows("SELECT * FROM FILES WHERE ID=?", googleFileId);
		if(rows.size() == 0)
		{
			Date asof = new Date();
			com.google.api.services.drive.model.File metadata = drive.getRemote().files().get(googleFileId).execute();
			try { refresh(metadata, asof); }
			catch(SQLException e) {}
			rows = drive.getDatabase().getRows("SELECT * FROM FILES WHERE ID=?", googleFileId);
		}
		readBasicMetadata(rows.get(0));
		
		playLogOnMetadata();
	}
	
	void playLogOnMetadata() throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG WHERE CATEGORY='metadata' ORDER BY ID ASC"))
			playOnMetadata(row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}
	
	static void playLogOnRemote(Drive drive) throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG ORDER BY ID ASC"))
		{
			playOnRemote(drive, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
			drive.getDatabase().execute("DELETE FROM UPDATELOG WHERE ID=?", row.getInteger("ID"));
		}
	}
	
	private void readBasicMetadata(DatabaseRow row) throws MalformedURLException
	{
		title = row.getString("TITLE");
		mimeType = row.getString("MIMETYPE");
		size = row.getLong("SIZE");
		modifiedTime = row.getTimestamp("MTIME");
		metadataAsOfDate = row.getTimestamp("METADATAREFRESHED");
		childrenAsOfDate = row.getTimestamp("CHILDRENREFRESHED");
		parentsAsOfDate = row.getTimestamp("PARENTSREFRESHED");
		localFileId = row.getUuid("LOCALID");
		fileMd5 = row.getString("MD5HEX");
		downloadUrl = row.getString("DOWNLOADURL") != null ? new URL(row.getString("DOWNLOADURL")) : null;
	}
	
	public void refresh() throws IOException
	{
		acquireWrite();
		try
		{
			Date asof = new Date();
			com.google.api.services.drive.model.File metadata = drive.getRemote().files().get(googleFileId).execute();
			try
			{
				refresh(metadata, asof);
				if(isDirectory())
				{
					childrenAsOfDate = null;
					parentsAsOfDate = null;
					drive.getDatabase().execute("UPDATE FILES SET CHILDRENREFRESHED = NULL, PARENTSREFRESHED = NULL WHERE ID=?", googleFileId);
					final File f = this;
					worker.execute(new Runnable()
					{
						@Override
						public void run()
						{
							try
							{
								if(f.isDirectory()) f.getChildren();
								f.getParents();
							}
							catch(IOException e)
							{
								throw new RuntimeException(e);
							}
						}
					});
				}
			}
			catch(SQLException e)
			{
				throw new IOException(e);
			}
		}
		finally
		{
			releaseWrite();
		}
	}
	
	public String getId()
	{
		return googleFileId;
	}
	
	private synchronized void clearChildrenCache() throws IOException
	{
		drive.getDatabase().execute("UPDATE FILES SET CHILDRENREFRESHED = NULL WHERE ID=?", googleFileId);
		children = null;
		childrenAsOfDate = null;
	}
	
	/**
	 * Refresh this file if the file hasn't been refreshed recently (within threshold).
	 */
	public void considerSynchronousDirectoryRefresh(long threshold, TimeUnit units) throws IOException
	{
		acquireWrite();
		try
		{
			if(!isDirectory()) throw new Error("This method must not be called on non-directories; called on "+googleFileId+"; consider calling method on this file's parent");
	
			Date updateThreshold = new Date(System.currentTimeMillis()-units.toMillis(threshold));
			if(getMetadataDate().before(updateThreshold)) refresh();
			if(childrenAsOfDate != null && childrenAsOfDate.before(updateThreshold)) refresh();
		}
		finally
		{
			releaseWrite();
		}
	}
	
	/**
	 * Asynchronously refresh this file if the file hasn't been refreshed recently (within threshold).
	 */
	public void considerAsyncDirectoryRefresh(final long threshold, final TimeUnit units)
	{
		// Sanity check that we are in fact calling this method on a directory.
		// We do the mimecheck ourselves instead of calling isDirectory() to avoid potentially doing I/O on an asynchronous code path
		if(Boolean.FALSE.equals(isDirectoryNoIO()))
			throw new Error("This method must not be called on non-directories; called on "+googleFileId+"; consider calling method on this file's parent");
		
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
					throw new RuntimeException(e);
				}
			}
		});
	}
	
	public List<File> getChildren(String title) throws IOException
	{
		List<File> children = new ArrayList<File>();
		for(File child : getChildren())
			if(title.equals(child.getTitle()))
				children.add(child);
		return children;
	}
	
	public List<File> getChildren() throws IOException
	{
		acquireRead();
		try
		{
			if(children != null)
			{
				considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
				for(File child : children)
					if(child.isDirectory())
						child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
				return ImmutableList.copyOf(children);
			}
			
			if(!isDirectory()) return children;
			
			try
			{
				if(childrenAsOfDate == null && metadataAsOfDate == null) readBasicMetadata(); // See if maybe it's just not in the memory cache (DB faster than Google)
				if(childrenAsOfDate != null)
				{
					List<File> children = drive.getDatabase().execute(new Transaction<List<File>>()
					{
						@Override
						public List<File> run(Database db) throws Throwable
						{
							List<File> children = new ArrayList<File>();
							List<String> files = drive.getDatabase().getStrings("SELECT CHILD FROM RELATIONSHIPS WHERE PARENT=?", googleFileId);
							for(String file : files)
								children.add(drive.getFile(file));
							if(!children.isEmpty())
							{
								considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
								for(File child : children)
									if(child.isDirectory()) child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
								return children;
							}
							return null;
						}
					});
					playLogOnChildrenList(children);
					this.children = children;
					return ImmutableList.copyOf(children);
				}
			}
			catch(SQLException e1)
			{
				e1.printStackTrace();
				throw new RuntimeException(e1);
			}
			
			updateChildrenFromRemote();
			considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
			for(File child : children)
				if(child.isDirectory())
					child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
			
			return ImmutableList.copyOf(children);
		}
		finally
		{
			releaseRead();
		}
	}
	
	private void updateChildrenFromRemote()
	{
		if(children != null && !lock.isWriteLockedByCurrentThread()) throw new Error("Children cached, so doibng a remote fetch require a write lock!");

		try
		{
			final Date childrenUpdateDate = new Date();
			com.google.api.services.drive.Drive.Files.List lst = drive.getRemote().files().list().setQ("'"+googleFileId+"' in parents and trashed=false");
			final List<com.google.api.services.drive.model.File> googleChildren = new ArrayList<com.google.api.services.drive.model.File>();
			do
			{
				FileList files = lst.execute();
				for(com.google.api.services.drive.model.File child : files.getItems())
					googleChildren.add(child);
				lst.setPageToken(files.getNextPageToken());
			} while(lst.getPageToken() != null && lst.getPageToken().length() > 0);

			List<File> children = drive.getDatabase().execute(new Transaction<List<File>>()
			{
				@Override
				public List<File> run(Database arg0) throws Throwable
				{
					List<File> children = new ArrayList<File>();
					drive.getDatabase().execute("DELETE FROM RELATIONSHIPS WHERE PARENT=?", googleFileId);
					for(com.google.api.services.drive.model.File child : googleChildren)
					{
						children.add(drive.getFile(child.getId()));
						drive.getDatabase().execute("INSERT INTO RELATIONSHIPS(PARENT, CHILD) VALUES(?,?)", getId(), child.getId());
					}
					return children;
				}
			});
			playLogOnChildrenList(children);
			this.children = children;
			
			// We happen to have metadata for our children available, so we should have the worker update them at earliest convinence
			for(final com.google.api.services.drive.model.File child : googleChildren)
				worker.execute(new Runnable(){
					@Override
					public void run()
					{
						try
						{
							File f = drive.getFile(child.getId());
							f.acquireWrite();
							try { f.refresh(child, childrenUpdateDate); }
							finally { f.releaseWrite(); }
						}
						catch(IOException e)
						{
							throw new RuntimeException(e);
						}
						catch(SQLException e)
						{
							throw new RuntimeException(e);
						}
					}});
			
			// Sketchy as hell, but I can't think of any way that updating the timestamp could cause a fatal race condition
			drive.getDatabase().execute("UPDATE FILES SET CHILDRENREFRESHED = ? WHERE ID = ?", childrenUpdateDate, googleFileId);
			this.childrenAsOfDate = childrenUpdateDate;
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	private void refresh(final com.google.api.services.drive.model.File file, final Date asof) throws IOException, SQLException
	{
		if(lock.getReadLockCount() == 0 && !lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
		
		GregorianCalendar nextUpdateTimestamp = new GregorianCalendar();
		nextUpdateTimestamp.add(Calendar.DAY_OF_YEAR, 1);

		drive.getDatabase().execute(new Transaction<Void>()
		{
			@Override
			public Void run(Database db) throws Throwable
			{
				String uuid = UUID.randomUUID().toString();
				try { uuid = db.getString("SELECT LOCALID FROM FILES WHERE ID=?", file.getId()); }
				catch(NoSuchElementException e) { /* do nothing, proceed with random uuid; TODO: Parse from description */ }
				
				db.execute("DELETE FROM FILES WHERE ID=?", file.getId());
				db.execute("INSERT INTO FILES(ID, LOCALID, TITLE, MIMETYPE, MD5HEX, SIZE, MTIME, DOWNLOADURL, METADATAREFRESHED, CHILDRENREFRESHED) VALUES(?,?,?,?,?,?,?,?,?,?)", file.getId(), uuid, file.getTitle(), file.getMimeType(), file.getMd5Checksum(), file.getQuotaBytesUsed(), new Date(file.getModifiedDate().getValue()), file.getDownloadUrl(), asof, childrenAsOfDate != null ? childrenAsOfDate : null);
				return null;
			}
		});

		readBasicMetadata();
	}
	
	public String getTitle() throws IOException
	{
		acquireRead();
		try
		{
			if(title == null) readBasicMetadata();
			
			// Somebody appears to be watching this file; let's keep it up-to-date
			if(isDirectory()) considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
			else if(parents != null) for(File file : parents) file.considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
			
			return title;
		}
		finally
		{
			releaseRead();
		}
	}
	
	public String getMimeType() throws IOException
	{
		acquireRead();
		try
		{
			if(mimeType == null) readBasicMetadata();
			return mimeType;
		}
		finally
		{
			releaseRead();
		}
	}
	
	Boolean isDirectoryNoIO()
	{
		Date asof = metadataAsOfDate;
		String mimeType = this.mimeType;
		if(asof == null && mimeType == null) return null;
		return MIME_FOLDER.equals(mimeType);
	}
	
	public boolean isDirectory() throws IOException
	{
		acquireRead();
		try
		{
			if(mimeType == null && metadataAsOfDate == null) readBasicMetadata();
			return MIME_FOLDER.equals(mimeType);
		}
		finally
		{
			releaseRead();
		}
	}
	
	public long getSize() throws IOException
	{
		acquireRead();
		try
		{
			if(size == null) readBasicMetadata();
			return size;
		}
		finally
		{
			releaseRead();
		}
	}
	
	public Date getMetadataDate() throws IOException
	{
		acquireRead();
		try
		{
			if(metadataAsOfDate == null) readBasicMetadata();
			return metadataAsOfDate;
		}
		finally
		{
			releaseRead();
		}
	}
	
	public Timestamp getModified() throws IOException
	{
		acquireRead();
		try
		{
			if(modifiedTime == null) readBasicMetadata();
			return modifiedTime;
		}
		finally
		{
			releaseRead();
		}
	}
	
	public URL getDownloadUrl() throws IOException
	{
		acquireRead();
		try
		{
			if(downloadUrl == null) readBasicMetadata();
			return downloadUrl;
		}
		finally
		{
			releaseRead();
		}
	}
	
	public String getMd5Checksum() throws IOException
	{
		acquireRead();
		try
		{
			if(isDirectory()) return null;
			
			// Somebody appears to be watching this file; let's keep it up-to-date
			for(File file : getParents()) file.considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
			
			if(fileMd5 == null) readBasicMetadata();
			return fileMd5;
		}
		finally
		{
			releaseRead();
		}
	}
	
	private static final String MIME_FOLDER = "application/vnd.google-apps.folder";
	public File mkdir(String name) throws IOException
	{
		acquireWrite();
		try
		{
			com.google.api.services.drive.model.File newRemoteDirectory = new com.google.api.services.drive.model.File();
			newRemoteDirectory.setTitle(name);
			newRemoteDirectory.setMimeType(MIME_FOLDER);
			newRemoteDirectory.setParents(Arrays.asList(new ParentReference().setId(googleFileId)));
			
			newRemoteDirectory = drive.getRemote().files().insert(newRemoteDirectory).execute();
			File newDirectory = drive.getFile(newRemoteDirectory.getId());
			
			clearChildrenCache();
	
		    return newDirectory;
		}
		finally
		{
			releaseWrite();
		}
	}
		
	
	
	
    public byte[] read(final long size, final long offset) throws DatabaseConnectionException, IOException
    {
		acquireRead();
		try
		{
			byte[] data = getBytesByAnyMeans(offset, offset + size);
			if(data.length != size) throw new Error("expected: " + size + " actual: " + data.length + " " + new String(data));
			return data;
		}
		finally
		{
			releaseRead();
		}
	}
    
    
    
    protected byte[] downloadFragment(long startPosition, long endPosition) throws IOException
	{
    	System.out.println("Downloading "+fileMd5+" "+startPosition+" "+endPosition);
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition == endPosition) return new byte[0];
            
        ByteArrayOutputStream out = new ByteArrayOutputStream();

		HttpRequestFactory requestFactory = drive.getTransport().createRequestFactory(drive.getRemote().getRequestFactory().getInitializer());

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
	
	public UUID getLocalId() throws IOException
	{
		acquireRead();
		try
		{
			if(localFileId == null) readBasicMetadata();
			return localFileId;
		}
		finally
		{
			releaseRead();
		}
	}
    
    public void setTitle(String title) throws IOException
    {
    	acquireWrite();
    	try
    	{
	    	playEverywhere("setTitle", this.getLocalId().toString(), getTitle(), title);
    	}
    	finally
    	{
    		releaseWrite();
    	}
    }
    
    /** Writes the action to the database to be replayed on Google's servers later, plays transaction on local memory **/
    synchronized void playEverywhere(String command, String... logEntry) throws IOException
    {
    	playOnMetadata(command, logEntry);
    	playOnDatabase(command, logEntry);
    }
    
    /** Writes the action to the database to be replayed on Google's servers later, plays transaction on local memory **/
    void playOnDatabase(String command, String... logEntry) throws IOException
    {
    	if(!lock.isWriteLockedByCurrentThread()) throw new Error("Must acquire write lock if you're doing writes!");
    	String category = null;
    	if("setTitle".equals(command)) category = "metadata";
    	if("addRelationship".equals(command)) category = "relationship";
    	if("removeRelationship".equals(command)) category = "relationship";
    	
    	drive.getDatabase().execute("INSERT INTO UPDATELOG(CATEGORY, COMMAND, DETAILS) VALUES(?,?,?)", category, command, new XStream().toXML(logEntry)); 
    	drive.pokeLogPlayer();
    }
    
    private void playOnParentsList(List<File> parents, String command, String... logEntry) throws IOException
    {
		if(parents == null) return;
		
		if("addRelationship".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);

			if(!getLocalId().equals(childLocalId)) return;
			
			parents.add(drive.getFile(parentLocalId));
		}
		else if("removeRelationship".equals(command))
		{
			UUID childId = UUID.fromString(logEntry[1]);
			UUID parentId = UUID.fromString(logEntry[0]);
			
			if(!getLocalId().equals(childId)) return;
			
			File parent = null;
			for(File f : parents)
				if(parentId.equals(f.getLocalId()))
					parent = f;

			parents.remove(parent);
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
    }
    
	private void playLogOnParentsList(List<File> parents) throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG WHERE COMMAND='addRelationship' OR COMMAND='removeRelationship' ORDER BY ID ASC"))
			playOnParentsList(parents, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}
	
	private void playOnChildrenList(List<File> children, String command, String... logEntry) throws IOException
	{
		if(children == null) return;
		
		if("addRelationship".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);
			
			if(getLocalId().equals(parentLocalId) && children != null) children.add(drive.getFile(childLocalId));
		}
		else if("removeRelationship".equals(command))
		{
			UUID childId = UUID.fromString(logEntry[1]);
			UUID parentId = UUID.fromString(logEntry[0]);
			
			if(!getLocalId().equals(parentId)) return;
			
			File child = null;
			for(File f : children)
				if(childId.equals(f.getLocalId()))
					child = f;

			children.remove(child);
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
	}
    
	private void playLogOnChildrenList(List<File> children) throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG WHERE COMMAND='addRelationship' OR COMMAND='removeRelationship' ORDER BY ID ASC"))
			playOnChildrenList(children, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}
	
	synchronized void playOnMetadata(String command, String... logEntry) throws IOException
	{
		if("setTitle".equals(command))
		{
			// Sanity checks
			if(!getLocalId().equals(UUID.fromString(logEntry[0]))) return;
			if(!getTitle().equals(logEntry[1])) new Throwable("WARNING: Title does not match title from logs (expected: " + logEntry[1] + " was: " + getTitle() + ")").printStackTrace();

			// Perform update
			title = logEntry[2];
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
	}

	static void playOnRemote(Drive drive, String command, String... logEntry) throws IOException
	{
		if("setTitle".equals(command))
		{
			// Perform update
			String googleFileId = getGoogleId(drive, UUID.fromString(logEntry[0]));
			com.google.api.services.drive.model.File file = drive.getRemote().files().get(googleFileId).execute();
			if(!file.getTitle().equals(logEntry[1])) new Throwable("WARNING: Title does not match title from logs (expected: " + logEntry[1] + " was: " + file.getTitle() + ")").printStackTrace();
			file.setTitle(logEntry[2]);
			drive.getRemote().files().update(googleFileId, file).execute();
		}
		else if("addRelationship".equals(command))
		{
			String parentGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[0]));
			String childGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[1]));
		
			ParentReference newParent = new ParentReference();
			newParent.setId(parentGoogleFileId);
			System.out.println("INSERTing "+childGoogleFileId+" into "+newParent+" on remote");
			drive.getRemote().parents().insert(childGoogleFileId, newParent).execute();
			System.out.println("INSERTED "+childGoogleFileId+" into "+newParent+" on remote");
		}
		else if("removeRelationship".equals(command))
		{
			System.out.println("writing remote: "+Arrays.toString(logEntry));
			String parentGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[0]));
			String childGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[1]));
			
			drive.getRemote().parents().delete(childGoogleFileId, parentGoogleFileId).execute();
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
	}
	
	static String getGoogleId(Drive drive, UUID localId)
	{
		return drive.getDatabase().getString("SELECT ID FROM FILES WHERE LOCALID=?", localId.toString());
	}

	public List<File> getParents() throws IOException
	{
		acquireRead();
		try
		{
			if(parents != null) return parents;

			if(parentsAsOfDate == null && metadataAsOfDate == null) readBasicMetadata(); // See if maybe it's just not in the memory cache (DB faster than Google)
			if(parentsAsOfDate != null)
			{
				// Fetch from database
				List<File> parents = new ArrayList<File>();
				List<String> files = drive.getDatabase().getStrings("SELECT CHILD FROM RELATIONSHIPS WHERE CHILD=?", googleFileId);
				for(String file : files) parents.add(drive.getFile(file));
				playLogOnParentsList(parents);
				this.parents = parents;
				return parents;
			}
			
			updateParentsFromRemote();
			return parents;
		}
		finally
		{
			releaseRead();
		}
	}
	
	private void updateParentsFromRemote() throws IOException
	{
		if(parents != null && !lock.isWriteLockedByCurrentThread()) throw new Error("Must have write lock");
		if(parents == null && !lock.isWriteLockedByCurrentThread() && lock.getReadLockCount() == 0) throw new Error("Must have read or write lock");
		List<File> parents = new ArrayList<File>();
		for(ParentReference parent : drive.getRemote().files().get(googleFileId).execute().getParents())
			parents.add(drive.getFile(parent.getId()));
		playLogOnParentsList(parents);
		this.parents = parents;
	}

	public void addChild(File child) throws IOException
	{
		acquireWrite();
		try
		{
			if(child.getParents().contains(this)) return;
			if(this.equals(child) || parentsContainNode(child)) throw new RuntimeException("Can not add a child to one of the node's parents (direct or indirect)");
			
			if(!isDirectory()) throw new UnsupportedOperationException("Can not add child to non-directory");
	
			playOnDatabase("addRelationship", this.getLocalId().toString(), child.getLocalId().toString());
			playOnChildrenList(children, "addRelationship", this.getLocalId().toString(), child.getLocalId().toString());
			child.playOnParentsList(child.parents, "addRelationship", this.getLocalId().toString(), child.getLocalId().toString());
		}
		finally
		{
			releaseWrite();
		}
	}
	
	private boolean parentsContainNode(File node) throws IOException
	{
		System.out.println(this.getTitle()+" "+node.getTitle()+" "+Thread.currentThread().getId());
		boolean parentContained = false;
		for(File parent : this.getParents())
			if(node.equals(parent)) return true;
			else if(parent.parentsContainNode(node))
			{
				parentContained = true;
				break;
			}
		return parentContained;
	}

	public void removeChild(File child) throws IOException
	{
		acquireWrite();
		try
		{
			if(!this.getChildren().contains(child)) return;
			if(child.getParents().size() <= 1) throw new UnsupportedOperationException("Child must have at least one parent");
	
			playOnDatabase("removeRelationship", this.getLocalId().toString(), child.getLocalId().toString());
			playOnChildrenList(children, "removeRelationship", this.getLocalId().toString(), child.getLocalId().toString());
			child.playOnParentsList(child.parents, "removeRelationship", this.getLocalId().toString(), child.getLocalId().toString());
		}
		finally
		{
			releaseWrite();
		}
	}

	public void trash() throws IOException
	{
		if(this.equals(drive.getRoot())) throw new UnsupportedOperationException("Can not trash the root node");
		List<File> parents = getParents();
		drive.getRemote().files().trash(googleFileId).execute();

		// Clear the parent's cache of children
		for(File parent : parents)
			parent.clearChildrenCache();
	}
	
	@Override
	public boolean equals(Object other)
	{
		if(!File.class.equals(other.getClass())) return false;
		if(this.getId() != null && this.getId().equals(((File)other).getId())) return true;
		try
		{
			if(this.getLocalId().equals(((File)other).getLocalId())) return true;
		}
		catch(IOException e)
		{
			return false;
		}
		return false;
	}
    
	@Override
	public String toString()
	{
		return "File(" + googleFileId + ")";
	}

	private void acquireRead()
	{
		lock.readLock().lock();
	}
	
	private void acquireWrite()
	{
		drive.writeLock.lock();
		lock.writeLock().lock();
	}
	
	private void releaseRead()
	{
		lock.readLock().unlock();
	}
	
	private void releaseWrite()
	{
		drive.writeLock.unlock();
		lock.writeLock().unlock();
	}
}
