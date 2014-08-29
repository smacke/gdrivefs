package com.gdrivefs.simplecache;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gdrivefs.ConflictingOperationInProgressException;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.IOUtils;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;
import com.google.api.services.drive.model.Property;
import com.google.common.collect.ImmutableList;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.DatabaseRow;
import com.jimsproch.sql.Transaction;
import com.thoughtworks.xstream.XStream;

/**
 * File represents a particular remote file (as represented by Google's file ID),
 * but provides a clean interface for performing reads and writes
 * through the localhost cache layer.
 */
public class File
{
    private static Logger logger = LoggerFactory.getLogger(File.class);
	public static final int FRAGMENT_BOUNDARY = 1<<25; //32 MiB
	private static final int MAX_UPDATE_THREADS = 5;
	static ExecutorService updaterService = Executors.newFixedThreadPool(MAX_UPDATE_THREADS);
	private static long WRITE_THRESHOLD_MILLIS = 10000;

	String googleFileId;
	UUID localFileId;
	final Drive drive;
	
	// Cache (null indicates field must be fetched from db)
	Long metadataAsOfDate;
	Date childrenAsOfDate;
	Date parentsAsOfDate;
	String title;
	String fileMd5;
	URL downloadUrl;
	String mimeType;
	Long size;
	Timestamp modifiedTime;
	DuplicateRejectingList children;
	DuplicateRejectingList parents;
	final Lock uploadLock = new ReentrantLock();
	final Lock scratchSpaceLock = new ReentrantLock(); // MUST ALWAYS BE ACQUIRED BEFORE WRITELOCK IF ACQUIRED IN SUCCESSION
	final ExponentialBackOff uploadBackoff = new ExponentialBackOff.Builder()
		    .setInitialIntervalMillis(500)
		    .setMaxElapsedTimeMillis(900000)
		    .setMaxIntervalMillis(6000)
		    .setMultiplier(1.5)
		    .setRandomizationFactor(0.5)
		    .build();
	volatile long lastWriteTime=-1;
	AtomicBoolean freshWrite = new AtomicBoolean(false);
	ScheduledExecutorService writeCheckService = Executors.newScheduledThreadPool(1);
	{
		writeCheckService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run()
			{
				if (System.currentTimeMillis() - lastWriteTime > WRITE_THRESHOLD_MILLIS && freshWrite.getAndSet(false)) {
					// schedule the upload if it's been 10 seconds
					try
					{
						update(true);
					}
					catch(IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}
			
		}, WRITE_THRESHOLD_MILLIS, WRITE_THRESHOLD_MILLIS, TimeUnit.MILLISECONDS);
	}
	
	File(Drive drive, String id)
	{
		if(drive == null) throw new NullPointerException("drive must not be null");
		if(id == null) throw new NullPointerException("id must not be null");
		this.drive = drive;
		this.googleFileId = id;
	}
	
	File(Drive drive, UUID id)
	{
		if(drive == null) throw new NullPointerException("drive must not be null");
		if(id == null) throw new NullPointerException("id must not be null");
		this.drive = drive;
		this.localFileId = id;
	}
	
	/** Reads basic metadata from the cache, throwing an exception if the file metadata isn't in our database **/
	private void readBasicMetadata() throws IOException
	{
		if(googleFileId == null) {
			if (localFileId == null) {
				throw new Error("either googleFileId or localFileId must be non-null");
			}
			googleFileId = getGoogleId(drive, localFileId);
		}
		
		if(googleFileId != null)
		{
			if(drive.lock.getReadLockCount() == 0 && !drive.lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
			List<DatabaseRow> rows = drive.getDatabase().getRows("SELECT * FROM FILES WHERE ID=?", googleFileId);
			if(rows.size() == 0)
			{
				logger.debug("Requesting file metadata from Google for file id: {}", googleFileId);
				Date asof = new Date();
				com.google.api.services.drive.model.File metadata = drive.getRemote().files().get(googleFileId).execute();
				try { refresh(metadata, asof); }
				catch(SQLException e) {}
				rows = drive.getDatabase().getRows("SELECT * FROM FILES WHERE ID=?", googleFileId);
			}
			
			DatabaseRow row = rows.get(0);
			title = row.getString("TITLE");
			mimeType = row.getString("MIMETYPE");
			size = row.getLong("SIZE");
			modifiedTime = row.getTimestamp("MTIME");
			metadataAsOfDate = row.getTimestamp("METADATAREFRESHED").getTime();
			childrenAsOfDate = row.getTimestamp("CHILDRENREFRESHED");
			parentsAsOfDate = row.getTimestamp("PARENTSREFRESHED");
			localFileId = row.getUuid("LOCALID");
			fileMd5 = row.getString("MD5HEX");
			downloadUrl = row.getString("DOWNLOADURL") != null ? new URL(row.getString("DOWNLOADURL")) : null;
		}
		
		playLogOnMetadata();  //TODO: Log must be played before data is set to memory
	}
	
	void playLogOnMetadata() throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG WHERE COMMAND='setTitle' OR COMMAND='mkdir' OR COMMAND='createFile' OR COMMAND='truncate' OR COMMAND='write' OR COMMAND='update' ORDER BY ID ASC"))
			playOnMetadata(row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}
	
	static void playLogEntryOnRemote(Drive drive) throws IOException, SQLException
	{
		DatabaseRow row = null;
		try { row = drive.getDatabase().getRow("SELECT * FROM UPDATELOG ORDER BY ID ASC FETCH NEXT ROW ONLY"); }
		catch(NoSuchElementException e) { /* row doesn't exist; shouldn't happen on modern copies of jimboxutilities (which now just returns null) */ }
	
		if(row == null) return;  // We're done processing queue, just return (no need to continue poking the log player either).
			
		try {
            playOnRemote(drive, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
            drive.getDatabase().execute("DELETE FROM UPDATELOG WHERE ID=?", row.getInteger("ID"));
		} catch (ConflictingOperationInProgressException e) {
			// in this case, we refuse to delete from the update log
			// and will retry the most recent log entry, w/ some
			// exponential backoff
		}
		drive.pokeLogPlayer();
	}
	
	public void refresh() throws IOException
	{
		logger.debug("Refreshing: {} {}", googleFileId, localFileId);
		acquireWrite();
		try
		{
			if(googleFileId == null) googleFileId = getGoogleId(drive, localFileId);
			if(googleFileId == null) return;
			
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
						if(!drive.fileUpdateWorker.isShutdown())
							drive.fileUpdateWorker.execute(new Runnable()
							{
								@Override
								public void run()
								{
									try
									{
										if(drive.fileUpdateWorker.isShutdown()) return;
										if(f.isDirectory()) f.getChildren();
										if(drive.fileUpdateWorker.isShutdown()) return;
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
		
		try
		{
			drive.fileUpdateWorker.execute(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						if(drive.fileUpdateWorker.isShutdown()) return;
						considerSynchronousDirectoryRefresh(threshold, units);
					}
					catch(IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			});
		}
		catch(Exception e)
		{
			// do nothing.  Probably the worker is shutting down and we don't care.
			// We're only required to consider a refresh, so consider this considered :).
			System.out.println("Note: skipping refresh due to: "+e.getMessage());
		}
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
			
			if(!isDirectory()) return ImmutableList.copyOf(children);
			
			try
			{
				if(childrenAsOfDate == null && metadataAsOfDate == null) readBasicMetadata(); // See if maybe it's just not in the memory cache (DB faster than Google)
				if(googleFileId == null || childrenAsOfDate != null)
				{
					DuplicateRejectingList children = drive.getDatabase().execute(new Transaction<DuplicateRejectingList>()
					{
						@Override
						public DuplicateRejectingList run(Database db) throws Throwable
						{
							DuplicateRejectingList children = new DuplicateRejectingList();
							List<String> files = drive.getDatabase().getStrings("SELECT CHILD FROM RELATIONSHIPS WHERE PARENT=?", googleFileId);
							for(String file : files)
								children.add(drive.getCachedFile(file));

							considerAsyncDirectoryRefresh(10, TimeUnit.MINUTES);
							for(File child : children)
								if(child.isDirectory()) child.considerAsyncDirectoryRefresh(30, TimeUnit.MINUTES);
							return children;
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
		logger.debug("Updating children of {} from remote", googleFileId);
		if(children != null && !drive.lock.isWriteLockedByCurrentThread()) throw new Error("Children cached, so doibng a remote fetch require a write lock!");

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


			DuplicateRejectingList children = new DuplicateRejectingList();
			for(com.google.api.services.drive.model.File child : googleChildren)
				children.add(drive.getFile(child, childrenUpdateDate));
			
			drive.getDatabase().execute(new Transaction<Void>()
			{
				@Override
				public Void run(Database arg0) throws Throwable
				{
					drive.getDatabase().execute("DELETE FROM RELATIONSHIPS WHERE PARENT=?", googleFileId);
					for(com.google.api.services.drive.model.File child : googleChildren)
						drive.getDatabase().execute("INSERT INTO RELATIONSHIPS(PARENT, CHILD) VALUES(?,?)", getId(), child.getId());
					return null;
				}
			});
			playLogOnChildrenList(children);
			this.children = children;
			
			// Ok, dirty trick... we have the data anyway, and we can update without a lock if the child doesn't have a metadata date.  Ewww!
			for(final com.google.api.services.drive.model.File child : googleChildren)
			{
				final File f = drive.getFile(child, childrenUpdateDate);
				
				try
				{
					if(!drive.fileUpdateWorker.isShutdown())
						drive.fileUpdateWorker.execute(new Runnable(){
							@Override
							public void run()
							{
								try
								{
									if(drive.fileUpdateWorker.isShutdown()) return;
									
									if(drive.fileUpdateWorker.isShutdown()) return;
									f.acquireWrite();
									try {f.refresh(child, childrenUpdateDate); }
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
				}
				catch(Exception e)
				{
					// worker is probably shutting down; it was just a convinence update anyway
					System.out.println("skipping adding due to: "+e.getMessage());
				}
			}
			
			// Sketchy as hell, but I can't think of any way that updating the timestamp could cause a fatal race condition
			drive.getDatabase().execute("UPDATE FILES SET CHILDRENREFRESHED = ? WHERE ID = ?", childrenUpdateDate, googleFileId);
			this.childrenAsOfDate = childrenUpdateDate;
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	void refresh(final com.google.api.services.drive.model.File file, final Date asof) throws IOException, SQLException
	{
		refresh(file, asof.getTime());
	}
	
	void refresh(final com.google.api.services.drive.model.File file, final long asof) throws IOException, SQLException
	{
		if(drive.lock.getReadLockCount() == 0 && !drive.lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
		
		if(metadataAsOfDate != null && metadataAsOfDate >= asof) return; // Bail with no action if the asof date isn't newer.
		
		if(this.googleFileId != null && !this.googleFileId.equals(file.getId())) throw new Error("File ID Miss-match: "+this.googleFileId+" "+file.getId());
		this.googleFileId = file.getId();
		
		GregorianCalendar nextUpdateTimestamp = new GregorianCalendar();
		nextUpdateTimestamp.add(Calendar.DAY_OF_YEAR, 1);
		
		drive.getDatabase().execute(new Transaction<Void>()
		{
			@Override
			public Void run(Database db) throws Throwable
			{
				if(localFileId == null)
					try { localFileId = db.getUuid("SELECT LOCALID FROM FILES WHERE ID=?", file.getId()); }
					catch(NoSuchElementException e) { /* do nothing, proceed with random uuid; TODO: Parse from description */ }
				if(localFileId == null) localFileId = UUID.randomUUID();
				
				db.execute("DELETE FROM FILES WHERE ID=?", file.getId());
				db.execute("INSERT INTO FILES(ID, LOCALID, TITLE, MIMETYPE, MD5HEX, SIZE, MTIME, DOWNLOADURL, METADATAREFRESHED, CHILDRENREFRESHED) VALUES(?,?,?,?,?,?,?,?,?,?)", file.getId(), localFileId, file.getTitle(), file.getMimeType(), file.getMd5Checksum(), file.getQuotaBytesUsed(), new Date(file.getModifiedDate().getValue()), file.getDownloadUrl(), new Date(asof), childrenAsOfDate != null ? childrenAsOfDate : null);
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
		Long asof = metadataAsOfDate;
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
			return size == null ? 0 : size;
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
			return new Date(metadataAsOfDate);
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
			File newDirectory = drive.getFile(UUID.randomUUID());
			long creationTime = System.currentTimeMillis();
			
			playOnDatabase("mkdir", this.getLocalId().toString(), newDirectory.getLocalId().toString(), name, Long.toString(creationTime));
			playOnMetadata("mkdir", this.getLocalId().toString(), newDirectory.getLocalId().toString(), name, Long.toString(creationTime));
			playOnChildrenList(children, "mkdir", this.getLocalId().toString(), newDirectory.getLocalId().toString(), name, Long.toString(creationTime));
			
		    return newDirectory;
		}
		finally
		{
			releaseWrite();
		}
	}

	public File createFile(String name) throws IOException
	{
		acquireWrite();
		try
		{
			File newFile = drive.getFile(UUID.randomUUID());
			long creationTime = System.currentTimeMillis();
			playOnDatabase("createFile", this.getLocalId().toString(), newFile.getLocalId().toString(), name, Long.toString(creationTime));
			playOnMetadata("createFile", this.getLocalId().toString(), newFile.getLocalId().toString(), name, Long.toString(creationTime));
			playOnChildrenList(children, "createFile", this.getLocalId().toString(), newFile.getLocalId().toString(), name, Long.toString(creationTime));
			playOnParentsList(newFile.parents, "createFile", this.getLocalId().toString(), newFile.getLocalId().toString(), name, Long.toString(creationTime));
			
			return newFile;
		}
		finally
		{
			releaseWrite();
		}
	}

	public void update(boolean force) throws IOException
	{
		if (!force && !freshWrite.getAndSet(false)) {
			// Nothing has changed (locally at least),
			// and we're not forcing an upload, so return.
			return;
		}

		// TODO this is a hack to prevent any downloads
		// from overwriting any writes happening concurrently
		scratchSpaceLock.lock();
		try {
            fillInGapsBetween(0, getSize());
            storeFragmentsToUploadFile();

            acquireWrite();
		} finally {
			scratchSpaceLock.unlock();
		}
		try
		{

			// TODO md5 computation is pretty expensive for large files, actually,
			// and shouldn't happen behind a writelock. What we really want to do
			// is keep track of md5's in the fragments table, associating with each
			// chunk the md5 of the file at the time that chunk was definitely part
			// of the file. We can then download things not behind any lock, and
			// detect when things got out of sync while downloading somehow. This will
			// likely also involve looking up lastModified timestamps to prevent the
			// situation on the google end where we have A -> B -> A.
//            FileInputStream stream = new FileInputStream(getUploadFile());
//            String md5;
//            try { md5 = DigestUtils.md5Hex(stream); }
//            finally { stream.close(); }
//            
//            // don't bother to do the upload if nothing changed
//            if (md5.equals(fileMd5)) {
//            	return;
//            }
//            // set the new md5
//            fileMd5 = md5;

			// N.B. (smacke): there's a slight race condition here, in that
			// by the time the logPlayer starts playing the upload file, somebody
			// else could have come in and updated the file. This is okay, though -- we'll
			// just be uploading a more up-to-date file to Google. The only caveat
			// is that we have to make sure to scratch-space-lock the actual upload
			// to google as well, since otherwise it could be changing while we're
			// uploading it.
			String[] logEntry = new String[]{this.getLocalId().toString(), Long.toString(getUploadFile().length()), null};
			playOnDatabase("update", logEntry);
			playOnMetadata("update", logEntry);
		}
		finally
		{
			releaseWrite();
		}
	}
	
    public byte[] read(final int size, final long offset) throws IOException
    {
		acquireRead();
		try
		{
			System.out.printf("read %d bytes at offset %d for file %s\n", size, offset, getTitle());
			byte[] data = getBytesByAnyMeans(offset, offset + size);
			if(data.length != size) throw new Error("expected: " + size + " actual: " + data.length + " " + new String(data));
			return data;
		}
		finally
		{
			releaseRead();
		}
	}
	
    public void truncate(final long offset) throws IOException
    {
    	RandomAccessFile uploadFile = null;
    	try {
    		uploadFile = new RandomAccessFile(getUploadFile(), "rw");
    		uploadFile.setLength(offset);
    	} finally {
    		if (uploadFile != null) {
    			uploadFile.close();
    		}
    	}
		acquireWrite();
		System.out.println("truncate for " + getTitle());
		try
		{
			long oldSize = getSize();
			dropFragmentsStartingAtOrAfter(offset);
			// extend by 0 if necessary
			storeFragment(null, oldSize, new byte[(int)Math.max(0, offset-oldSize)]);
			playOnDatabase("truncate", this.getLocalId().toString(), Long.toString(offset), "null");
			playOnMetadata("truncate", this.getLocalId().toString(), Long.toString(offset), "null");
		}
		finally
		{
			releaseWrite();
		}
	}
	
    public void write(byte[] bytes, final long offset) throws IOException
    {
    	String chunkMd5 = DigestUtils.md5Hex(bytes);
    	// this way, a download coming in won't overwrite the
    	// things we are about to write to the fragments table.
    	scratchSpaceLock.lock();
    	try {
            acquireWrite();
            try
            {
                System.out.printf("write %d bytes at offset %d for file %s\n", bytes.length, offset, getTitle());
                lastWriteTime = System.currentTimeMillis();
                freshWrite.set(true);
                storeFragment(null, offset, bytes);
                playOnDatabase("write", this.getLocalId().toString(), Long.toString(offset), Long.toString(bytes.length), chunkMd5, "null");
                playOnMetadata("write", this.getLocalId().toString(), Long.toString(offset), Long.toString(bytes.length), chunkMd5, "null");
            }
            finally
            {
                releaseWrite();
            }
    	} finally {
    		scratchSpaceLock.unlock();
    	}
	}

    /**
     * 
     * @param startPosition
     * @param endPosition
     * @return Number of bytes downloaded.
     * @throws IOException
     */
    protected int downloadFragment(long startPosition, long endPosition) throws IOException
	{
    	System.out.println("Downloading for " + getTitle() + " " +fileMd5+" "+startPosition+" "+endPosition);
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition == endPosition) return 0;
            
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
        
        return bytes.length;
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


    /**
     * Ideally, this method should only do destructive database ops when write-lock protected.
     * E.g. anything inserted during a download should not overlap any existing fragments.
     * 
     * @param fileMd5
     * @param fragmentStartByte
     * @param fragment
     * @throws IOException
     */
    private void storeFragment(
    		@Nullable String fileMd5, // null file md5 indicates that this op is coming in locally and is most up-to-date
    		long fragmentStartByte,
    		byte[] fragment) throws IOException {
    	long start = fragmentStartByte;
    	long end = fragmentStartByte + fragment.length;
    	if (fragment.length==0) {
    		return;
    	}
    	System.out.println("store fragment for " + getTitle() + ": start=" + start + ", end=" + end);
    	// get overlapping fragments
    	// sort by startbyte asc
    	// then endbyte desc
    	// this allows for a slight optimization during merging --
    	// we use the fragment that spans more data when possible,
    	// if two fragments start in the same place (fewer I/O calls)
    	List<DatabaseRow> rows = drive.getDatabase().getRows("SELECT * FROM FRAGMENTS WHERE LOCALID=? AND ((ENDBYTE > ? AND ENDBYTE <= ?) OR (STARTBYTE >= ? AND STARTBYTE < ?) OR (STARTBYTE <= ? AND ENDBYTE >= ?)) ORDER BY STARTBYTE ASC, ENDBYTE DESC", getLocalId().toString(), start, end, start, end, start, end);

    	long globalStartByte = start;
    	long globalEndByte = end;
    	for (DatabaseRow row : rows) {
    		globalEndByte = Math.max(globalEndByte, row.getLong("ENDBYTE"));
    	}

    	byte[] merged;
    	if (rows.size() > 0) {
    		globalStartByte = Math.min(globalStartByte, rows.get(0).getLong("STARTBYTE"));
    		merged = new byte[(int)(globalEndByte - globalStartByte)];

    		System.arraycopy(fragment, 0, merged, (int)(start-globalStartByte), fragment.length);

    		long position = globalStartByte;

    		// skip the new fragment; we've already done an arraycopy
    		if (position == start) {
    			position = end;
    		}
    		for (int chunk=0; chunk<rows.size(); chunk++) {
    			long chunkEnd = rows.get(chunk).getLong("ENDBYTE");
    			if (position >= chunkEnd) {
    				continue;
    			}
    			String chunkMd5 = rows.get(chunk).getString("CHUNKMD5");
    			long chunkStart = rows.get(chunk).getLong("STARTBYTE");
    			if (position < chunkStart) {
    				throw new Error("inexplicable gap");
    			}
    			if (chunkEnd - chunkStart > FRAGMENT_BOUNDARY) {
    				throw new Error("chunk larger than max google fragment size!");
    			}
    			byte[] fileBytes = FileUtils.readFileToByteArray(getCacheFile(chunkMd5));
    			while (position < chunkEnd) {
    				merged[(int)(position-globalStartByte)] = fileBytes[(int)(position-chunkStart)];
    				position++;
    				// skip the new fragment; we've already done an arraycopy
    				if (position == start) {
    					position = end;
    				}
    			}
    		}

    		drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE LOCALID=? AND ((ENDBYTE > ? AND ENDBYTE <= ?) OR (STARTBYTE >= ? AND STARTBYTE < ?) OR (STARTBYTE <= ? AND ENDBYTE >= ?))", getLocalId().toString(), start, end, start, end, start, end);
    	} else {
    		merged = fragment;
    	}
    	String chunkMd5 = DigestUtils.md5Hex(merged);
    	FileUtils.writeByteArrayToFile(getCacheFile(chunkMd5), merged);
    	drive.getDatabase().execute("INSERT INTO FRAGMENTS(LOCALID, FILEMD5, CHUNKMD5, STARTBYTE, ENDBYTE) VALUES(?,?,?,?,?)",
    			getLocalId(), fileMd5, chunkMd5, globalStartByte, globalStartByte + merged.length);

    }
    
    private void dropFragmentsStartingAtOrAfter(long offset) throws IOException {
    	if (!drive.lock.isWriteLockedByCurrentThread()) {
    		throw new Error("need write lock to do writes");
    	}
    	drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE LOCALID=? AND STARTBYTE>=?", getLocalId(), offset);
    }
    
	static java.io.File getCacheFile(String chunkMd5)
	{
		java.io.File cacheFile = new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "cache");
		for(byte c : chunkMd5.getBytes()) {
			cacheFile = new java.io.File(cacheFile, Character.toString((char) c));
		}
		cacheFile = new java.io.File(cacheFile, chunkMd5);
		return cacheFile;
	}
	
	private java.io.File getUploadFile() throws IOException
	{
		java.io.File uploadFile = new java.io.File(new java.io.File(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "uploads"), getLocalId().toString()), getTitle().replaceAll("/", ""));
		uploadFile.getParentFile().mkdirs();
		return uploadFile;
	}
	
	void storeFragmentsToUploadFile() throws IOException {
		java.io.File uploadFile = getUploadFile();
    	List<DatabaseRow> rows = drive.getDatabase().getRows("SELECT * FROM FRAGMENTS WHERE LOCALID=? ORDER BY STARTBYTE ASC, ENDBYTE DESC", getLocalId().toString());
    	FileOutputStream out = null;
    	try {
    		out = new FileOutputStream(uploadFile);
    		long position = 0;
    		for (DatabaseRow row : rows) {
    			long startByte = row.getLong("STARTBYTE");
    			if (startByte > position) {
    				throw new Error("unexpected gap");
    			}
    			long endByte = row.getLong("ENDBYTE");
    			if (endByte <= position) {
    				continue;
    			}
    			byte[] chunk = FileUtils.readFileToByteArray(getCacheFile(row.getString("CHUNKMD5")));
    			// sanity check
    			if (chunk.length != endByte-startByte) {
    				throw new Error("unexpected byte array from file");
    			}
    			out.write(chunk, (int)(position-startByte), (int)(endByte-position));
    			position += (endByte - position);
    			if (position == getSize()) {
    				break;
    			}
    		}
    		if (position != getSize()) {
    			throw new Error("something went wrong reading upload file from fragments");
    		}
    	} finally {
    		if (out != null) {
    			out.close();
    		}
    	}
	}
	
	public byte[] getBytesByAnyMeans(long start, long end) throws IOException
	{
		// this will be holding a read lock
		fillInGapsBetween(start, end);
		byte[] output = new byte[(int)(end-start)];
		List<DatabaseRow> fragments = drive.getDatabase().getRows("SELECT * FROM FRAGMENTS WHERE LOCALID=? AND STARTBYTE < ? AND ENDBYTE > ? ORDER BY STARTBYTE ASC", getLocalId(), end, start);

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
			
			if (startbyte > currentPosition) {
				throw new Error("should not have gaps");
			}

			// Consume the fragment
			int copyStart = (int)(currentPosition-startbyte);
			System.out.println("endbyte"+endbyte+" "+"currentPosition"+currentPosition+" "+"startbyte"+startbyte+" "+"readend"+end);
			int copyEnd = Math.min((int)(endbyte-startbyte), (int)(end-startbyte));
			System.out.println("readstart"+start+" "+"readend"+end+" "+"copyStart"+copyStart+" "+"copyEnd"+copyEnd+" "+"destpos"+(currentPosition-start)+" "+"length"+(copyEnd-copyStart)+" ");
			System.out.println("outputlength"+output.length+" "+"fragmentlength"+FileUtils.readFileToByteArray(cachedChunkFile).length);
			System.arraycopy(FileUtils.readFileToByteArray(cachedChunkFile), copyStart, output, (int)(currentPosition-start), copyEnd-copyStart);
			currentPosition += copyEnd-copyStart;
			
			if (currentPosition >= end) {
				if (currentPosition > end) {
					throw new Error("should not be reading past end");
				}
				break;
			}
		}
		
		if (currentPosition < end) {
			throw new Error("unexpected gap at end");
		}
		
		return output;
	}
	
	void fillInGapsBetween(long start, long end) throws IOException
	{
		// This method may not necessarily be holding a read lock
		// TODO (smacke): should it??
		
		List<DatabaseRow> fragments = drive.getDatabase().getRows("SELECT * FROM FRAGMENTS WHERE LOCALID=? AND STARTBYTE < ? AND ENDBYTE > ? ORDER BY STARTBYTE ASC", getLocalId(), end, start);

		long currentPosition = start;
		for(DatabaseRow fragment : fragments)
		{
			long startbyte = fragment.getLong("STARTBYTE");
			long endbyte = fragment.getLong("ENDBYTE");
			String chunkMd5 = fragment.getString("CHUNKMD5");
			java.io.File cachedChunkFile = File.getCacheFile(chunkMd5);
			
			if(!cachedChunkFile.exists() || cachedChunkFile.length() != endbyte-startbyte)
			{
				drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE CHUNKMD5=?", chunkMd5);
				continue;
			}
			
			// If the fragment starts after the byte we need, download the piece we still need
			// TODO (smacke): If the gap is larger than 32 MiB, need to break it up into chunks
			if(startbyte > currentPosition)
			{
				System.out.println("had to fill in gap for file " + getLocalId());
				currentPosition += downloadFragment(currentPosition, Math.min(startbyte, end));
			}

			// Consume the fragment
			currentPosition += Math.min(endbyte - currentPosition, end - currentPosition);
		}
		
		currentPosition += downloadFragment(currentPosition, end);
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
    		java.io.File oldCacheLocation = getUploadFile();
	    	playEverywhere("setTitle", this.getLocalId().toString(), getTitle(), title);
	    	if(oldCacheLocation.exists()) oldCacheLocation.renameTo(getUploadFile());
    	}
    	finally
    	{
    		releaseWrite();
    	}
    }
    
    /** Writes the action to the database to be replayed on Google's servers later, plays transaction on local memory **/
    void playEverywhere(String command, String... logEntry) throws IOException
    {
    	playOnMetadata(command, logEntry);
    	playOnDatabase(command, logEntry);
    }
    
    /** Writes the action to the database to be replayed on Google's servers later, plays transaction on local memory **/
    void playOnDatabase(String command, String... logEntry) throws IOException
    {
    	// TODO (smacke): does this actually have to be write-locked?
    	if(!drive.lock.isWriteLockedByCurrentThread()) throw new Error("Must acquire write lock if you're doing writes!");
    	
    	drive.getDatabase().execute("INSERT INTO UPDATELOG(COMMAND, DETAILS) VALUES(?,?)", command, new XStream().toXML(logEntry));
    	drive.pokeLogPlayer();
    }
    
    private void playOnParentsList(DuplicateRejectingList parents, String command, String... logEntry) throws IOException
    {
		if(parents == null) return;
		
		if("addRelationship".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);

			if(!getLocalId().equals(childLocalId)) return;
			
			File parent = drive.getFile(parentLocalId);
			if(!parents.contains(parent)) parents.add(parent);
		}
		else if("mkdir".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);

			if(!getLocalId().equals(childLocalId)) return;

			File parent = drive.getFile(parentLocalId);
			if(!parents.contains(parent)) parents.add(parent);
		}
		else if("createFile".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);

			if(!getLocalId().equals(childLocalId)) return;

			File parent = drive.getFile(parentLocalId);
			if(!parents.contains(parent)) parents.add(parent);
		}
		else if("removeRelationship".equals(command))
		{
			UUID childId = UUID.fromString(logEntry[1]);
			UUID parentId = UUID.fromString(logEntry[0]);
			
			if(!getLocalId().equals(childId)) return;
			
			File parent = null;
			for(File f : parents) {
				if(parentId.equals(f.getLocalId())) {
					parent = f;
				}
			}

			parents.remove(parent);
		}
		else throw new Error("Unknown log entry: "+command+" "+Arrays.toString(logEntry));
    }
    
	private void playLogOnParentsList(DuplicateRejectingList parents) throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG WHERE COMMAND='addRelationship' OR COMMAND='removeRelationship' OR COMMAND='mkdir' OR COMMAND='createFile' ORDER BY ID ASC"))
			playOnParentsList(parents, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}
	
	private void playOnChildrenList(DuplicateRejectingList children, String command, String... logEntry) throws IOException
	{
		if(children == null) return;

		if("addRelationship".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);
			
			File child = drive.getFile(childLocalId);
			if(getLocalId().equals(parentLocalId) && children != null && !children.contains(child)) children.add(child);
		}
		else if("createFile".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);

			File child = drive.getFile(childLocalId);
			if(getLocalId().equals(parentLocalId) && children != null && !children.contains(child)) children.add(child);
		}
		else if("mkdir".equals(command))
		{
			UUID parentLocalId = UUID.fromString(logEntry[0]);
			UUID childLocalId = UUID.fromString(logEntry[1]);

			File child = drive.getFile(childLocalId);
			if(getLocalId().equals(parentLocalId) && children != null && !children.contains(child)) children.add(child);
		}
		else if("removeRelationship".equals(command))
		{
			UUID childId = UUID.fromString(logEntry[1]);
			UUID parentId = UUID.fromString(logEntry[0]);
			
			if(!getLocalId().equals(parentId)) return;
			
			File child = null;
			for(File f : children) {
				if(childId.equals(f.getLocalId())) {
					child = f;
				}
			}

			children.remove(child);
		}
		else if ("trash".equals(command))
		{
			boolean appliesToMe = false;
			for(int i = 1; i < logEntry.length; i++)
				if(this.getLocalId().equals(UUID.fromString(logEntry[i])))
					appliesToMe = true;
			if(!appliesToMe) return;
			
			UUID trashedFile = UUID.fromString(logEntry[0]);
			
			File child = null;
			for(File f : children)
				if(trashedFile.equals(f.getLocalId()))
					child = f;

			children.remove(child);
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
	}
    
	private void playLogOnChildrenList(DuplicateRejectingList children) throws IOException
	{
		for(DatabaseRow row : drive.getDatabase().getRows("SELECT * FROM UPDATELOG WHERE COMMAND='addRelationship' OR COMMAND='removeRelationship' OR COMMAND='mkdir' OR COMMAND='createFile' OR COMMAND='trash' ORDER BY ID ASC"))
			playOnChildrenList(children, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}
	
	void playOnMetadata(String command, String... logEntry) throws IOException
	{
		if("setTitle".equals(command))
		{
			// Sanity checks
			if(!getLocalId().equals(UUID.fromString(logEntry[0]))) return;
			if(!getTitle().equals(logEntry[1])) new Throwable("WARNING: Title does not match title from logs (expected: " + logEntry[1] + " was: " + getTitle() + ")").printStackTrace();

			// Perform update
			title = logEntry[2];
		}
		else if("mkdir".equals(command))
		{
			// Sanity checks
			if(!getLocalId().equals(UUID.fromString(logEntry[1]))) return;

			// Perform update
			title = logEntry[2];
			
			mimeType = MIME_FOLDER;
			size = null;
			modifiedTime = new Timestamp(Long.parseLong(logEntry[3]));
			metadataAsOfDate = new Timestamp(Long.parseLong(logEntry[3])).getTime();
			childrenAsOfDate = null;
			parentsAsOfDate = null;
			localFileId = UUID.fromString(logEntry[1]);
			fileMd5 = null;
			downloadUrl = null;	
		}
		else if("createFile".equals(command))
		{
			// Sanity checks
			if(!getLocalId().equals(UUID.fromString(logEntry[1]))) return;

			// Perform update
			title = logEntry[2];
			
			mimeType = "application/octet-stream";
			size = 0L;
			modifiedTime = new Timestamp(Long.parseLong(logEntry[3]));
			metadataAsOfDate = new Timestamp(Long.parseLong(logEntry[3])).getTime();
			childrenAsOfDate = null;
			parentsAsOfDate = null;
			localFileId = UUID.fromString(logEntry[1]);
			fileMd5 = DigestUtils.md5Hex("");
			downloadUrl = null;
		}
		else if("update".equals(command))
		{
			if(!getLocalId().equals(UUID.fromString(logEntry[0]))) return;
			size = Long.parseLong(logEntry[1]);
			fileMd5 = "null".equals(logEntry[2]) ? null : logEntry[2];
		}
		else if("truncate".equals(command))
		{
			if(!getLocalId().equals(UUID.fromString(logEntry[0]))) return;
			size = Long.parseLong(logEntry[1]);
			fileMd5 = "null".equals(logEntry[2]) ? null : logEntry[2];
		}
		else if("write".equals(command))
		{
			if(!getLocalId().equals(UUID.fromString(logEntry[0]))) return;
			long offset = Long.parseLong(logEntry[1]);
			long length = Long.parseLong(logEntry[2]);
			if (size == null) { // N.B. (smacke): can't call getSize() or we will infinite recurse
				size = offset+length;
			} else {
				size = Math.max(size, offset+length);
			}
			fileMd5 = "null".equals(logEntry[4]) ? null : logEntry[4];
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
	}

	static void playOnRemote(final Drive drive, final String command, final String... logEntry) throws IOException, SQLException, ConflictingOperationInProgressException
	{
		logger.debug("Playing on remote: ", command, Arrays.toString(logEntry));
		if("setTitle".equals(command))
		{
			// Perform update
			String googleFileId = getGoogleId(drive, UUID.fromString(logEntry[0]));
			if(googleFileId == null) throw new Error("googleFileId id should not be null at this point for "+logEntry[0]);
			com.google.api.services.drive.model.File file = drive.getRemote().files().get(googleFileId).execute();
			if(!file.getTitle().equals(logEntry[1]) && !file.getTitle().equals(logEntry[2])) new Throwable("WARNING: Title does not match title from logs (expected: " + logEntry[1] + " was: " + file.getTitle() + ")").printStackTrace();
			file.setTitle(logEntry[2]);
			drive.getRemote().files().update(googleFileId, file).execute();
		}
		else if("addRelationship".equals(command))
		{
			String parentGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[0]));
			String childGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[1]));

			if(parentGoogleFileId == null) throw new Error("parentGoogleFileId id should not be null at this point for "+logEntry[0]);
			if(childGoogleFileId == null) throw new Error("childGoogleFileId id should not be null at this point for "+logEntry[1]);
		
			ParentReference newParent = new ParentReference();
			newParent.setId(parentGoogleFileId);
			drive.getRemote().parents().insert(childGoogleFileId, newParent).execute();
		}
		else if("removeRelationship".equals(command))
		{
			String parentGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[0]));
			String childGoogleFileId = getGoogleId(drive, UUID.fromString(logEntry[1]));
			
			drive.getRemote().parents().delete(childGoogleFileId, parentGoogleFileId).execute();
		}
		else if("mkdir".equals(command))
		{
			com.google.api.services.drive.model.File newRemoteDirectory = new com.google.api.services.drive.model.File();
			newRemoteDirectory.setTitle(logEntry[2]);
			newRemoteDirectory.setDescription("gdrivefsid="+logEntry[1]);
			newRemoteDirectory.setMimeType(MIME_FOLDER);
			newRemoteDirectory.setParents(Arrays.asList(new ParentReference().setId(getGoogleId(drive, UUID.fromString(logEntry[0])))));

			Property gdrivefsid = new Property();
			gdrivefsid.setKey("com.gdrivefs.id");
			gdrivefsid.setValue(logEntry[1]);
			gdrivefsid.setVisibility("PRIVATE");
			newRemoteDirectory.setProperties(ImmutableList.of(gdrivefsid));
			
			File newLocalDirectory = drive.getFile(UUID.fromString(logEntry[1]));
			Date asof = new Date();
			newRemoteDirectory = drive.getRemote().files().insert(newRemoteDirectory).execute();
			
			newLocalDirectory.acquireRead();
			try { newLocalDirectory.refresh(newRemoteDirectory, asof); }
			finally { newLocalDirectory.releaseRead(); }
			
			drive.lock.writeLock().lock();
			try
			{
				// Fetching the file by old ID will cause the new google identifier to be found and the cache updated
				newLocalDirectory.metadataAsOfDate = null;
				newLocalDirectory.childrenAsOfDate = null;
				newLocalDirectory.parentsAsOfDate = null;
				drive.getFile(newRemoteDirectory, asof);
			}
			finally
			{
				drive.lock.writeLock().unlock();
			}
			

			if(getGoogleId(drive, UUID.fromString(logEntry[1])) == null) throw new Error("GoogleId should not be null at this point for: "+logEntry[1]);
		}
		else if("createFile".equals(command))
		{
			com.google.api.services.drive.model.File newRemoteDirectory = new com.google.api.services.drive.model.File();
			newRemoteDirectory.setTitle(logEntry[2]);
			newRemoteDirectory.setDescription("gdrivefsid="+logEntry[1]);
			newRemoteDirectory.setMimeType("application/octet-stream");
			newRemoteDirectory.setParents(Arrays.asList(new ParentReference().setId(getGoogleId(drive, UUID.fromString(logEntry[0])))));

			File newLocalDirectory = drive.getFile(UUID.fromString(logEntry[1]));

			Property gdrivefsid = new Property();
			gdrivefsid.setKey("com.gdrivefs.id");
			gdrivefsid.setValue(logEntry[1]);
			gdrivefsid.setVisibility("PRIVATE");
			newRemoteDirectory.setProperties(ImmutableList.of(gdrivefsid));

	//		String type = Files.probeContentType(Paths.get(getUploadFile(newLocalDirectory).getAbsolutePath()));
	//		FileContent mediaContent = new FileContent(type, getUploadFile(newLocalDirectory));

			Date asof = new Date();
			newRemoteDirectory = drive.getRemote().files().insert(newRemoteDirectory).execute();

			newLocalDirectory.acquireRead();
			try { newLocalDirectory.refresh(newRemoteDirectory, asof); }
			finally { newLocalDirectory.releaseRead(); }
			
			drive.lock.writeLock().lock();
			try
			{
				// Fetching the file by old ID will cause the new google identifier to be found and the cache updated
				newLocalDirectory.metadataAsOfDate = null;
				newLocalDirectory.childrenAsOfDate = null;
				newLocalDirectory.parentsAsOfDate = null;
				drive.getFile(newRemoteDirectory, asof);
			}
			finally
			{
				drive.lock.writeLock().unlock();
			}
		}
		else if("trash".equals(command))
		{
			final File file = drive.getFile(UUID.fromString(logEntry[0]));
			System.out.println("trash remote file " + file.getTitle());
			try {
                if (file.uploadLock.tryLock(file.uploadBackoff.nextBackOffMillis(), TimeUnit.MILLISECONDS)) {
                    try {
                    	file.uploadBackoff.reset();
                        drive.getRemote().files().trash(file.getGoogleId()).execute();
                    } finally {
                        file.uploadLock.unlock();
                    }
                } else {
                	throw new ConflictingOperationInProgressException();
                }
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		else if("update".equals(command) || "truncate".equals(command))
		{
			final File file = drive.getFile(UUID.fromString(logEntry[0]));
			final AtomicReference<ConflictingOperationInProgressException> exnCapture = new AtomicReference<>();
			final AtomicBoolean madeAttemptToGetLock = new AtomicBoolean(false);
			final com.google.api.services.drive.model.File newRemoteDirectory = drive.getRemote().files().get(file.getId()).execute();
			// all the fancy locking is mostly unnecessary, since we have # uploader threads
			// set to 1 currently. The only time it helps is when
			// we try to trash a file which is in the process of being uploaded.
			// In this case, the log player will block until the upload
			// is complete, so that the trashed file is actually trashed.
			updaterService.submit(new Runnable() {
				@Override
				public void run()
				{
					try
					{
						logger.info("Attempting to {} {}", command, file.getId());
						if (file.uploadLock.tryLock(file.uploadBackoff.nextBackOffMillis(), TimeUnit.MILLISECONDS)) {
							try {
								synchronized(file) {
									madeAttemptToGetLock.set(true);
									file.notify();
								}
								file.uploadBackoff.reset();
								file.scratchSpaceLock.lock(); // expected behavior: writes to large files during uploads will block,
																// for those big files only
								try {
									file.uploadFileContentsToGoogle(newRemoteDirectory);
								} finally {
									file.scratchSpaceLock.unlock();
								}
							} catch(IOException | SQLException e) {
								throw new RuntimeException(e);
							} finally {
								file.uploadLock.unlock();
							}
						} else {
							exnCapture.set(new ConflictingOperationInProgressException());
							logger.info("Conflict while uploading file: "+file.getGoogleId(), exnCapture.get());
                            synchronized(file) {
                            	madeAttemptToGetLock.set(true);
                                file.notify();
                            }
						}
					}
					catch(InterruptedException | IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			});
			synchronized(file) {
				while (!madeAttemptToGetLock.get()) {
					try
					{
						file.wait();
					}
					catch(InterruptedException e)
					{
						throw new RuntimeException(e);
					}
				}
			}
			if (exnCapture.get() != null) {
				throw exnCapture.get();
			}
		}
		else if("write".equals(command))
		{
//			final File file = drive.getFile(UUID.fromString(logEntry[0]));
//			file.lastWriteTime = System.currentTimeMillis();
//			file.freshWrite.set(true);
			// This actually needs to happen at the time of the write
		}
		else throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
	}
	
	String getGoogleId() throws IOException {
		return getGoogleId(drive, getLocalId());
	}
	
	static String getGoogleId(Drive drive, UUID localId)
	{
		try { return drive.getDatabase().getString("SELECT ID FROM FILES WHERE LOCALID=?", localId.toString()); }
		catch(NoSuchElementException e) { return null; }
	}

	public List<File> getParents() throws IOException
	{
		acquireRead();
		try
		{
			if(parents != null) return ImmutableList.copyOf(parents);

			if(parentsAsOfDate == null && metadataAsOfDate == null) readBasicMetadata(); // See if maybe it's just not in the memory cache (DB faster than Google)
			if(googleFileId == null || parentsAsOfDate != null)
			{
				// Fetch from database
				DuplicateRejectingList parents = new DuplicateRejectingList();
				List<String> files = drive.getDatabase().getStrings("SELECT CHILD FROM RELATIONSHIPS WHERE CHILD=?", googleFileId);
				
				for(String file : files) parents.add(drive.getCachedFile(file));
				playLogOnParentsList(parents);
				this.parents = parents;
				return ImmutableList.copyOf(parents);
			}
			
			updateParentsFromRemote();
			return ImmutableList.copyOf(parents);
		}
		finally
		{
			releaseRead();
		}
	}
	
	private void updateParentsFromRemote() throws IOException
	{
		if(parents != null && !drive.lock.isWriteLockedByCurrentThread()) throw new Error("Must have write lock");
		if(parents == null && !drive.lock.isWriteLockedByCurrentThread() && drive.lock.getReadLockCount() == 0) throw new Error("Must have read or write lock");
		DuplicateRejectingList parents = new DuplicateRejectingList();
		for(ParentReference parent : drive.getRemote().files().get(googleFileId).execute().getParents())
		{
			int rowsInDb = drive.getDatabase().getInteger("SELECT COUNT(*) FROM FILES WHERE ID=?", parent.getId());
			Date asof = new Date();
			if(rowsInDb > 0) parents.add(drive.getCachedFile(parent.getId()));
			else parents.add(drive.getFile(drive.getRemote().files().get(parent.getId()).execute(), asof));
		}
		
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

		acquireWrite();
		try
		{
			List<File> parents = getParents();
			String[] logEntry = new String[parents.size()+1];
			logEntry[0] = this.getLocalId().toString();
			for(int i = 0; i < parents.size(); i++) logEntry[i+1] = parents.get(i).getLocalId().toString();
			playOnDatabase("trash", logEntry);
			for(File parent : parents)
			{
				parent.acquireWrite();
				try { parent.playOnChildrenList(parent.children, "trash", logEntry); }
				finally { parent.releaseWrite(); }
			}
			dropFragmentsFromDb();
		}
		finally
		{
			releaseWrite();
		}
	}
	
	/**
	 * @param newRemoteDirectory A new Google remote file for which we have called create (synchronously).
	 * @throws IOException
	 * @throws SQLException
	 */
	void uploadFileContentsToGoogle(com.google.api.services.drive.model.File newRemoteDirectory) throws IOException, SQLException {
		System.out.println("uploading contents of " + getTitle() + " to google");
		logger.info("uploading contents of {} ({}) to google", getId(), getTitle());
		String type = Files.probeContentType(Paths.get(getUploadFile().getAbsolutePath()));
		FileContent mediaContent = new FileContent(type, getUploadFile());

		newRemoteDirectory.setMimeType(type);

		Date asof = new Date();
		newRemoteDirectory = drive.getRemote().files().update(getId(), newRemoteDirectory, mediaContent).execute();

		logger.info("done uploading contents of {} ({}) to google", getId(), getTitle());

		acquireWrite();
		try {
			refresh(newRemoteDirectory, asof);
		} finally {
			releaseWrite();
		}
	}
	
	void dropFragmentsFromDb() throws IOException {
		acquireWrite();
		try {
			System.out.println("dropped fragments for file " + title);
			drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE LOCALID=?", getLocalId());
		} finally {
			releaseWrite();
		}
	}
	
	@Override
	public boolean equals(Object other)
	{
		if(!File.class.equals(other.getClass())) return false;
		if(this.googleFileId != null && this.googleFileId.equals(((File)other).googleFileId)) return true;
		if(this.localFileId != null && this.localFileId.equals(((File)other).localFileId)) return true;
		return false;
	}
    
	@Override
	public String toString()
	{
		return "File(" + googleFileId+", "+localFileId+", "+title + ")";
	}

	private void acquireRead()
	{
		drive.lock.readLock().lock();
	}
	
	private void acquireWrite()
	{
		drive.lock.writeLock().lock();
	}
	
	private void releaseRead()
	{
		drive.lock.readLock().unlock();
	}
	
	private void releaseWrite()
	{
		drive.lock.writeLock().unlock();
	}
}
