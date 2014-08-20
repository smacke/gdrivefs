package com.gdrivefs.simplecache;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gdrivefs.simplecache.internal.DriveExecutorService;
import com.google.api.client.http.HttpTransport;
import com.google.api.services.drive.model.Property;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.MemoryDatabase;

/**
 * Represents a cached version of the GoogleDrive for a particular user.
 * Cache utilizes a relational database to store metadata and file fragments.
 * Users can get a root file by calling drive.getRoot()
 * 
 * This package is designed to be thread-safe, but threads should not be interrupted while in the drive/file code path.
 * Users should invoke the flush() method to ensure changes are durable on disk, but should note that large file uploads may take a long time to flush!
 */
public class Drive implements Closeable
{
	private Database db;
    private HttpTransport transport;
	private com.google.api.services.drive.Drive remote;
	
	// Drive-wide singularity cache
	// TODO: Use soft references and clear the map when references are dropped
	Map<String, File> googleFiles = Maps.newConcurrentMap();
	Map<UUID, File> unsyncedFiles = Maps.newConcurrentMap();

	DriveExecutorService logPlayer = new DriveExecutorService();
	DriveExecutorService fileUpdateWorker = new DriveExecutorService(new ThreadFactoryBuilder().setDaemon(true).build());

	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	String rootId;
	
	public Drive(com.google.api.services.drive.Drive remote, HttpTransport transport)
	{
		this.db = new MemoryDatabase();
		
		db.execute("CREATE TABLE DRIVES(ROOT VARCHAR(255))");
		db.execute("CREATE TABLE FILES(ID VARCHAR(255), LOCALID CHAR(36) NOT NULL, TITLE VARCHAR(255) NOT NULL, MIMETYPE VARCHAR(255) NOT NULL, MD5HEX CHAR(32), SIZE BIGINT, MTIME TIMESTAMP, DOWNLOADURL CLOB, METADATAREFRESHED TIMESTAMP, CHILDRENREFRESHED TIMESTAMP, PARENTSREFRESHED TIMESTAMP)");
		db.execute("CREATE TABLE RELATIONSHIPS(PARENT VARCHAR(255), CHILD VARCHAR(255))");
		db.execute("CREATE TABLE FRAGMENTS(LOCALID CHAR(36) NOT NULL, FILEMD5 CHAR(32), CHUNKMD5 CHAR(32) NOT NULL, STARTBYTE INT NOT NULL, ENDBYTE INT NOT NULL)");
		
		// UPDATELOG contains operations that have logically happened on the local 
		// memory model but may not have been synced with Google's servers.
		// The persistent database tables represent the state of the world,
		// and the memory model represents the logical state of localhost (the difference is stored in this table)
		// When updating the memory model, you must select from the other tables and then iterate over this table 
		// to replay changes that have yet to be sync'd
		// Rows from this table may be played somewhat out of order 
		// (eg. uploads take a long time and might be delayed, while deletes might happen immediately), though in-order is ideal)
		// so long as it doesn't break any individual file's logical view of the world
		// ID allows the table to be sorted by logical event ID for replaying, isdone indicates
		// if Google should be aware of the change, and details stores details of the task
		db.execute("CREATE TABLE UPDATELOG(ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), COMMAND VARCHAR(64), ISDONE SMALLINT DEFAULT 0, DETAILS CLOB)");

		db.execute("CREATE UNIQUE INDEX FILE_ID ON FILES(ID)");
		db.execute("CREATE UNIQUE INDEX RELATIONSHIPS_CHILD_PARENT ON RELATIONSHIPS(CHILD, PARENT)");
//		db.execute("CREATE INDEX FRAGMENTS_FILEMD5 ON FRAGMENTS(FILEMD5)");

		this.remote = remote;
		this.transport = transport;

		java.io.File home = new java.io.File(System.getProperty("user.home"), ".googlefs");
		new java.io.File(home, "cache").mkdirs();
		new java.io.File(home, "uploads").mkdirs();
		new java.io.File(home, "auth").mkdirs();
	}
	
	com.google.api.services.drive.Drive getRemote()
	{
	//	new Throwable("Note: Getting remote").printStackTrace();
		return remote;
	}
	
	Database getDatabase()
	{
		return db;
	}
	
	public boolean isShutdown()
	{
		return logPlayer.isShutdown();
	}
	
	public File getRoot() throws IOException
	{
		lock.readLock().lock();
		try
		{
			if(rootId == null)
				try
				{
					rootId = getDatabase().getString("SELECT ROOT FROM DRIVES");
				}
				catch(NoSuchElementException e)
				{
					rootId = getRemote().about().get().execute().getRootFolderId();
					getDatabase().execute("INSERT INTO DRIVES(ROOT) VALUES(?)", rootId);
				}
			
			File rootFile = googleFiles.get(rootId);
			if(rootFile != null) return rootFile;
			rootFile = new File(this, rootId);
			googleFiles.put(rootId, rootFile);
			return rootFile;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	File getFile(final com.google.api.services.drive.model.File remoteFile, final Date asof) throws IOException
	{
		lock.readLock().lock();
		try
		{
			if(lock.getReadLockCount() == 0 && !lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
			
			File file = googleFiles.get(remoteFile.getId());
			if(file == null && remoteFile.getProperties() != null)
				for(Property property : remoteFile.getProperties())
				{
					if("com.gdrivefs.id".equals(property.getKey()))
					{
						UUID localFileId = UUID.fromString(property.getValue());
						if(unsyncedFiles.containsKey(localFileId))
						{
							file = unsyncedFiles.remove(localFileId);
							googleFiles.put(remoteFile.getId(), file);
						}
					}
				}
			if(file == null && remoteFile.getDescription() != null && remoteFile.getDescription().startsWith("gdrivefsid="))
			{
				
				UUID localFileId = UUID.fromString(remoteFile.getDescription().substring("gdrivefsid=".length()));
				if(unsyncedFiles.containsKey(localFileId))
				{
					file = unsyncedFiles.remove(localFileId);
					googleFiles.put(remoteFile.getId(), file);
				}
			}
			if(file == null)
			{
				file = new File(this, remoteFile.getId());
				googleFiles.put(remoteFile.getId(), file);
			}
			
			// Now the file is guaranteed to exist, to be in the google cache, and not in the unsynced map.
			
			lock.readLock().lock();
			try
			{
				// If we have a write lock and invalid cache, we can refresh, otherwise schedule an async refresh.
				final File fileReference = file;
				if(file.metadataAsOfDate == null) file.refresh(remoteFile, asof);
				else fileUpdateWorker.execute(new Runnable()
				{
					@Override
					public void run()
					{
						lock.writeLock().lock();
						try
						{
							fileReference.refresh(remoteFile, asof);
						}
						catch(IOException e)
						{
							throw new RuntimeException(e);
						}
						catch(SQLException e)
						{
							throw new RuntimeException(e);
						}
						finally
						{
							lock.writeLock().unlock();
						}
						
					}
				});
			}
			catch(SQLException e)
			{
				throw new RuntimeException(e);
			}
			finally
			{
				lock.readLock().unlock();
			}
			
			return file;
		}
		finally
		{
			lock.readLock().unlock();
		}
		
	}
	
	File getCachedFile(String googleId)
	{
		lock.readLock().lock();
		try
		{
			if(lock.getReadLockCount() == 0 && !lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
			File file = googleFiles.get(googleId);
			if(file != null) return file;
			file = new File(this, googleId);
			googleFiles.put(googleId, file);
			return file;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	File getFile(UUID id) throws IOException
	{
		lock.readLock().lock();
		try
		{
			if(lock.getReadLockCount() == 0 && !lock.isWriteLockedByCurrentThread()) throw new Error("Read or write lock required");
			String googleId = File.getGoogleId(this, id);
			if(googleId != null && googleFiles.containsKey(googleId)) return googleFiles.get(googleId);
			
			if(googleId == null)
			{
				// Get unsynced file or create a new one
				File file = unsyncedFiles.get(id);
				if(file != null) return file;
				file = new File(this, id);
				unsyncedFiles.put(id, file);
				return file;
			}
			
			// Get the unsynced file, move it to the google list, return file
			File file = unsyncedFiles.get(id);
			if(file == null)
			{
				file = new File(this, googleId);
			}
			googleFiles.put(googleId, file);
			unsyncedFiles.remove(id);
			return file;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	HttpTransport getTransport()
	{
	//	new Throwable("Note: Getting remote").printStackTrace();
		return transport;
	}
	
	void pokeLogPlayer()
	{
		final Drive drive = this;
		
		try
		{
			if(!logPlayer.isShutdown())
				logPlayer.execute(new Runnable()
				{
					@Override
					public void run()
					{
						try
						{
							synchronized(logPlayer)
							{
								if(!logPlayer.isShutdown()) File.playLogEntryOnRemote(drive);
							}
						}
						catch(IOException e)
						{
							throw new RuntimeException(e);
						}
						catch(SQLException e)
						{
							throw new RuntimeException(e);
						}
					}
				});
		}
		catch(Exception e)
		{
			System.out.println("note: could not poke player due to: "+e.getMessage());
		}
	}
	
	/**
	 * 
	 * @param flushUploads - if false, only metadata changes will be flushed.  If true, this method will block even on large file uploads.
	 * @throws InterruptedException
	 */
	public void flush(boolean flushUploads) throws InterruptedException
	{
		logPlayer.flush();
	}

	@Override
	public void close() throws IOException
	{
		if(db == null) return; // already closed
		
		logPlayer.shutdownNow();
		fileUpdateWorker.shutdownNow();
		
		try
		{
			// Wait up to a minute for the logPlayer.
			// If the logPlayer finished in less than six seconds, give the fileUpdateWorker up to those six seconds to finish.
			long end = System.currentTimeMillis()+30*1000;
			logPlayer.awaitTermination(60, TimeUnit.SECONDS);
			fileUpdateWorker.awaitTermination(Math.max(end-System.currentTimeMillis(), 0), TimeUnit.MILLISECONDS);
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
		
		db.close();
		db = null;
	}
}
