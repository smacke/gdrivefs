package com.gdrivefs.simplecache;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.api.client.http.HttpTransport;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.MemoryDatabase;

/**
 * Represents a cached version of the GoogleDrive for a particular user.
 * Cache utilizes a relational database to store metadata and file fragments.
 * Users can get a root file by calling drive.getRoot()
 */
public class Drive implements Closeable
{
	private Database db;
    private HttpTransport transport;
	private com.google.api.services.drive.Drive remote;
	
	// Drive-wide singularity cache
	// TODO: Use soft references and clear the map when references are dropped
	Map<String, File> googleFiles = new HashMap<String, File>();
	Map<UUID, File> unsyncedFiles = new HashMap<UUID, File>();

	final ExecutorService logPlayer = Executors.newSingleThreadExecutor();
	
	String rootId;
	
	public Drive(com.google.api.services.drive.Drive remote, HttpTransport transport)
	{
		this.db = new MemoryDatabase();
		
		db.execute("CREATE TABLE DRIVES(ROOT VARCHAR(255))");
		db.execute("CREATE TABLE FILES(ID VARCHAR(255), LOCALID CHAR(36) NOT NULL, TITLE VARCHAR(255) NOT NULL, MIMETYPE VARCHAR(255) NOT NULL, MD5HEX CHAR(32), SIZE BIGINT, MTIME TIMESTAMP, DOWNLOADURL CLOB, METADATAREFRESHED TIMESTAMP, CHILDRENREFRESHED TIMESTAMP, PARENTSREFRESHED TIMESTAMP)");
		db.execute("CREATE TABLE RELATIONSHIPS(PARENT VARCHAR(255), CHILD VARCHAR(255))");
		db.execute("CREATE TABLE FRAGMENTS(FILEMD5 CHAR(32) NOT NULL, CHUNKMD5 CHAR(32) NOT NULL, STARTBYTE INT NOT NULL, ENDBYTE INT NOT NULL)");
		
		// UPDATELOG contains operations that have logically happened on the local memory model but may not have been synced with Google's servers.
		// The persistent database tables represent the state of the world, and the memory model represents the logical state of localhost (the difference is stored in this table)
		// When updating the memory model, you must select from the other tables and then iterate over this table to replay changes that have yet to be sync'd
		// Rows from this table may be played somewhat out of order (eg. uploads take a long time and might be delayed, while deletes might happen immediately), though in-order is ideal) so long as it doesn't break any individual file's logical view of the world
		// ID allows the table to be sorted by logical event ID for replaying, isdone indicates if Google should be aware of the change, and details stores details of the task
		db.execute("CREATE TABLE UPDATELOG(ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), CATEGORY VARCHAR(64), COMMAND VARCHAR(64), ISDONE SMALLINT DEFAULT 0, DETAILS CLOB)");

		db.execute("CREATE UNIQUE INDEX FILE_ID ON FILES(ID)");
		db.execute("CREATE UNIQUE INDEX RELATIONSHIPS_CHILD_PARENT ON RELATIONSHIPS(CHILD, PARENT)");
		db.execute("CREATE INDEX FRAGMENTS_FILEMD5 ON FRAGMENTS(FILEMD5)");

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
	
	public synchronized File getRoot() throws IOException
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
		
		return getFile(rootId);
	}
	
	public synchronized File getFile(String id) throws IOException
	{
		File file = googleFiles.get(id);
		if(file == null)
		{
			file = new File(this, id);
			googleFiles.put(file.getId(), file);
		}
		return file;
	}
	
	public synchronized File getFile(UUID id) throws IOException
	{
		String googleId = File.getGoogleId(this, id);
		if(googleId != null)
		{
			unsyncedFiles.remove(id);
			return googleFiles.get(googleId);
		}
		File file = unsyncedFiles.get(id);
		if(file == null)
		{
			file = new File(this, id);
			unsyncedFiles.put(id, file);
		}
		return file;
	}
	
	HttpTransport getTransport()
	{
	//	new Throwable("Note: Getting remote").printStackTrace();
		return transport;
	}
	
	void pokeLogPlayer()
	{
		final Drive drive = this;
		logPlayer.execute(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					File.playLogOnRemote(drive);
				}
				catch(IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		});
	}

	@Override
	public void close() throws IOException
	{
		logPlayer.shutdownNow();
		try
		{
			logPlayer.awaitTermination(30, TimeUnit.SECONDS);
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
		db.close();
	}
}
