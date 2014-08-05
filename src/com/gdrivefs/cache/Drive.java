package com.gdrivefs.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.api.client.http.HttpTransport;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.MemoryDatabase;

/**
 * Represents a cached version of the GoogleDrive for a particular user.
 * Cache utilizes a relational database to store metadata and file fragments.
 * Users can get a root file by calling drive.getRoot()
 */
public class Drive
{
	Database db;
    HttpTransport transport;
	com.google.api.services.drive.Drive remote;
	
	// Drive-wide singularity cache
	// TODO: Use soft references and clear the map when references are dropped
	Map<String, File> files = new HashMap<String, File>();
	
	String rootId;
	
	public Drive(com.google.api.services.drive.Drive remote, HttpTransport transport)
	{
		this.db = new MemoryDatabase();
		db.execute("CREATE TABLE DRIVES(ROOT VARCHAR(255))");
		db.execute("CREATE TABLE FILES(ID VARCHAR(255), TITLE VARCHAR(255) NOT NULL, MIMETYPE VARCHAR(255) NOT NULL, MD5HEX CHAR(32), SIZE INT, MTIME TIMESTAMP, DOWNLOADURL CLOB, METADATAREFRESHED TIMESTAMP, CHILDRENREFRESHED TIMESTAMP)");
		db.execute("CREATE TABLE RELATIONSHIPS(PARENT VARCHAR(255), CHILD VARCHAR(255))");
        db.execute("CREATE TABLE FRAGMENTS(FILEMD5 CHAR(32) NOT NULL, CHUNKMD5 CHAR(32) NOT NULL, STARTBYTE INT NOT NULL, ENDBYTE INT NOT NULL)");
        db.execute("CREATE UNIQUE INDEX FILE_ID ON FILES(ID)");
        db.execute("CREATE UNIQUE INDEX RELATIONSHIPS_CHILD_PARENT ON RELATIONSHIPS(CHILD, PARENT)");
        db.execute("CREATE INDEX FRAGMENTS_FILEMD5 ON FRAGMENTS(FILEMD5)");
		this.remote = remote;
		this.transport = transport;
	}
	
	com.google.api.services.drive.Drive getRemote()
	{
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
		File file = files.get(id);
		if(file == null)
		{
			file = new File(this, id);
			files.put(file.getId(), file);
		}
		file.considerAsyncDirectoryRefresh(10*60*1000);
		return file;
	}
}
