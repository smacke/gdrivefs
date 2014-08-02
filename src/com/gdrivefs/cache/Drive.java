package com.gdrivefs.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.api.client.http.HttpTransport;
import com.google.api.services.drive.model.ChildList;
import com.google.api.services.drive.model.ChildReference;
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
		db.execute("CREATE TABLE FILES(ID VARCHAR(255), PARENT VARCHAR(255), TITLE VARCHAR(255) NOT NULL, MD5HEX CHAR(32), SIZE INT, MTIME TIMESTAMP, OBSERVED TIMESTAMP, DOWNLOADURL CLOB)");
        db.execute("CREATE TABLE FRAGMENTS(FILEMD5 CHAR(32) NOT NULL, CHUNKMD5 CHAR(32) NOT NULL, STARTBYTE INT NOT NULL, ENDBYTE INT NOT NULL)");
        db.execute("CREATE INDEX FILE_ID ON FILES(ID)");
        db.execute("CREATE UNIQUE INDEX FILE_ID_PARENT ON FILES(ID, PARENT)");
        db.execute("CREATE UNIQUE INDEX FILE_PARENT ON FILES(ID, PARENT)");
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
		if(rootId == null) rootId = new File(this).getId();
		return getFile(rootId);
	}
	
	public synchronized File getFile(com.google.api.services.drive.model.File remote) throws IOException
	{
		File file = files.get(remote.getId());
		if(file == null)
		{
			file = new File(this, remote);
			files.put(file.getId(), file);
		}
		return file;
	}
	
	public synchronized File getFile(String id) throws IOException
	{
		File file = files.get(id);
		if(file == null)
		{
			try { file = new File(this, id); }
			catch(NoSuchElementException e) { file = new File(this, getRemote().files().get(id).execute()); }
			files.put(file.getId(), file);
		}
		return file;
	}
}
