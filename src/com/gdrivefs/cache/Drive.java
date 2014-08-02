package com.gdrivefs.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.services.drive.model.ChildList;
import com.google.api.services.drive.model.ChildReference;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.MemoryDatabase;

public class Drive
{
	Database db;
	com.google.api.services.drive.Drive remote;
	
	public Drive(com.google.api.services.drive.Drive remote)
	{
		this.db = new MemoryDatabase();
		db.execute("CREATE TABLE FILES(ID VARCHAR(255), PARENT VARCHAR(255), TITLE VARCHAR(255) NOT NULL, MD5HEX CHAR(32), SIZE INT, MTIME TIMESTAMP, OBSERVED TIMESTAMP)");
		
		this.remote = remote;
	}
	
	com.google.api.services.drive.Drive getRemote()
	{
		return remote;
	}
	
	Database getDatabase()
	{
		return db;
	}
	
	public File getRoot() throws IOException
	{
		String rootId = remote.about().get().execute().getRootFolderId();
		return new File(this, remote.files().get(rootId).execute());
	}
}
