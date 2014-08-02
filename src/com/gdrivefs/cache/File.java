package com.gdrivefs.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.api.services.drive.model.FileList;


/**
 * File represents a particular remote file (as represented by Google's file ID), but provides a clean interface for performing reads and writes
 * through the localhost cache layer.
 */
public class File
{
	String id;
	Drive drive;
	
	File(Drive drive, com.google.api.services.drive.model.File file) throws IOException
	{
		this.drive = drive;
		this.id = file.getId();
		
		observeFile(drive, file);
	}
	
	String getId()
	{
		return id;
	}
	
	public List<File> getChildren()
	{
		java.util.List<File> children = new ArrayList<File>();
		
		if(!isDirectory()) return children;
		
		try
		{
			com.google.api.services.drive.Drive.Files.List lst = drive.getRemote().files().list().setQ("'"+id+"' in parents and trashed=false");

			do
			{
				try
				{
					FileList files = lst.execute();
					drive.getDatabase().execute("DELETE FROM FILES WHERE PARENT=?", id);
					for(com.google.api.services.drive.model.File child : files.getItems()) children.add(new File(drive, child));
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
		drive.getDatabase().execute("INSERT INTO FILES(ID, TITLE, MD5HEX, SIZE, MTIME) VALUES(?,?,?,?,?)", file.getId(), file.getTitle(), file.getMd5Checksum(), file.getQuotaBytesUsed(), new Date(file.getModifiedDate().getValue()));
	}
	
	public String getTitle() throws IOException
	{
		return drive.getDatabase().getString("SELECT TITLE FROM FILES WHERE ID=?", id);
	}
	
	public boolean isDirectory()
	{
		return drive.getDatabase().getString("SELECT MD5HEX FROM FILES WHERE ID=?", id) == null;
	}
}
