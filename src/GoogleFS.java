
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.gdrivefs.cache.Drive;
import com.gdrivefs.cache.File;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.FileList;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.MemoryDatabase;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterAssumeImplemented;

public class GoogleFS extends FuseFilesystemAdapterAssumeImplemented
{
	Drive drive;
	Database db = new MemoryDatabase();
	
	public GoogleFS(Drive drive, HttpTransport transport)
	{
		this.drive = drive;
	}
	
	public static void main(final String... args) throws FuseException, GeneralSecurityException, IOException
	{
		if (args.length != 1) {
			System.err.println("Usage: MemoryFS <mountpoint>");
			System.exit(1);
		}

		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

		FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "auth"));
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV", Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory).build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		
		com.google.api.services.drive.Drive remote = new com.google.api.services.drive.Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("GDrive").build();
		com.gdrivefs.cache.Drive drive = new com.gdrivefs.cache.Drive(remote, httpTransport);
		
		new GoogleFS(drive, httpTransport).log(true).mount(args[0]);
	}

	@Override
	public int access(final String path, final int access)
	{
		return 0;
	}

	@Override
	public int create(final String path, final ModeWrapper mode, final FileInfoWrapper info)
	{
		throw new Error("unimplemented!");
		/*
		if (getPath(path) != null) {
			return -ErrorCodes.EEXIST();
		}
		final MemoryPath parent = getParentPath(path);
		if (parent instanceof MemoryDirectory) {
			((MemoryDirectory) parent).mkfile(getLastComponent(path));
			return 0;
		}
		return -ErrorCodes.ENOENT();
		*/
	}

	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		try
		{
			File f = getCachedPath(drive, path);
			
			if(f.isDirectory())
			{
				stat.setMode(NodeType.DIRECTORY);
				return 0;
			}
			else
			{
				stat.setMode(NodeType.FILE, true, true, false, false, false, false, false, false, false).size(f.getSize()).mtime(f.getModified().getTime()/1000);
				return 0;
			}
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();			
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}
	

	private String getLastComponent(String path)
	{
		while (path.substring(path.length() - 1).equals("/")) {
			path = path.substring(0, path.length() - 1);
		}
		if (path.isEmpty()) {
			return "";
		}
		return path.substring(path.lastIndexOf("/") + 1);
	}

	private File getParentPath(String path) throws IOException
	{
		while(path.endsWith("/")) path = path.substring(0, path.length()-1);
		path = path.substring(0, path.lastIndexOf("/"));
		if("".equals(path)) return getCachedPath(drive, "/");
		else return getCachedPath(drive, path);
	}

	@Override
	public int mkdir(String path, final ModeWrapper mode)
	{
		System.out.println(path);
		while(path.endsWith("/")) path = path.substring(0, path.length()-1);
		
		try
		{
			try { getCachedPath(drive, path); return -ErrorCodes.EEXIST(); }
			catch(NoSuchElementException e) { /* Do nothing, the directory doesn't yet exist */ }
			
			File parent;
			try { parent = getParentPath(path); }
			catch(NoSuchElementException e) { return -ErrorCodes.ENOENT(); }
			
			parent.mkdir(path.substring(path.lastIndexOf('/')+1));
			return 0;
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
		
		/*
		if (getPath(path) != null) {
			
		}
		final MemoryPath parent = getParentPath(path);
		if (parent instanceof MemoryDirectory) {
			((MemoryDirectory) parent).mkdir(getLastComponent(path));
			return 0;
		}
		
		*/
	}

	@Override
	public int open(final String path, final FileInfoWrapper info)
	{
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		/*
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		return ((MemoryFile) p).read(buffer, size, offset);
		*/
		
		try
		{
			for(File f : getCachedPath(drive, path).getChildren())
				if(f.getTitle().equals(path.substring(1)))
				{
					// Error checking
					if(f.isDirectory()) return -ErrorCodes.EISDIR();
					
					byte[] bytes = f.read(size, offset);
					buffer.put(bytes);
					return bytes.length;
				}
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		try
		{
			File directory = getCachedPath(drive, path);
			if(!directory.isDirectory()) return -ErrorCodes.ENOTDIR();
			for(File child : directory.getChildren()) 
				filler.add(child.getTitle());
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
        
		return 0;
	}
	
	private static File getCachedPath(Drive drive, String localPath) throws IOException
	{
		if(!localPath.startsWith("/")) throw new IllegalArgumentException("Expected local path to start with a slash ("+localPath+")");
		String[] pathElements = localPath.split("/");
		
		File current = drive.getRoot();
		for(int i = 1; i < pathElements.length; i++)
		{
			if("".equals(pathElements[i])) continue;
			
			File candidateChild = null;
			for(File child : current.getChildren()) 
				if(pathElements[i].equals(child.getTitle()))
					if(candidateChild == null) candidateChild = child;
					else throw new RuntimeException("Path is ambiguous: "+localPath);
			
			if(candidateChild == null) throw new NoSuchElementException(localPath);
			current = candidateChild;
		}
		
		return current;
	}

	@Override
	public int rename(final String path, final String newName)
	{
		throw new Error("unimplemented!");
		/*
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		final MemoryPath newParent = getParentPath(newName);
		if (newParent == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(newParent instanceof MemoryDirectory)) {
			return -ErrorCodes.ENOTDIR();
		}
		p.delete();
		p.rename(newName.substring(newName.lastIndexOf("/")));
		((MemoryDirectory) newParent).add(p);
		return 0;
		*/
	}

	@Override
	public int rmdir(final String path)
	{
		try
		{
			File directory = getCachedPath(drive, path);
			if(!directory.isDirectory()) return -ErrorCodes.ENOTDIR();
			if(directory.getChildren().size() != 0) throw new RuntimeException("Can not delete non-empty directory");
			directory.delete();
			return 0;
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public int truncate(final String path, final long offset)
	{
		throw new Error("unimplemented!");
		/*
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		((MemoryFile) p).truncate(offset);
		return 0;
		*/
	}

	@Override
	public int unlink(final String path)
	{
		throw new Error("unimplemented!");
		/*
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		p.delete();
		return 0;
		*/
	}

	@Override
	public int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper)
	{
		throw new Error("unimplemented!");
		/*
		final MemoryPath p = getPath(path);
		if (p == null) {
			return -ErrorCodes.ENOENT();
		}
		if (!(p instanceof MemoryFile)) {
			return -ErrorCodes.EISDIR();
		}
		return ((MemoryFile) p).write(buf, bufSize, writeOffset);
		*/
	}
	
}
