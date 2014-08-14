package com.gdrivefs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.FuseJna;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.XattrFiller;
import net.fusejna.XattrListFiller;
import net.fusejna.types.TypeMode.ModeWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterAssumeImplemented;

import com.gdrivefs.internal.FileWriteCollector;
import com.gdrivefs.simplecache.Drive;
import com.gdrivefs.simplecache.File;
import com.google.api.client.http.HttpTransport;
import com.jimsproch.sql.Database;
import com.jimsproch.sql.MemoryDatabase;

public class GoogleDriveLinuxFs extends FuseFilesystemAdapterAssumeImplemented
{
	Drive drive;
	Database db = new MemoryDatabase();
	
	public GoogleDriveLinuxFs(Drive drive, HttpTransport transport)
	{
		this.drive = drive;
	}

	public GoogleDriveLinuxFs setLoggingStatus(boolean isEnabled)
	{
		super.log(isEnabled);
		return this;
	}
	
	public void flush(boolean flushUploads) throws InterruptedException
	{
		drive.flush(flushUploads);
	}
	
	@Override
	public void beforeMount(java.io.File mountPoint)
	{
		try
		{
			FuseJna.unmount(mountPoint);
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public synchronized File getRoot() throws IOException
	{
		return drive.getRoot();
	}

	@Override
	public int access(final String path, final int access)
	{
		return 0;
	}

	@Override
	public int create(String path, final ModeWrapper mode, final FileInfoWrapper info)
	{
		while(path.endsWith("/")) path = path.substring(0, path.length()-1);
		
		try
		{
			try { getCachedPath(path); return -ErrorCodes.EEXIST(); }
			catch(NoSuchElementException e) { /* Do nothing, the directory doesn't yet exist */ }
			
			File parent;
			try { parent = getParentPath(path); }
			catch(NoSuchElementException e) { return -ErrorCodes.ENOENT(); }
			
			parent.createFile(path.substring(path.lastIndexOf('/')+1));
			return 0;
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public int getattr(final String path, final StatWrapper stat)
	{
		try
		{
			File f = getCachedPath(path);
			
			if(f.isDirectory())
			{
				stat.setMode(NodeType.DIRECTORY, true, true, true, false, false, false, false, false, false);
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
		catch(AmbiguousPathException e)
		{
			return -ErrorCodes.ENOTUNIQ();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		catch(Throwable e)
		{
			e.printStackTrace();
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
		if("/".equals(path)) return drive.getRoot();
		while(path.endsWith("/")) path = path.substring(0, path.length()-1);
		path = path.substring(0, path.lastIndexOf("/"));
		if("".equals(path)) return getCachedPath("/");
		else return getCachedPath(path);
	}

	@Override
	public int mkdir(String path, final ModeWrapper mode)
	{
		System.out.println(path);
		while(path.endsWith("/")) path = path.substring(0, path.length()-1);
		
		try
		{
			try { getCachedPath(path); return -ErrorCodes.EEXIST(); }
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
	}

	@Override
	public int open(final String path, final FileInfoWrapper info)
	{
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info)
	{
		try
		{
			File f = getCachedPath(path);

			if(f.isDirectory()) return -ErrorCodes.EISDIR();

			boolean isGoogleDoc = f.getMimeType().startsWith("application/vnd.google-apps.");
			if(isGoogleDoc) return -ErrorCodes.EMEDIUMTYPE();

			byte[] bytes = f.read(Math.min(size, f.getSize()-offset), offset);
			buffer.put(bytes);
			return bytes.length;
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler)
	{
		try
		{
			File directory = getCachedPath(path);
			getParentPath(path).considerAsyncDirectoryRefresh(2, TimeUnit.MINUTES);
			if(!directory.isDirectory()) return -ErrorCodes.ENOTDIR();
			for(File child : directory.getChildren())
			{
				// Hack to support slashes in file names (swap in and out a nearly identical UTF-8 character)
				filler.add(child.getTitle().replaceAll("/", "∕"));
			}
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
        
		return 0;
	}
	
	private File getCachedPath(String localPath) throws IOException
	{
		if(!localPath.startsWith("/")) throw new IllegalArgumentException("Expected local path to start with a slash ("+localPath+")");
		
		String[] pathElements = localPath.split("/");
		
		// Hack to support slashes in file names (swap in and out a nearly identical UTF-8 character)
		for(int i = 0; i < pathElements.length; i++)
			pathElements[i] = pathElements[i].replaceAll("∕", "/");
		
		File current = drive.getRoot();
		for(int i = 1; i < pathElements.length; i++)
		{
			if("".equals(pathElements[i])) continue;
			
			List<File> children = current.getChildren(pathElements[i]);
			if(children.isEmpty()) throw new NoSuchElementException(localPath);
			else if(children.size() == 1) current = children.get(0);
			else throw new AmbiguousPathException(localPath);
		}
		
		if(current.isDirectory()) current.considerAsyncDirectoryRefresh(1, TimeUnit.MINUTES);
		return current;
	}

	@Override
	public int rename(final String oldPath, final String newPath)
	{
		try
		{
			File file = getCachedPath(oldPath);
			File oldParent = getParentPath(oldPath);
			File newParent = getParentPath(newPath);
			String oldName = getLastComponent(oldPath);
			String newName = getLastComponent(newPath);
			
			if(!newParent.isDirectory()) return -ErrorCodes.ENOTDIR();
			
			// Rename is typically atomic on Linux, so systems will use it as a way of doing atomic writes by overriding destination
			try { getCachedPath(newPath).trash(); }
			catch(NoSuchElementException e) { /* destination didn't exist, which is fine */ }
			
			if(!oldParent.equals(newParent))
			{
				newParent.addChild(file);
				oldParent.removeChild(file);
			}
			
			if(!oldName.equals(newName))
			{
				file.setTitle(newName);
			}
			
			return 0;
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int rmdir(final String path)
	{
		try
		{
			File directory = getCachedPath(path);
			if(!directory.isDirectory()) return -ErrorCodes.ENOTDIR();
			
			if(directory.getParents().size() > 1) getParentPath(path).removeChild(directory);
			else
			{
				// TODO: Decide what to do if parent is in trash, maybe add drive root as parent?
				directory.trash();
			}
			return 0;
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int truncate(final String path, final long offset)
	{
		System.out.println("truncate: "+offset);
		return 0;
	}

	@Override
	public int unlink(final String path)
	{
		try
		{
			File file = getCachedPath(path);
			if(file.getParents().size() > 1) getParentPath(path).removeChild(file);
			else
			{
				// TODO: Decide what to do if parent is in trash, maybe add drive root as parent?
				file.trash();
			}
			return 0;
		}
		catch(NoSuchElementException e)
		{
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
	}
	
	public Map<String, FileWriteCollector> openFiles = new HashMap<String, FileWriteCollector>();

	@Override
	public synchronized int write(final String path, final ByteBuffer buf, final long bufSize, final long writeOffset, final FileInfoWrapper wrapper)
	{
		FileWriteCollector collector = openFiles.get(path);
		try
		{
			if(collector == null)
			{
				// we cannot guarantee consistency for files that are being written while being moved. :(
				File f = getCachedPath(path);
				collector = new FileWriteCollector(f, path.substring(path.lastIndexOf('/')+1));
				openFiles.put(path, collector);
			}
			
			collector.write(writeOffset, buf, bufSize);
			
			return (int)bufSize;
		}
		catch(NoSuchElementException e)
		{
			try{collector.getFile();}
			catch(Throwable t){t.printStackTrace();}
			openFiles.remove(path);
			return -ErrorCodes.ENOENT();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			try{collector.getFile();}
			catch(Throwable t){t.printStackTrace();}
			openFiles.remove(path);
			return -ErrorCodes.EIO();
		}
	}
	
	@Override
	public synchronized int flush(String path, FileInfoWrapper info)
	{
		FileWriteCollector collector = openFiles.get(path);
		try
		{
			if(collector == null) return 0;
			collector.flushCurrentFragmentToDb();
			getCachedPath(path).update(collector.getFile());
			openFiles.remove(path);
			collector.getFile();
			return 0;
		}
		catch(IOException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
		catch(RuntimeException e)
		{
			e.printStackTrace();
			return -ErrorCodes.EIO();
		}
	}

	@Override
	public int fsync(String path, int datasync, FileInfoWrapper info)
	{
		return flush(path, info);
	}

	@Override
	public int getxattr(final String path, final String xattr, final XattrFiller filler, final long size, final long position)
	{
		return super.getxattr(path, xattr, filler, size, position);
		/*
		if (!path.equals(filename)) {
			return -ErrorCodes.firstNonNull(ErrorCodes.ENOATTR(), ErrorCodes.ENOATTR(), ErrorCodes.ENODATA());
		}
		if (!helloTxtAttrs.containsKey(xattr)) {
			return -ErrorCodes.firstNonNull(ErrorCodes.ENOATTR(), ErrorCodes.ENOATTR(), ErrorCodes.ENODATA());
		}
		filler.set(helloTxtAttrs.get(xattr));
		return 0;
		*/
	}

	@Override
	public int listxattr(final String path, final XattrListFiller filler)
	{
		return super.listxattr(path, filler);
		/*
		if (!path.equals(filename)) {
			return -ErrorCodes.ENOTSUP();
		}
		filler.add(helloTxtAttrs.keySet());
		return 0;
		*/
	}

	@Override
	public int removexattr(final String path, final String xattr)
	{
		return super.removexattr(path, xattr);
		/*
		if (!path.equals(filename)) {
			return -ErrorCodes.ENOTSUP();
		}
		if (!helloTxtAttrs.containsKey(xattr)) {
			return -ErrorCodes.ENOATTR();
		}
		helloTxtAttrs.remove(xattr);
		return 0;
		*/
	}

	@Override
	public int setxattr(final String path, final String xattrName, final ByteBuffer buf, final long size, final int flags,
			final int position)
	{
		return super.setxattr(path, xattrName, buf, size, flags, position);
		/*
		if (!path.equals(filename)) {
			return -ErrorCodes.ENOTSUP();
		}
		byte[] attr;
		if (!helloTxtAttrs.containsKey(xattr)) {
			attr = new byte[(int) (size + position)];
			helloTxtAttrs.put(xattr, attr);
		}
		else {
			attr = helloTxtAttrs.get(xattr);
			if (attr.length < size + position) {
				attr = Arrays.copyOf(attr, (int) (size + position));
			}
		}
		buf.get(attr, position, (int) size);
		return 0;
		*/
	}
	
	@Override
	public void destroy()
	{
		RuntimeException exception = null;
		
		super.destroy();
		
		try
		{
			for (FileWriteCollector collector : openFiles.values()) {
				collector.flushCurrentFragmentToDb();
			}
			drive.close();
		}
		catch(IOException e)
		{
			if(exception == null) exception = new RuntimeException(e);
		}
		catch(RuntimeException e)
		{
			if(exception == null) exception = new RuntimeException(e);
		}
		
		try
		{
			unmount();
		}
		catch(IOException e)
		{
			if(exception == null) exception = new RuntimeException(e);
		}
		catch(FuseException e)
		{
			if(exception == null) exception = new RuntimeException(e);
		}
		
		synchronized(this) { notifyAll(); }
		
		System.out.println(isMounted());
		
		if(exception != null)
		{
			exception.printStackTrace();
			throw new RuntimeException(exception);
		}
	}
	
}
