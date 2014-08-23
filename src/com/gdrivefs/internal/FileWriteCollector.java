package com.gdrivefs.internal;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.gdrivefs.simplecache.File;

public class FileWriteCollector
{
	java.io.File diskFile;
	File cachedFile;
	RandomAccessFile delegate;
	long currentPosition = 0;
	long prevPosition = currentPosition;
	
	public FileWriteCollector(File cachedFile, String name) throws IOException
	{
		this.cachedFile = cachedFile;
		this.diskFile = new java.io.File(new java.io.File(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "scratchspace"), UUID.randomUUID().toString()), name);
	}
	
	public void write(long position, ByteBuffer buf, long length) throws IOException
	{
		if(buf.limit() != length) throw new IOException("Unexpected discrepency!");
		if(buf.capacity() != length) throw new IOException("Unexpected discrepency!");
		if(delegate == null)
		{
			diskFile.getParentFile().mkdirs();
			delegate = new RandomAccessFile(diskFile, "rw");
		}
		
		if(position != currentPosition) {
			flushFragmentToDb(prevPosition, currentPosition);
			prevPosition = currentPosition = position;
		}
		delegate.getChannel().write(buf, currentPosition);
		currentPosition += length;
	}
	
	public java.io.File getFile() throws IOException
	{
		delegate.close();
		currentPosition = -1;
		return diskFile;
	}

	public void flushCurrentFragmentToDb() throws IOException {
		flushFragmentToDb(prevPosition, currentPosition);
		prevPosition = currentPosition;
	}
	
	private void flushFragmentToDb(long start, long stop) throws IOException {
		if (start > stop) {
			throw new IllegalArgumentException(start + ", " + stop);
		}
		if (start == stop) {
			return;
		}
		long position = start;
		int len = (int)(stop-start);
		byte[] fragment = new byte[len];
		delegate.seek(position);
		delegate.read(fragment, 0, len);
		cachedFile.write(fragment, position, diskFile);
	}
}
