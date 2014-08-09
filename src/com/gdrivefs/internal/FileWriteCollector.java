package com.gdrivefs.internal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.UUID;

public class FileWriteCollector
{
	File file;
	RandomAccessFile delegate;
	long currentPosition = 0;
	
	public FileWriteCollector(String name) throws IOException
	{
		file = new java.io.File(new java.io.File(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "scratchspace"), UUID.randomUUID().toString()), name);
	}
	
	public void write(long position, ByteBuffer buf, long length) throws IOException
	{
		if(buf.limit() != length) throw new IOException("Unexpected discrepency!");
		if(buf.capacity() != length) throw new IOException("Unexpected discrepency!");
		if(delegate == null)
		{
			file.getParentFile().mkdirs();
			delegate = new RandomAccessFile(file, "rw");
		}
		
		if(position != currentPosition) throw new IOException("Only linear writes are supported!");
		delegate.getChannel().write(buf, currentPosition);
		currentPosition += length;
	}
	
	public File getFile() throws IOException
	{
		delegate.close();
		currentPosition = -1;
		return file;
	}
}
