package com.gdrivefs.test.cases;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.GeneralSecurityException;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.test.util.DriveBuilder;

public class RandomAccessTests
{
	@Test
	public void testRandomAccessReads() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			{
				java.io.File test = builder.cleanMountedDirectory();
				java.io.File file = new java.io.File(test, "hello.txt");
				FileUtils.writeStringToFile(file, "Hello World!");
				builder.flush();
				
				RandomAccessFile raf = new RandomAccessFile(file, "r");
				try
				{
					byte[] bytes = new byte[5];
					raf.seek(6);
					raf.read(bytes);
					Assert.assertEquals("World", new String(bytes));
					raf.seek(11);
					Assert.assertEquals('!', raf.read());
					raf.seek(0);
					raf.read(bytes);
					Assert.assertEquals("Hello", new String(bytes));
					raf.close();
				}
				finally
				{
					raf.close();
				}
			}
		}
		finally
		{
			builder.close();
		}
	}
}
