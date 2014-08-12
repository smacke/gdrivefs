package com.gdrivefs.test.cases;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.GeneralSecurityException;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.test.util.DriveBuilder;

public class FailingUnitTests
{
	@Test
	public void testTrivial() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			{
				File test = builder.cleanMountedDirectory();
				File helloFile = new File(test, "hello.txt");
				FileUtils.write(helloFile, "hello world");
				builder.flush();
				FileUtils.readFileToString(helloFile);
				FileUtils.write(helloFile, "hello JIM!");
				Assert.assertEquals("hello JIM!", FileUtils.readFileToString(helloFile));
			}
		}
		finally
		{
			builder.close();
		}
	}
	
	@Test
	public void testTruncateChangesSize() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			{
				java.io.File test = builder.cleanMountedDirectory();
				java.io.File helloFile = new java.io.File(test, "hello.txt");
				FileUtils.write(helloFile, "123456789");
			}
			
			builder.flush();
			
			{
				com.gdrivefs.simplecache.File test = builder.uncleanDriveDirectory();
				com.gdrivefs.simplecache.File helloFile = test.getChildren("hello.txt").get(0);
				Assert.assertEquals(9, helloFile.getSize());
				helloFile.truncate(5);
				Assert.assertEquals(5, helloFile.getSize());
			}
			
			builder.flush();

			{
				com.gdrivefs.simplecache.File test = builder.uncleanDriveDirectory();
				com.gdrivefs.simplecache.File helloFile = test.getChildren("hello.txt").get(0);
				Assert.assertEquals(5, helloFile.getSize());
			}
			
		}
		finally
		{
			builder.close();
		}
	}
	
	@Test
	public void testRandomAccessWrites() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			{
				java.io.File test = builder.cleanMountedDirectory();
				RandomAccessFile file = new RandomAccessFile(new java.io.File(test, "hello.txt"), "rw");
				file.writeBytes("00000");
				file.seek(3);
				file.writeBytes("456789");
				file.seek(0);
				file.writeBytes("123");
				file.close();
				Assert.assertEquals("123456789", FileUtils.readFileToString(new java.io.File(test, "hello.txt")));
			}
		}
		finally
		{
			builder.close();
		}
	}
}
