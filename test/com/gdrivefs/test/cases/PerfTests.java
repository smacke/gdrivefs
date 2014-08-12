package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;

public class PerfTests
{
	@Test
	public void testChildrenOfEmptyDirectory() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			
			// Warm the cache
			directory.getChildren();
			
			int count = 0;
			long start = System.currentTimeMillis();
			while(System.currentTimeMillis()-start < 1000)
			{
				Assert.assertEquals(0, directory.getChildren().size());
				count++;
			}
			System.out.println(count);
			Assert.assertTrue(count > 10000);
		}
		finally
		{
			builder.close();
		}
	}

	@Test
	public void testCachedReads() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			java.io.File test = builder.cleanMountedDirectory();
			java.io.File hello = new java.io.File(test, "hello.txt");
			FileUtils.write(hello, "Hello World!");
			builder.flush();
			
			// Pull it into cache
			FileUtils.readFileToString(hello);
			
			int count = 0;
			long start = System.currentTimeMillis();
			while(System.currentTimeMillis()-start < 1000)
			{
				Assert.assertEquals("Hello World!", FileUtils.readFileToString(hello));
				count++;
			}
			System.out.println(count);
			Assert.assertTrue(count > 25);
		}
		finally
		{
			builder.close();
		}
	}

	@Test
	public void testParents() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			
			// Warm the cache
			directory.getParents();
			
			int count = 0;
			long start = System.currentTimeMillis();
			while(System.currentTimeMillis()-start < 1000)
			{
				Assert.assertEquals(1, directory.getParents().size());
				count++;
			}
			System.out.println(count);
			Assert.assertTrue(count > 10000);
		}
		finally
		{
			builder.close();
		}
	}

	@Test
	public void testMkdir() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			
			// Warm the cache
			directory.getChildren();
			
			// Create 10 directories in under a second
			// This number was chosen because it should certainly be possible to create a directory in under 100ms average
			// If this test fails:
			//   - I've seen the single log insert take 60ms (seems slow - perhaps something to investigate)
			//   - Check for lock contention (If the remotereplay grabs a lock, that could take a full second alone, which would blast us)
			long start = System.currentTimeMillis();
			for(int i = 0; i < 10 && System.currentTimeMillis()-start < 1000; i++)
				directory.mkdir(Integer.toString(i));
			
			Assert.assertEquals(10, directory.getChildren().size());
		}
		finally
		{
			builder.close();
		}
	}
}
