package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

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
			File test = builder.cleanTestDir();
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
	public void testParents() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanTestDir();
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
			File test = builder.cleanTestDir();
			File directory = test.mkdir("MyAwesomeDirectory");
			
			// Warm the cache
			directory.getChildren();
			
			// Create 100 directories in under a second
			long start = System.currentTimeMillis();
			for(int i = 0; i < 100 && System.currentTimeMillis()-start < 1000; i++)
				directory.mkdir(Integer.toString(i));
			
			Assert.assertEquals(100, directory.getChildren().size());
		}
		finally
		{
			builder.close();
		}
	}
}
