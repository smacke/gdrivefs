package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;

public class TestMetadata
{
	@Test
	public void testTitleTrivial() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			Assert.assertEquals(1, test.getChildren().size());
			Assert.assertEquals("MyAwesomeDirectory", directory.getTitle());

			test = builder.uncleanDriveDirectory();
			directory = test.getChildren("MyAwesomeDirectory").get(0);
			Assert.assertEquals("MyAwesomeDirectory", directory.getTitle());
		}
		finally
		{
			builder.close();
		}
	}

	@Test
	public void testTitleChange() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			directory.setTitle("Nonsense");
			Assert.assertEquals("Nonsense", directory.getTitle());
			Assert.assertEquals("Nonsense", test.getChildren().get(0).getTitle());

			test = builder.uncleanDriveDirectory();
			Assert.assertEquals("Nonsense", test.getChildren().get(0).getTitle());
		}
		finally
		{
			builder.close();
		}
	}

	@Test
	public void testTitleSlashes() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanDriveDirectory();
			test.mkdir("My /-\\wesom@ D1rec|0ry!");

			Assert.assertEquals("My /-\\wesom@ D1rec|0ry!", test.getChildren().get(0).getTitle());
			test = builder.uncleanDriveDirectory();
			Assert.assertEquals("My /-\\wesom@ D1rec|0ry!", test.getChildren().get(0).getTitle());
		}
		finally
		{
			builder.close();
		}
	}

}
