package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;
import com.gdrivefs.test.util.GoogleFilesystemRunner;

@RunWith(GoogleFilesystemRunner.class)
public class TestMetadata
{
	@Test
	public void testTitleTrivial(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			Assert.assertEquals(1, test.getChildren().size());
			Assert.assertEquals("MyAwesomeDirectory", directory.getTitle());

			builder.flush();
			test = builder.uncleanDriveDirectory();
			directory = test.getChildren("MyAwesomeDirectory").get(0);
			Assert.assertEquals("MyAwesomeDirectory", directory.getTitle());
	}

	@Test
	public void testTitleChange(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
			File directory = test.mkdir("MyAwesomeDirectory");
			directory.setTitle("Nonsense");
			Assert.assertEquals("Nonsense", directory.getTitle());
			Assert.assertEquals("Nonsense", test.getChildren().get(0).getTitle());

			builder.flush();
			test = builder.uncleanDriveDirectory();
			Assert.assertEquals("Nonsense", test.getChildren().get(0).getTitle());
	}

	@Test
	public void testTitleSlashes(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
			test.mkdir("My /-\\wesom@ D1rec|0ry!");

			Assert.assertEquals("My /-\\wesom@ D1rec|0ry!", test.getChildren().get(0).getTitle());

			builder.flush();
			
			test = builder.uncleanDriveDirectory();
			Assert.assertEquals("My /-\\wesom@ D1rec|0ry!", test.getChildren().get(0).getTitle());
	}

}
