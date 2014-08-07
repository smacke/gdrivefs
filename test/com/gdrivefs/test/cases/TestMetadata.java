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
	public void testTitleTrivial() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();
		File directory = test.mkdir("MyAwesomeDirectory");
		Assert.assertEquals(1, test.getChildren().size());
		Assert.assertEquals("MyAwesomeDirectory", directory.getTitle());
		
		test = DriveBuilder.uncleanTestDir();
		directory = test.getChildren("MyAwesomeDirectory").get(0);
		Assert.assertEquals("MyAwesomeDirectory", directory.getTitle());
	}

	@Test
	public void testTitleChange() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();
		File directory = test.mkdir("MyAwesomeDirectory");
		directory.setTitle("Nonsense");
		Assert.assertEquals("Nonsense", directory.getTitle());
		Assert.assertEquals("Nonsense", test.getChildren().get(0).getTitle());
		
		test = DriveBuilder.uncleanTestDir();
		Assert.assertEquals("Nonsense", test.getChildren().get(0).getTitle());
	}

	@Test
	public void testTitleSlashes() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();
		test.mkdir("My /-\\wesom@ D1rec|0ry!");
		
		Assert.assertEquals("My /-\\wesom@ D1rec|0ry!", test.getChildren().get(0).getTitle());
		test = DriveBuilder.uncleanTestDir();
		Assert.assertEquals("My /-\\wesom@ D1rec|0ry!", test.getChildren().get(0).getTitle());
	}

}
