package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;

public class TestRelationships
{
	@Test
	public void testTrivial() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();
		Assert.assertEquals(0, test.getChildren().size());
	}

	@Test
	public void testCreateChild() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();

		Assert.assertEquals(0, test.getChildren().size());
		test.mkdir("foo");
		Assert.assertEquals(1, test.getChildren().size());
		test.refresh();
		Assert.assertEquals(1, test.getChildren().size());
		
		test = DriveBuilder.uncleanTestDir();
		Assert.assertEquals(1, test.getChildren().size());
	}

	@Test
	public void testGetChildren() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();

		Assert.assertEquals(0, test.getChildren().size());
		test.mkdir("foo");
		test.mkdir("bar");
		test.mkdir("noise");
		Assert.assertEquals(1, test.getChildren("noise").size());
	}

	@Test
	public void testDuplicates() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();

		Assert.assertEquals(0, test.getChildren().size());
		test.mkdir("foo");
		test.mkdir("noise");
		test.mkdir("noise");
		test.mkdir("noise");
		test.mkdir("bar");
		Assert.assertEquals(3, test.getChildren("noise").size());
		test.refresh();
		Assert.assertEquals(3, test.getChildren("noise").size());
		
		test = DriveBuilder.uncleanTestDir();
		Assert.assertEquals(3, test.getChildren("noise").size());
	}

	@Test
	public void testNested() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();

		File foo = test.mkdir("foo");
		foo.mkdir("bar");
		foo.mkdir("noise");
		Assert.assertEquals(2, foo.getChildren().size());
		

		test = DriveBuilder.uncleanTestDir();
	}

	@Test
	public void testAddChild() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();

		File foo = test.mkdir("foo");
		File bar = test.mkdir("bar");
		foo.addChild(bar);
		
		Assert.assertEquals(2, test.getChildren().size());
		Assert.assertEquals(1, foo.getChildren().size());
		
		test.refresh();
		foo.refresh();
		bar.refresh();
		
		Assert.assertEquals(2, test.getChildren().size());
		Assert.assertEquals(1, foo.getChildren().size());
	}

	@Test
	public void testMove() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();

		File foo = test.mkdir("foo");
		File bar = test.mkdir("bar");
		foo.addChild(bar);
		test.removeChild(bar);
		
		Assert.assertEquals(1, test.getChildren().size());  // test should contain foo
		Assert.assertEquals(1, foo.getChildren().size());  // foo should contain bar
		
		test.refresh();
		foo.refresh();
		bar.refresh();
		
		Assert.assertEquals(1, test.getChildren().size());
		Assert.assertEquals(1, foo.getChildren().size());
	}
}
