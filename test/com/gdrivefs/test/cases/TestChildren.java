package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;

public class TestChildren
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
		Assert.assertEquals(2, test.getChildren().get(0).getChildren().size());
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

	@Test
	public void testLoop() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();
	
		File foo = test.mkdir("foo");
		File bar = test.mkdir("bar");
		
		foo.getChildren();
		bar.getChildren();
		
		foo.addChild(bar);
		
		try
		{
			bar.addChild(foo);
			Assert.fail("Adding foo as a child of bar should cause exception (loops are prohibited)");
		}
		catch(Exception e)
		{
			/* expected, SUCCESS */
		}
	}

	@Test
	public void testCacheReplayConsistency() throws IOException, GeneralSecurityException
	{
		File test = DriveBuilder.cleanTestDir();
		
		File parent = test.mkdir("parent");
		for(int i = 0; i < 10; i++) test.mkdir(Integer.toString(i));

		for(int i = 0; i < 10; i++) parent.addChild(test.getChildren(Integer.toString(i)).get(0));
		parent.refresh();
		
		Assert.assertEquals(10, parent.getChildren().size());
	}
}
