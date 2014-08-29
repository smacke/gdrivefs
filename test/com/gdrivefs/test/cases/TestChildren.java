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
public class TestChildren
{
	@Test
	public void testTrivial(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
			Assert.assertEquals(0, test.getChildren().size());
	}

	@Test
	public void testCreateChild(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
	
			Assert.assertEquals(0, test.getChildren().size());
			test.mkdir("foo");
			Assert.assertEquals(1, test.getChildren().size());
			test.refresh();
			Assert.assertEquals(1, test.getChildren().size());
			
			test = builder.uncleanDriveDirectory();
			Assert.assertEquals(1, test.getChildren().size());
	}

	@Test
	public void testGetChildren(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
	
			Assert.assertEquals(0, test.getChildren().size());
			test.mkdir("foo");
			test.mkdir("bar");
			test.mkdir("noise");
			Assert.assertEquals(1, test.getChildren("noise").size());
	}

	@Test
	public void testDuplicates(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
	
			Assert.assertEquals(0, test.getChildren().size());
			test.mkdir("foo");
			test.mkdir("noise");
			test.mkdir("noise");
			test.mkdir("noise");
			test.mkdir("bar");
			Assert.assertEquals(3, test.getChildren("noise").size());
			test.refresh();
			Assert.assertEquals(3, test.getChildren("noise").size());
			
			builder.flush();
			test = builder.uncleanDriveDirectory();
			Assert.assertEquals(3, test.getChildren("noise").size());
	}

	@Test
	public void testNested(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
	
			File foo = test.mkdir("foo");
			foo.mkdir("bar");
			foo.mkdir("noise");
			Assert.assertEquals(2, foo.getChildren().size());
			
			builder.flush();
			test = builder.uncleanDriveDirectory();
			Assert.assertEquals(2, test.getChildren().get(0).getChildren().size());
	}

	@Test
	public void testAddChild(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
	
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
	public void testMove(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
	
			File foo = test.mkdir("foo");
			File bar = test.mkdir("bar");
			foo.addChild(bar);
			test.removeChild(bar);
			
			System.out.println(test.getChildren());
			Assert.assertEquals(1, test.getChildren().size());  // test should contain foo
			Assert.assertEquals(1, foo.getChildren().size());  // foo should contain bar
			
			test.refresh();
			foo.refresh();
			bar.refresh();
			
			Assert.assertEquals(1, test.getChildren().size());
			Assert.assertEquals(1, foo.getChildren().size());
	}

	@Test
	public void testLoop(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
		
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
	public void testCacheReplayConsistency(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
			
			File parent = test.mkdir("parent");
			for(int i = 0; i < 10; i++) test.mkdir(Integer.toString(i));
	
			for(int i = 0; i < 10; i++) parent.addChild(test.getChildren(Integer.toString(i)).get(0));
			parent.refresh();
			
			Assert.assertEquals(10, parent.getChildren().size());
	}
}
