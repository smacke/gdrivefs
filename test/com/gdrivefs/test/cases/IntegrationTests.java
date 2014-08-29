package com.gdrivefs.test.cases;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.gdrivefs.test.util.DriveBuilder;
import com.gdrivefs.test.util.GoogleFilesystemRunner;

@RunWith(GoogleFilesystemRunner.class)
public class IntegrationTests
{
	@Test
	public void testTrivial(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			{
				File test = builder.cleanMountedDirectory();
				File helloFile = new File(test, "hello.txt");
				FileUtils.write(helloFile, "hello world");
			}
			
			builder.flush();
			
			{
				File test = builder.uncleanMountedDirectory();
				System.out.println(Arrays.toString(test.listFiles()));
				Assert.assertEquals(1, test.listFiles().length);
			}
	}
	
	@Test
	public void testFileRead(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			{
				File test = builder.cleanMountedDirectory();
				File helloFile = new File(test, "hello.txt");
				FileUtils.write(helloFile, "hello world");
			}
			
			builder.flush();
			
			{
				File test = builder.uncleanMountedDirectory();
				File helloFile = new File(test, "hello.txt");
				Assert.assertEquals("hello world", FileUtils.readFileToString(helloFile));
			}
	}
	
	@Test
	public void testMove(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			{
				File test = builder.cleanMountedDirectory();
				File src = new File(test, "src");
				File file = new File(src, "hello.txt");
				src.mkdir();
				FileUtils.write(file, "hello world");
			}
			
			builder.flush();
			
			{
				File test = builder.uncleanMountedDirectory();
				File src = new File(new File(test, "src"), "hello.txt");
				File dst = new File(new File(test, "dst"), "hello.txt");
				dst.getParentFile().mkdir();
				src.renameTo(dst);
				
				Assert.assertEquals("hello world", FileUtils.readFileToString(dst));
			}
	}

	
	@Test
	public void testMkdirs(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			{
				File test = builder.cleanMountedDirectory();
				
				File nestedDirectories = new File(new File(new File(new File(test,"foo"), "bar"), "noise"), "sweet");
				
				Assert.assertTrue(nestedDirectories.mkdirs());
				Assert.assertEquals("noise", test.listFiles()[0].listFiles()[0].listFiles()[0].getName());
			}
			
			builder.flush();
			
			{
				File test = builder.uncleanMountedDirectory();
				
				Assert.assertEquals("noise", test.listFiles()[0].listFiles()[0].listFiles()[0].getName());
			}
	}

	

}
