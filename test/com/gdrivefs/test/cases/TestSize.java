package com.gdrivefs.test.cases;

import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;

import net.fusejna.FuseException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.gdrivefs.test.util.DriveBuilder;
import com.gdrivefs.test.util.GoogleFilesystemRunner;

@RunWith(GoogleFilesystemRunner.class)
public class TestSize
{
	@Test
	public void testTruncateChangesSize(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			{
				java.io.File test = builder.cleanMountedDirectory();
				java.io.File helloFile = new java.io.File(test, "hello.txt");
				FileUtils.write(helloFile, "123456789");
				Assert.assertEquals(9, helloFile.length());
			}
			
			builder.flush();
			
			{
				com.gdrivefs.simplecache.File test = builder.uncleanDriveDirectory();
				com.gdrivefs.simplecache.File helloFile = test.getChildren("hello.txt").get(0);
				Assert.assertEquals("123456789", new String(helloFile.read(9, 0)));
				Assert.assertEquals(DigestUtils.md5Hex("123456789"), helloFile.getMd5Checksum());
				Assert.assertEquals(9, helloFile.getSize());
//				helloFile.write("987654321".getBytes(), 5, null);
//				Assert.assertEquals(14, helloFile.getSize());
//				Assert.assertNotEquals(DigestUtils.md5Hex("123456789"), helloFile.getMd5Checksum());
			}
	}
	
	@Test
	public void testWriteChangesSize(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			java.io.File test = builder.cleanMountedDirectory();
			java.io.File helloFile = new java.io.File(test, "hello.txt");
			
			FileOutputStream fileHandle = new FileOutputStream(helloFile);
			try
			{
				String dataToWrite = "Hello World!";
				for(int i = 0; i < dataToWrite.length(); i++)
				{
					Assert.assertEquals(i, helloFile.length());
					fileHandle.write(dataToWrite.charAt(i));
					Assert.assertEquals(i+1, helloFile.length()); // FileOutputStream is not buffered, so size should have changed
				}
			}
			finally
			{
				fileHandle.close();
			}
	}
}
