package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import net.fusejna.FuseException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.test.util.DriveBuilder;

public class TestWrite
{
	@Test
	public void testTruncateChangesSize() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
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
				helloFile.write("987654321".getBytes(), 5);
				Assert.assertEquals(14, helloFile.getSize());
				Assert.assertNotEquals(DigestUtils.md5Hex("123456789"), helloFile.getMd5Checksum());
			}
		}
		finally
		{
			builder.close();
		}
	}
}
