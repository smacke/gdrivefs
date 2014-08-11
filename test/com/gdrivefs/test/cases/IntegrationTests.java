package com.gdrivefs.test.cases;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.test.util.DriveBuilder;

public class IntegrationTests
{
	@Test
	public void testTrivial() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
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
		finally
		{
			builder.close();
		}
	}


}
