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

public class FailingUnitTests
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
				builder.flush();
				FileUtils.readFileToString(helloFile);
				FileUtils.write(helloFile, "hello JIM!");
				Assert.assertEquals("hello JIM!", FileUtils.readFileToString(helloFile));
			}
		}
		finally
		{
			builder.close();
		}
	}
}
