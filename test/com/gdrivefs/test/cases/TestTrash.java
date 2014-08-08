package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;

public class TestTrash
{
	@Test
	public void testTrivial() throws IOException, GeneralSecurityException, InterruptedException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			File test = builder.cleanTestDir();
			Assert.assertEquals(0, test.getChildren().size());
			File noise = test.mkdir("noise");
			Assert.assertEquals(1, test.getChildren().size());
			noise.trash();
			Assert.assertEquals(0, test.getChildren().size());
			builder.flush();
			Assert.assertEquals(0, test.getChildren().size());
			test = builder.uncleanTestDir();
			test.refresh();
			Assert.assertEquals(0, test.getChildren().size());
		}
		finally
		{
			builder.close();
		}
	}
}
