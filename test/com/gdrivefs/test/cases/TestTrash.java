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
public class TestTrash
{
	@Test
	public void testTrivial(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException
	{
			File test = builder.cleanDriveDirectory();
			Assert.assertEquals(0, test.getChildren().size());
			File noise = test.mkdir("noise");
			Assert.assertEquals(1, test.getChildren().size());
			noise.trash();
			Assert.assertEquals(0, test.getChildren().size());
			builder.flush();
			Assert.assertEquals(0, test.getChildren().size());
			test = builder.uncleanDriveDirectory();
			test.refresh();
			Assert.assertEquals(0, test.getChildren().size());
	}
}
