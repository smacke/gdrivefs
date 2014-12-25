package com.gdrivefs.test.cases;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.GeneralSecurityException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.gdrivefs.test.util.DriveBuilder;
import com.gdrivefs.test.util.GoogleFilesystemRunner;

@RunWith(GoogleFilesystemRunner.class)
public class ForeignCharactersTest extends Assert
{
	@Test
	public void testForeignCharacters(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		java.io.File test = builder.cleanMountedDirectory();
		java.io.File file = new java.io.File(test, "πλψ猫.txt");
		FileUtils.writeStringToFile(file, "πλψ猫!");
		builder.flush();

		byte[] bytes = FileUtils.readFileToByteArray(file);
		Assert.assertEquals("πλψ猫!", new String(bytes));
	}
}
