package com.gdrivefs.test.cases;

import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;

import net.fusejna.FuseException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.test.util.DriveBuilder;

public class TestWriteRenameWrite
{
	@Test
	public void test() throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		DriveBuilder builder = new DriveBuilder();
		try
		{
			{
				java.io.File test = builder.cleanMountedDirectory();
				java.io.File f1 = new java.io.File(test, "f1.txt");
				java.io.File f2 = new java.io.File(test, "f2.txt");
				java.io.File ft = new java.io.File(test, "tmp.txt");
				FileOutputStream foo = null, bar = null;

				try
				{
					foo = new FileOutputStream(f1);
					bar = new FileOutputStream(f2);
					foo.write("foo".getBytes());
					bar.write("bar".getBytes());
					f2.renameTo(ft);
					f1.renameTo(f2);
					ft.renameTo(f1);
					foo.write("foo".getBytes());
					bar.write("bar".getBytes());
				}
				finally
				{
					if(foo != null) foo.close();
					if(bar != null) bar.close();
				}

				Assert.assertEquals("barbar", FileUtils.readFileToString(f1));
				Assert.assertEquals("foofoo", FileUtils.readFileToString(f2));
			}
		}
		finally
		{
			builder.close();
		}
	}
}
