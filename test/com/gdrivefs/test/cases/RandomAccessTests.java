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
public class RandomAccessTests extends Assert
{
	@Test
	public void testRandomAccessReads(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			{
				java.io.File test = builder.cleanMountedDirectory();
				java.io.File file = new java.io.File(test, "hello.txt");
				FileUtils.writeStringToFile(file, "Hello World!");
				builder.flush();

				RandomAccessFile raf = new RandomAccessFile(file, "r");
				try
				{
					byte[] bytes = new byte[5];
					raf.seek(6);
					raf.read(bytes);
					Assert.assertEquals("World", new String(bytes));
					raf.seek(11);
					Assert.assertEquals('!', raf.read());
					raf.seek(0);
					raf.read(bytes);
					Assert.assertEquals("Hello", new String(bytes));
					raf.close();
				}
				finally
				{
					raf.close();
				}
			}
	}

	@Test
	public void stressTestRandomAccess(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
			java.io.File test = builder.cleanMountedDirectory();
			java.io.File file = new java.io.File(test, "hello.txt");

			Random rand = new Random(0);
			int fsize = 4097 + rand.nextInt(4095); // one block plus some spillover (probably doesn't matter)
			byte[] memoryFile = new byte[fsize];
			System.out.println("size: " + fsize);

			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			try
			{
				raf.write(memoryFile);
				for (int i=0; i<100; i++) {
					int pos = rand.nextInt(fsize);
					raf.seek(pos);
					int towrite = rand.nextInt(fsize-pos);
					System.out.println("writing " + towrite + " bytes to position " + pos);
					byte[] noise = new byte[towrite];
					rand.nextBytes(noise);
					raf.write(noise);
					System.arraycopy(noise, 0, memoryFile, pos, towrite);
				}
				byte[] written = new byte[fsize];
				raf.seek(0);
				System.out.printf("Now about to read %d bytes at position 0\n", fsize);
				raf.read(written);
				assertArrayEquals(memoryFile, written);
			}
			finally
			{
				raf.close();
			}
	}

	@Test
	public void stressTestRandomAccessConcurrent(DriveBuilder builder) throws IOException, GeneralSecurityException, InterruptedException, UnsatisfiedLinkError, FuseException
	{
		ExecutorService service = null;
		final AtomicReference<AssertionError> exnCapture = new AtomicReference<>();
		try
		{
			final java.io.File test = builder.cleanMountedDirectory();
			int nThreads = 5;
			service = Executors.newFixedThreadPool(nThreads);
			
			for (int t=0; t<nThreads; t++) {
				final int myId = t;
				service.submit(new Runnable() {
					@Override
					public void run() {
						java.io.File file = new java.io.File(test, "hello" + myId + ".txt");

						Random rand = new Random(System.nanoTime());
						int fsize = 4097 + rand.nextInt(4095); // one block plus some spillover (probably doesn't matter)
						byte[] memoryFile = new byte[fsize];
						System.out.println("size: " + fsize);

						RandomAccessFile raf = null;
						try
						{
							raf = new RandomAccessFile(file, "rw");
							raf.write(memoryFile);
							for (int i=0; i<10; i++) {
								int pos = rand.nextInt(fsize);
								raf.seek(pos);
								int towrite = rand.nextInt(fsize-pos);
								byte[] noise = new byte[towrite];
								rand.nextBytes(noise);
								raf.write(noise);
								System.arraycopy(noise, 0, memoryFile, pos, towrite);
							}
							byte[] written = new byte[fsize];
							raf.seek(0);
							raf.read(written);
							assertArrayEquals(memoryFile, written);
						}
						catch(IOException e)
						{
							exnCapture.set(new AssertionError("IO exception", e));
						}
						catch (AssertionError e) {
							exnCapture.set(e);
						}
						finally
						{
							if (raf != null) {
								try {
									raf.close();
								} catch (IOException e) {
									exnCapture.set(new AssertionError("IO exception", e));
								}
							}
						}
					}
				});
			}
		}
		finally
		{
			if (service != null) {
                service.shutdown();
                service.awaitTermination(1, TimeUnit.DAYS);
			}
		}
		if (exnCapture.get() != null) {
			throw exnCapture.get();
		}
	}
}
