package com.gdrivefs.test.cases;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import com.gdrivefs.simplecache.File;
import com.gdrivefs.test.util.DriveBuilder;

public class TestConcurrency
{
	@Test
	public void testChildrenOfEmptyDirectory() throws IOException, GeneralSecurityException, InterruptedException, ExecutionException
	{
		final File test = DriveBuilder.cleanTestDir();
		
		ExecutorService worker = Executors.newFixedThreadPool(10);
		
		List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
		for(int i = 0; i < 50; i++)
		{
			final int id = i;
			futures.add(worker.submit(new Callable<Boolean>(){
				@Override
				public Boolean call() throws Exception
				{
					// Make a directory
					File mydirectory = test.mkdir(Integer.toString(id));
					List<File> otherDirectories = test.getChildren();
					File randomDirectory = otherDirectories.get(new Random().nextInt(otherDirectories.size()));

					try { randomDirectory.addChild(mydirectory); }
					catch(RuntimeException e) { /* Not unexpected if use adds self as parent, etc */ }
					randomDirectory.setTitle(mydirectory.getTitle()+randomDirectory.getTitle());
					
					System.out.println(id);
					
					return true;
				}}));
		}
		
		for(Future<Boolean> future : futures)
			Assert.assertTrue(future.get());
	}

}
