package com.gdrivefs.simplecache.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * An executor service used internally by the Drive to manage the internal work queue.
 * 
 * Normal thread pools do not provide an easy way to flush the queue, perform a non-interrupting shutdown (required by derby), etc.
 */
public class DriveExecutorService implements ExecutorService
{
	ThreadPoolExecutor delegate;
	
	public DriveExecutorService()
	{
		this(new ThreadFactoryBuilder().build());
	}
	
	public DriveExecutorService(ThreadFactory threadFactory)
	{
		delegate = new ThreadPoolExecutor(1, 1, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), threadFactory);
		delegate.allowCoreThreadTimeOut(true);
	}

	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
	{
		return delegate.awaitTermination(timeout, unit);
	}

	public void execute(Runnable command)
	{
		delegate.execute(command);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0, long arg1, TimeUnit arg2) throws InterruptedException
	{
		return delegate.invokeAll(arg0, arg1, arg2);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> arg0) throws InterruptedException
	{
		return delegate.invokeAll(arg0);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
	{
		return delegate.invokeAny(tasks, timeout, unit);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> arg0) throws InterruptedException, ExecutionException
	{
		return delegate.invokeAny(arg0);
	}

	public boolean isShutdown()
	{
		return delegate.isShutdown();
	}

	public boolean isTerminated()
	{
		return delegate.isTerminated();
	}

	public boolean isTerminating()
	{
		return delegate.isTerminating();
	}

	public void shutdown()
	{
		delegate.shutdown();
		notifyAll();
	}

	public List<Runnable> shutdownNow()
	{
		List<Runnable> runnables = new ArrayList<Runnable>();
		delegate.getQueue().drainTo(runnables);
		delegate.shutdown();
		// TODO: handle the flush command; we want to make it blow up, since it didn't get to flush.
		return runnables;
	}

	public <T> Future<T> submit(Callable<T> task)
	{
		return delegate.submit(task);
	}

	public <T> Future<T> submit(Runnable task, T result)
	{
		return delegate.submit(task, result);
	}

	public Future<?> submit(Runnable task)
	{
		return delegate.submit(task);
	}
	
	public void flush() throws InterruptedException
	{
		try
		{
			delegate.submit(new FlushEntry()).get();
		}
		catch(ExecutionException e)
		{
			throw new RuntimeException(e);
		}
	}

	public String toString()
	{
		return delegate.toString();
	}
}

class FlushEntry implements Runnable
{
	Boolean success = true;
	@Override
	public void run()
	{
	}
	
	
}
