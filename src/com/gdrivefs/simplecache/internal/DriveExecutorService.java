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
import java.util.concurrent.atomic.AtomicBoolean;

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
		
		try { this.notifyAll(); }
		catch(IllegalMonitorStateException e) { throw new Error("Service monitor lock must be held, because you should have been checking the isShutdown() state before adding task!", e); }
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
		notifyAll();
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
		final DriveExecutorService executor = this;
		final AtomicBoolean flushed = new AtomicBoolean(false);
		delegate.execute(new Runnable()
		{
			@Override
			public void run()
			{
				flushed.set(true);
				synchronized(executor) { executor.notifyAll(); }
			}
		});
		
		synchronized(this)
		{
			while(true)
			{
				if(flushed.get() == true) return;
				if(executor.isShutdown()) throw new InterruptedException("Drive has been shutdown before all tasks could be flushed");
				executor.wait();
			}
		}
	}

	public String toString()
	{
		return delegate.toString();
	}
}
