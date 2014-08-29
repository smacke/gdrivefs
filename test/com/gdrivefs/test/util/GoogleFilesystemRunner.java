package com.gdrivefs.test.util;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

public class GoogleFilesystemRunner extends Runner
{
	Class<?> clazz;
	
	public GoogleFilesystemRunner(Class<?> klass) throws InitializationError {
		this.clazz = klass;
	}

	protected Statement methodBlock(final DriveBuilder builder, final FrameworkMethod method) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				Object target = clazz.newInstance();
				method.invokeExplosively(target, builder);
			}
		};
	}	

	public Description getDescription()
	{
		return Description.createSuiteDescription(clazz);
	}
	
	@Override
	public void run(RunNotifier notifier)
	{
		List<FrameworkMethod> methods = new TestClass(clazz).getAnnotatedMethods(Test.class);

		try
		{
			notifier.fireTestStarted(getDescription());
			for(FrameworkMethod method : methods)
			{
				DriveBuilder builder = null;
				try
				{
					builder = new DriveBuilder();
					methodBlock(builder, method).evaluate();
				}
				catch(AssumptionViolatedException e)
				{
					notifier.fireTestFailure(new Failure(getDescription(), e));
					break;
				}
				catch(Throwable e)
				{
					notifier.fireTestFailure(new Failure(getDescription(), e));
					break;
				}
				finally
				{
					notifier.fireTestFinished(getDescription());
					if(builder != null) builder.close();
				}
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}
}
