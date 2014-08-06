package com.gdrivefs;

public class AmbiguousPathException extends RuntimeException
{
	private static final long serialVersionUID = -3741053213257140981L;
	String path;
	
	public AmbiguousPathException(String path)
	{
		super(path);
	}
	
	public AmbiguousPathException(String path, String message, Throwable t)
	{
		super(message, t);
	}
	
	public String getPath()
	{
		return path;
	}
}
