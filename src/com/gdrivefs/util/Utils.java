package com.gdrivefs.util;

import com.gdrivefs.simplecache.FileContent;

public class Utils
{
	private Utils() {
		// utilities class
	}
	
	public static long roundUpToFragmentBoundary(long val) {
		return (val / FileContent.FRAGMENT_BOUNDARY + 1) * FileContent.FRAGMENT_BOUNDARY;
	}
}
