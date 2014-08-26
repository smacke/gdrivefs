package com.gdrivefs.util;

import com.gdrivefs.simplecache.File;

public class Utils
{
	private Utils() {
		// utilities class
	}
	
	public static long roundUp(long val) {
		return (val / File.FRAGMENT_BOUNDARY + 1) * File.FRAGMENT_BOUNDARY;
	}
}
