package com.gdrivefs.simplecache;

import java.net.URL;
import java.sql.Timestamp;

public class SimpleFileMetadata
{
	Long metadataAsOfDate;
	String title;
	String fileMd5;
	URL downloadUrl;
	String mimeType;
	Long size;
	Timestamp modifiedTime;
	
	public SimpleFileMetadata(Long asOfDate)
	{
		this.metadataAsOfDate = asOfDate;
	}
}
