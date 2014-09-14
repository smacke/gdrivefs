package com.gdrivefs.simplecache;

import java.io.IOException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.common.base.Optional;
import com.jimsproch.sql.DatabaseRow;
import com.thoughtworks.xstream.XStream;

public class SimpleFileMetadata
{

	Optional<Long> asOfDate = Optional.absent(); // empty indicates not initialized
	String title = null;
	String fileMd5 = null;
	URL downloadUrl = null;
	String mimeType = null;
	Long size = null;
	Timestamp modifiedTime = null;
	
	public SimpleFileMetadata() {} // all fields uninitialized

	private SimpleFileMetadata(
			@Nullable Long metadataAsOfDate,
			String title,
			String fileMd5,
			URL downloadUrl,
			String
			mimeType,
			Long
			size,
			Timestamp
			modifiedTime) {
		this.asOfDate = Optional.fromNullable(metadataAsOfDate);
		this.title = title;
		this.fileMd5 = fileMd5;
		this.downloadUrl = downloadUrl;
		this.mimeType = mimeType;
		this.size = size;
		this.modifiedTime = modifiedTime;
	}

	public boolean isInited() {
		return asOfDate.isPresent();
	}

	// TODO: get rid of File dependency
	public void playLogOnMetadata(File file) throws IOException
	{
		for(DatabaseRow row : file.drive.getDatabase().getRows(
				"SELECT * FROM UPDATELOG "
				+ "WHERE COMMAND='setTitle' OR "
				+ "COMMAND='mkdir' OR COMMAND='createFile' "
				+ "OR COMMAND='truncate' "
				+ "OR COMMAND='write' OR "
				+ "COMMAND='update' "
				+ "ORDER BY ID ASC"))
			playOnInMemoryMetadata(file, row.getString("COMMAND"), (String[])new XStream().fromXML(row.getString("DETAILS")));
	}

	// TODO: get rid of File dependency
	public void playOnInMemoryMetadata(File file, String command, String... logEntry) throws IOException
	{
		if("setTitle".equals(command))
		{
			// Sanity checks
			if(!file.getLocalId().equals(UUID.fromString(logEntry[0]))) {
				return;
			}
			if(!title.equals(logEntry[1])) new Throwable("WARNING: Title does not match title from logs (expected: " + logEntry[1] + " was: " + file.getTitle() + ")").printStackTrace();

			// Perform update
			title = logEntry[2];
		}
		else if("mkdir".equals(command))
		{
			// Sanity checks
			if(!file.getLocalId().equals(UUID.fromString(logEntry[1]))) {
				return;
			}

			// Perform update
			title = logEntry[2];

			mimeType = File.MIME_FOLDER;
			size = null;
			modifiedTime = new Timestamp(Long.parseLong(logEntry[3]));
			asOfDate = Optional.of(new Timestamp(Long.parseLong(logEntry[3])).getTime());
			file.childrenAsOfDate = null;
			file.parentsAsOfDate = null;
			file.localFileId = UUID.fromString(logEntry[1]);
			fileMd5 = null;
			downloadUrl = null;
		}
		else if("createFile".equals(command))
		{
			// Sanity checks
			if(!file.getLocalId().equals(UUID.fromString(logEntry[1]))) {
				return;
			}

			// Perform update
			title = logEntry[2];

			mimeType = "application/octet-stream";
			size = 0L;
			modifiedTime = new Timestamp(Long.parseLong(logEntry[3]));
			asOfDate = Optional.of(new Timestamp(Long.parseLong(logEntry[3])).getTime());
			file.childrenAsOfDate = null;
			file.parentsAsOfDate = null;
			file.localFileId = UUID.fromString(logEntry[1]);
			fileMd5 = DigestUtils.md5Hex("");
			downloadUrl = null;
		}
		else if("update".equals(command))
		{
			if(!file.getLocalId().equals(UUID.fromString(logEntry[0]))) {
				return;
			}
			size = Long.parseLong(logEntry[1]);
			fileMd5 = "null".equals(logEntry[2]) ? null : logEntry[2];
		}
		else if("truncate".equals(command))
		{
			if(!file.getLocalId().equals(UUID.fromString(logEntry[0]))) {
				return;
			}
			size = Long.parseLong(logEntry[1]);
			fileMd5 = "null".equals(logEntry[2]) ? null : logEntry[2];
		}
		else if("write".equals(command))
		{
			if(!file.getLocalId().equals(UUID.fromString(logEntry[0]))) {
				return;
			}
			long offset = Long.parseLong(logEntry[1]);
			long length = Long.parseLong(logEntry[2]);
			if (size == null) { // N.B. (smacke): can't call getSize() or we will infinite recurse
				size = offset+length;
			} else {
				size = Math.max(size, offset+length);
			}
			fileMd5 = "null".equals(logEntry[4]) ? null : logEntry[4];
		}
		else {
			throw new Error("Unknown log entry: "+Arrays.toString(logEntry));
		}
	}
	
	public static class Builder {
		private Long metadataAsOfDate;
		private String title;
		private String fileMd5;
		private URL downloadUrl;
		private String mimeType;
		private Long size;
		private Timestamp modifiedTime;

		public Builder(Long asOfDate) {
			this.metadataAsOfDate = asOfDate;
		}
		public Builder title(String title) {
			this.title = title;
			return this;
		}
		public Builder fileMd5(String fileMd5) {
			this.fileMd5 = fileMd5;
			return this;
		}
		public Builder url(URL downlUrl) {
			this.downloadUrl = downlUrl;
			return this;
		}
		public Builder mimeType(String mimeType) {
			this.mimeType = mimeType;
			return this;
		}
		public Builder size(Long size) {
			this.size = size;
			return this;
		}
		public Builder lastModified(Timestamp modifiedTime) {
			this.modifiedTime = modifiedTime;
			return this;
		}
		public SimpleFileMetadata build() {
			return new SimpleFileMetadata(metadataAsOfDate, title, fileMd5, downloadUrl, mimeType, size, modifiedTime);
		}
	}
}
