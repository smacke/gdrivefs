
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.ExponentialBackOffPolicy;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.Beta;
import com.google.api.client.util.IOUtils;
import com.google.api.client.util.Preconditions;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.jimsproch.sql.Database;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

public final class FileCacheManager
{
	Drive drive;
	Database db;
	HttpTransport transport;
	HttpRequestInitializer httpRequestInitializer;
	
	public FileCacheManager(Drive drive, Database db, HttpTransport transport, HttpRequestInitializer httpRequestInitializer)
	{
		this.drive = drive;
		this.db = db;
		this.transport = transport;
		this.httpRequestInitializer = httpRequestInitializer;
	}
	
	public byte[] read(String path, final long size, final long offset)
	{
		File remoteFile;
		try
		{
			String fileId = db.getString("SELECT ID FROM FILES WHERE TITLE=?", path.substring(1));
			remoteFile = drive.files().get(fileId).execute();
			
			java.io.File localFile = new java.io.File("/home/kyle/.googlefs/cache/"+remoteFile.getMd5Checksum());

			
			// If the file doesn't exist or is the wrong size (incomplete), download it.
			if(!localFile.exists() || localFile.length() != remoteFile.getQuotaBytesUsed())
			{
			    java.io.File parentDir = new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "cache");
			    if (!parentDir.exists() && !parentDir.mkdirs()) {
			      throw new IOException("Unable to create parent directory");
			    }
			    OutputStream out = new FileOutputStream(new java.io.File(parentDir, remoteFile.getMd5Checksum()));

			    DownloadTask downloader = new DownloadTask(db, transport, httpRequestInitializer);
			    downloader.setDirectDownloadEnabled(false);
			    downloader.download(new GenericUrl(remoteFile.getDownloadUrl()), out);
			    
			    out.close();
			}
			
			
			final int bytesToRead = (int) Math.min(localFile.length() - offset, size);
			final byte[] bytesRead = new byte[bytesToRead];
			RandomAccessFile contents = new RandomAccessFile(localFile, "r");
			contents.seek((int) offset);
			contents.read(bytesRead);
			contents.close();
			return bytesRead;
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}
}
