import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.Drive.Files.List;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.ChildList;
import com.google.api.services.drive.model.ChildReference;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Uploads all files in ~/.googlefs/uploads
 */
public class DriveUploader
{
	public static void main(String[] args)
	{
		try
		{
			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

			FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "auth"));
			JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
			GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV", Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory).build();
			Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
			Drive drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("GDrive").build();

			for(String file : args)
				uploadFile(drive, new java.io.File(file));
			for(java.io.File file : new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "uploads").listFiles())
				if(!file.isDirectory()) uploadFile(drive, file);

			return;
		}
		catch(IOException e)
		{
			System.err.println(e.getMessage());
		}
		catch(Throwable t)
		{
			t.printStackTrace();
		}
		System.exit(1);
	}

	/** Uploads a file using either resumable or direct media upload. */
	private static File uploadFile(Drive drive, java.io.File fileToUpload) throws IOException
	{
		String type = Files.probeContentType(Paths.get(fileToUpload.getAbsolutePath()));
		System.out.println("Uploading " + fileToUpload + " of type " + type);
		File newFile = new File();
		newFile.setTitle(fileToUpload.getName());
		newFile.setMimeType(type);
		newFile.setParents(Arrays.asList(new ParentReference().setId("0B9V4qybqtJE-Y0t6bXBhb3hieHc")));

		FileContent mediaContent = new FileContent(type, fileToUpload);

		Drive.Files.Insert insert = drive.files().insert(newFile, mediaContent);
		MediaHttpUploader uploader = insert.getMediaHttpUploader();
		uploader.setDirectUploadEnabled(false);
		newFile = insert.execute();

		FileInputStream fis = new FileInputStream(fileToUpload);
		String md5 = DigestUtils.md5Hex(fis);
		fis.close();
		if(newFile.getMd5Checksum().equals(md5)) fileToUpload.delete();
		return newFile;
	}

}
