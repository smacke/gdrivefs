package com.gdrivefs.test.util;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.fusejna.FuseException;
import net.fusejna.FuseJna;

import com.gdrivefs.GoogleDriveLinuxFs;
import com.gdrivefs.simplecache.File;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.FileList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class DriveBuilder implements Closeable
{
	// Google Connection
	com.google.api.services.drive.Drive remote;
	HttpTransport httpTransport;
	com.gdrivefs.simplecache.Drive drive;
	java.io.File mountPoint;
	GoogleDriveLinuxFs filesystem;
	String testid;
	final static String unitTestDirectoryName = "googlefs-unit-test-scratch-space";
	
	public DriveBuilder() throws GeneralSecurityException, IOException
	{
		// Setup google connection
		httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "auth"));
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV", Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory).build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		remote = new com.google.api.services.drive.Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("GDrive").build();
	}

	public void flush() throws InterruptedException
	{
		if(filesystem != null) {
			filesystem.flush(true);
		}
		else drive.flush(true);
	}
	
	private com.google.api.services.drive.model.File getTestDirectory() throws IOException
	{
		com.google.api.services.drive.Drive.Files.List lst = remote.files().list().setQ(
				"'"+remote.about().get().execute().getRootFolderId()+"' in parents and title='"+unitTestDirectoryName+"'");

		FileList files = lst.execute();
		List<com.google.api.services.drive.model.File> testdirs = files.getItems();
		if(testdirs.size() == 0)
		{
			String userEmail = remote.about().get().execute().getUser().getEmailAddress();
			throw new Error("Could not find directory named '"+unitTestDirectoryName+"' in root of gdrive for "+userEmail);
		}
		if(testdirs.size() != 1) throw new Error("Unexpected number ("+testdirs.size()+") of directories named '"+unitTestDirectoryName+"'");
		if(lst.getPageToken() != null && lst.getPageToken().length() != 0) throw new Error("Unexpected number of test directories!");
		return testdirs.get(0);
	}
	
	public File cleanDriveDirectory() throws IOException, GeneralSecurityException, InterruptedException
	{
		close();
		
		resetTestDirectory(getTestDirectory());
		
		drive = new com.gdrivefs.simplecache.Drive(remote, httpTransport);

		File testdir = drive.getRoot().getChildren("googlefs-unit-test-scratch-space").get(0);
		testid = testdir.getId();
		return testdir;
	}
	
	private void resetTestDirectory(com.google.api.services.drive.model.File testFile) throws IOException, InterruptedException
	{
		ExponentialBackOff backoff = new ExponentialBackOff.Builder().build();
		while(true)
		{
			try
			{
				com.google.api.services.drive.Drive.Files.List lst = 
						remote.files().list().setQ("'"+testFile.getId()+"' in parents and trashed=false");
	
				final List<com.google.api.services.drive.model.File> googleChildren = Lists.newArrayList();
				do
				{
					FileList files = lst.execute();
					for(com.google.api.services.drive.model.File child : files.getItems()) {
						googleChildren.add(child);
					}
					lst.setPageToken(files.getNextPageToken());
				} while(lst.getPageToken() != null && lst.getPageToken().length() > 0);
	
				for(com.google.api.services.drive.model.File f : googleChildren) remote.files().delete(f.getId()).execute();
				break;
			}
			catch(GoogleJsonResponseException t)
			{
				if(t.getMessage().startsWith("500 Internal Server Error")) System.err.println("500 Internal Server Error while cleaning test directory");
				if(t.getMessage().startsWith("404 Not Found")) System.err.println("404 Not Found while cleaning test directory");
				t.printStackTrace();
			}
			catch(Throwable t)
			{
				t.printStackTrace();
			}
			long backOffMillis = backoff.nextBackOffMillis();
			if(backOffMillis == ExponentialBackOff.STOP)
			{
				throw new Error("Unable to clean directory due to repeated failures");
			}
			else
			{
				Thread.sleep(backOffMillis);
			}
		}
	}

	public File uncleanDriveDirectory() throws IOException, GeneralSecurityException, InterruptedException
	{
		flush();
		close();
		
		if(testid == null) throw new Error("Must clean up for new test (using cleanTestDir) before reusing directory with new drive");

		drive = new com.gdrivefs.simplecache.Drive(remote, httpTransport);
		return drive.getRoot().getChildren(unitTestDirectoryName).get(0);
	}
	
	private java.io.File mountTestDirectory() throws IOException, UnsatisfiedLinkError, FuseException
	{
		drive = new com.gdrivefs.simplecache.Drive(remote, httpTransport);

		// Create and mount the filesystem
		mountPoint = Files.createTempDir();
		filesystem = new GoogleDriveLinuxFs(drive, httpTransport);
		filesystem.setLoggingStatus(true);
		filesystem.mount(mountPoint, false);

		// Warm the cache by prefetching the drive root, which greatly improves the user experience
		filesystem.getRoot().getChildren();
		filesystem.getRoot().considerAsyncDirectoryRefresh(1, TimeUnit.HOURS);

		java.io.File testdir = new java.io.File(mountPoint, unitTestDirectoryName);
		if(!testdir.exists() || !testdir.isDirectory()) throw new Error("Could not locate test directory");

		return testdir;
	}
	
	public java.io.File cleanMountedDirectory() throws UnsatisfiedLinkError, FuseException, IOException, GeneralSecurityException, InterruptedException
	{
		close();
		
		com.google.api.services.drive.model.File testdir = getTestDirectory();
		testid = testdir.getId();
		resetTestDirectory(testdir);
		
		return mountTestDirectory();
	}
	
	public java.io.File uncleanMountedDirectory() throws UnsatisfiedLinkError, FuseException, IOException, GeneralSecurityException
	{
		close();

		return mountTestDirectory();
	}
	
	@Override
	public void finalize() throws Throwable
	{
		if(drive != null) drive.close();
		super.finalize();
	}
	
	@Override
	public void close() throws IOException
	{
		IOException exception = null;
		
		if(filesystem != null)
		{
			try
			{
				filesystem.unmount();
			}
			catch(FuseException e)
			{
				exception = new IOException(e);
			}
			filesystem = null;
		}

		if(drive != null)
		{
			drive.close();
			drive = null;
		}
		
		if(mountPoint != null && mountPoint.exists())
		{
			FuseJna.unmount(mountPoint);
			mountPoint.delete();
			mountPoint = null;
		}
		
		if(exception != null) throw exception;
	}
	
}
