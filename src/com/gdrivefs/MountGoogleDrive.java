package com.gdrivefs;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import net.fusejna.FuseException;
import net.fusejna.FuseJna;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.DriveScopes;

public class MountGoogleDrive
{
	public static void main(final String... args) throws FuseException, GeneralSecurityException, IOException, InterruptedException, ParseException
	{
		Options options = new Options();
		options.addOption("t", true, "Filesystem type (always gdrivefs; ignored)");
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		
		String email = null;
		java.io.File mountPoint = null;

		if(cmd.getArgs().length == 1)
		{
			mountPoint = new java.io.File(cmd.getArgs()[0]);
		}
		if(cmd.getArgs().length == 2)
		{
			email = args[0];
			mountPoint = new java.io.File(cmd.getArgs()[1]);
		}

		attemptInstall(false);

		FuseJna.unmount(mountPoint);
		
		String errors = checkArguments(email, mountPoint);
		if(errors != null)
		{
			System.err.println(errors);
			System.exit(1);
		}

		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

		FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "auth"));
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV", Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory).build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		
		com.google.api.services.drive.Drive remote = new com.google.api.services.drive.Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("GDrive").build();
		com.gdrivefs.simplecache.Drive drive = new com.gdrivefs.simplecache.Drive(remote, httpTransport);
		
		GoogleDriveLinuxFs filesystem = null;
		
		// Create and mount the filesystem
		filesystem = new GoogleDriveLinuxFs(drive, httpTransport);
		filesystem.setLoggingStatus(true);
		filesystem.mount(mountPoint, false);
			
		// Warm the cache by prefetching the drive root, which greatly improves the user experience
		filesystem.getRoot().getChildren();
		filesystem.getRoot().considerAsyncDirectoryRefresh(1, TimeUnit.HOURS);
			
		// Wait for filesystem to be unmounted by the user
		synchronized(filesystem)
		{
			while(filesystem.isMounted()) filesystem.wait();
		}
	}
	
	public static String checkArguments(String email, java.io.File mountPoint)
	{
		StringBuilder errors = new StringBuilder();
		
		if(email != null && !email.matches("[a-zA-Z0-9\\.\\-_]+@[a-zA-Z0-9\\-_][a-zA-Z0-9\\.\\-_]+\\.[a-zA-Z0-9\\.\\-_]+[a-zA-Z0-9\\-_]+"))
		{
			errors.append("Invalid email address: "+email+"\n");
		}

		if(mountPoint == null)
		{
			errors.append("Must specify a mount point (an empty directory) as a command line argument.\n");
		}
		else if(!mountPoint.exists())
		{
			errors.append("Mountpoint ("+mountPoint.getAbsolutePath()+") does not exist; expected empty directory.\n");
		}
		else if(!mountPoint.isDirectory())
		{
			errors.append("Mountpoint ("+mountPoint.getAbsolutePath()+") is not an empty directory.\n");
		}
		else if(mountPoint.listFiles().length > 0)
		{
			errors.append("Mountpoint ("+mountPoint.getAbsolutePath()+") is not an empty directory.\n");
		}
		
		if(errors.toString().length() == 0) return null;
		
		errors.append("Usage: "+MountGoogleDrive.class.getSimpleName()+" <email_address> <mountpoint>\n");
		return errors.toString();
	}
	
	
	public static void attemptInstall(boolean reportFailures)
	{
		try
		{
			if(new java.io.File("/sbin/mount.gdrive").exists() || new java.io.File("/sbin/mount.gdrive").exists()) return;
			// TODO: attempt to install into mount command.
		}
		catch(Throwable t)
		{
			if(reportFailures) throw new RuntimeException(t);
		}
	}
}
