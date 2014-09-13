package com.gdrivefs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import net.fusejna.FuseException;
import net.fusejna.FuseJna;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.StoredCredential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStore;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.client.util.store.MemoryDataStoreFactory;
import com.google.api.services.drive.DriveScopes;

public class MountGoogleDrive
{
	public static void main(final String... args) throws FuseException, GeneralSecurityException, IOException, InterruptedException, ParseException
	{
		Options options = new Options();
		options.addOption("t", true, "Filesystem type (always gdrivefs; ignored)");
		options.addOption("v", false, "Verbose");
		options.addOption("d", true, "Specify data directory for internal drive state (default: ~/.googlefs/)");
		options.addOption("c", true, "Specify cache directory (default is inside the data directory; ~/.googlefs/cache/)");
		options.addOption("a", true, "Specify auth directory (default is inside the data/auth directory; ~/.googlefs/auth/[emailaddress]/)");
		
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

		if(mountPoint != null) FuseJna.unmount(mountPoint);
		
		String errors = checkArguments(email, mountPoint);
		if(errors != null)
		{
			System.err.println(errors);
			System.exit(1);
		}

		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

		java.io.File dataDirectory = cmd.hasOption('d') ? new java.io.File(cmd.getOptionValue('d')) : new java.io.File(System.getProperty("user.home"), ".googlefs");
		java.io.File authDirectory = cmd.hasOption('a') ? new java.io.File(cmd.getOptionValue('a')) : new java.io.File(dataDirectory, "auth");
		java.io.File cacheDirectory = cmd.hasOption('c') ? new java.io.File(cmd.getOptionValue('c')) : new java.io.File(dataDirectory, "cache");
		
		if(email != null) new java.io.File(authDirectory, email).mkdirs();
		
		// Create data store for user's credentials
		DataStore<StoredCredential> credentialDataStore = new MemoryDataStoreFactory().getDataStore("StoredCredential");
		
		// If we know the user's email, attempt to load their credentials
		if(email != null)
		{
			DataStore<StoredCredential> fileStore = new FileDataStoreFactory(new java.io.File(authDirectory, email)).getDataStore("StoredCredential");
			for(String key : fileStore.keySet()) credentialDataStore.set(key, fileStore.get(key));
		}
		
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV", Collections.singleton(DriveScopes.DRIVE)).setCredentialDataStore(credentialDataStore).build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		
		java.io.File dbdir = new java.io.File(dataDirectory, "/db");
		dbdir.getParentFile().mkdirs();

		com.google.api.services.drive.Drive remote = new com.google.api.services.drive.Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("GDrive").build();
		com.gdrivefs.simplecache.Drive drive = new com.gdrivefs.simplecache.Drive(remote, httpTransport, dbdir);
		
		// Save the credentials using the account that the user ultimately authenticated with
		email = remote.about().get().execute().getUser().getEmailAddress();
		DataStore<StoredCredential> fileStore = new FileDataStoreFactory(new java.io.File(authDirectory, email)).getDataStore("StoredCredential");
		for(String key : credentialDataStore.keySet()) fileStore.set(key, credentialDataStore.get(key));
		
		GoogleDriveLinuxFs filesystem = null;
		
		// Create and mount the filesystem
		filesystem = new GoogleDriveLinuxFs(drive, httpTransport);
		filesystem.setLoggingStatus(cmd.hasOption('v'));
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
			File gdriveJar = findJar();
			if(gdriveJar == null) throw new IOException("Could not find jar!");
			FileUtils.copyFile(gdriveJar, new java.io.File("/usr/lib/gdrivefs.jar"));
			FileUtils.writeStringToFile(new java.io.File("/sbin/mount.gdrive"), "#!/bin/sh\n\nsu $SUDO_USER -c \"gdrivefs $1 $2\" &");
			FileUtils.writeStringToFile(new java.io.File("/sbin/mount.gdrivefs"), "#!/bin/sh\n\nsu $SUDO_USER -c \"gdrivefs $1 $2\" &");
			FileUtils.writeStringToFile(new java.io.File("/usr/bin/gdrive"), "#!/bin/sh\n\njava -Djna.nosys=true -jar /usr/lib/gdrivefs.jar $@");
			FileUtils.writeStringToFile(new java.io.File("/usr/bin/gdrivefs"), "#!/bin/sh\n\njava -Djna.nosys=true -jar /usr/lib/gdrivefs.jar $@");
			
	        Set<PosixFilePermission> executablePermissions = new HashSet<PosixFilePermission>();
	        executablePermissions.add(PosixFilePermission.OWNER_READ);
	        executablePermissions.add(PosixFilePermission.OWNER_EXECUTE);
	        executablePermissions.add(PosixFilePermission.GROUP_READ);
	        executablePermissions.add(PosixFilePermission.GROUP_EXECUTE);
	        executablePermissions.add(PosixFilePermission.OTHERS_READ);
	        executablePermissions.add(PosixFilePermission.OTHERS_EXECUTE);

	        Files.setPosixFilePermissions(Paths.get("/usr/lib/gdrivefs.jar"), executablePermissions);
	        Files.setPosixFilePermissions(Paths.get("/sbin/mount.gdrive"), executablePermissions);
	        Files.setPosixFilePermissions(Paths.get("/sbin/mount.gdrivefs"), executablePermissions);
	        Files.setPosixFilePermissions(Paths.get("/usr/bin/gdrive"), executablePermissions);
	        Files.setPosixFilePermissions(Paths.get("/usr/bin/gdrivefs"), executablePermissions);
		}
		catch(IOException e)
		{
			/* ignore; we tried! */
			if(reportFailures) e.printStackTrace();
		}
	}
	
	private static File findJar() throws IOException
	{
		String[] classpath = System.getProperty("java.class.path").split(":");
		
		for(String classPathEntry : classpath)
		{
			JarFile jar = new JarFile(new File(classPathEntry));
			try
			{
				Enumeration<JarEntry> entries = jar.entries();
				while(entries.hasMoreElements())
				{
					JarEntry jarEntry = entries.nextElement();
	
					if("com/gdrivefs/MountGoogleDrive.class".equals(jarEntry.getName())) return new File(classPathEntry);
				}
			}
			finally
			{
				jar.close();
			}
		}
		
		return null;
	}
	
	private static void attemptInstall(String resource, java.io.File destination, boolean reportFailures)
	{
		if(destination.exists()) return;
		try
		{
			FileUtils.writeByteArrayToFile(destination, IOUtils.toByteArray(MountGoogleDrive.class.getResourceAsStream(resource)));
		}
		catch(IOException e)
		{
			/* ignore; we tried! */
			if(reportFailures) e.printStackTrace();
		}
	}
}
