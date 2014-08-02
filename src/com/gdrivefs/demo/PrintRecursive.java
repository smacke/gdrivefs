package com.gdrivefs.demo;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;

import com.gdrivefs.cache.File;
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

public class PrintRecursive
{
	public static void main(String[] args) throws IOException, GeneralSecurityException
	{
		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		
		FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "auth"));
		JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV", Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory).build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		
		com.google.api.services.drive.Drive remote = new com.google.api.services.drive.Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("GDrive").build();
		com.gdrivefs.cache.Drive drive = new com.gdrivefs.cache.Drive(remote, httpTransport);
		File root = drive.getRoot();
		
		printDirectory(root, 0);
	}
	
	public static void printDirectory(File root, int depth) throws IOException
	{
		for(int i = 0; i < depth; i++) System.out.print("	");
		System.out.println(root.getTitle());
		
		for(File child : root.getChildren()) printDirectory(child, depth+1);
	}
	

}
