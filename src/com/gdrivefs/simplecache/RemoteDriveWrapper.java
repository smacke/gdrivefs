package com.gdrivefs.simplecache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.http.HttpRequestFactory;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.ParentReference;

/** Simple API for performing remote requests that require IO.  Makes it easier to audit code to make sure we're not holding locks while doing io, do remote request logging, etc **/
public class RemoteDriveWrapper
{
	private Drive local;
	private com.google.api.services.drive.Drive remote;
	
	public RemoteDriveWrapper(Drive local, com.google.api.services.drive.Drive remote)
	{
		this.local = local;
		this.remote = remote;
	}
	
	public String getRootFileId() throws IOException
	{
		if(local.lock.isWriteLockedByCurrentThread()) throw new Error("Should not be holding write lock while doing network io");
		return remote.about().get().execute().getRootFolderId();
	}

	public com.google.api.services.drive.model.File getFileMetadata(String googleFileId) throws IOException
	{
		if(local.lock.isWriteLockedByCurrentThread()) throw new Error("Should not be holding write lock while doing network io");
		return remote.files().get(googleFileId).execute();
	}
	
	public List<com.google.api.services.drive.model.File> getChildren(String googleFileId) throws IOException
	{
		com.google.api.services.drive.Drive.Files.List lst = remote.files().list().setQ("'"+googleFileId+"' in parents and trashed=false");

		final List<com.google.api.services.drive.model.File> googleChildren = new ArrayList<com.google.api.services.drive.model.File>();
		do
		{
			FileList files = lst.execute();
			for(com.google.api.services.drive.model.File child : files.getItems()) {
				googleChildren.add(child);
			}
			lst.setPageToken(files.getNextPageToken());
		} while(lst.getPageToken() != null && lst.getPageToken().length() > 0);

		return googleChildren;
	}

	public List<ParentReference> getParents(String googleFileId) throws IOException
	{
		return remote.files().get(googleFileId).execute().getParents();
	}
	
	public void trash(String googleFileId) throws IOException
	{
		remote.files().trash(googleFileId).execute();
	}
	
	public HttpRequestFactory getRequestFactory()
	{
		return remote.getRequestFactory();
	}
	
	public File update(String googleFileId, com.google.api.services.drive.model.File newRemoteDirectory, com.google.api.client.http.FileContent mediaContent) throws IOException
	{
		return remote.files().update(googleFileId, newRemoteDirectory, mediaContent).execute();
	}
	
	public void updateFile(String googleFileId, com.google.api.services.drive.model.File file) throws IOException
	{
		remote.files().update(googleFileId, file).execute();
	}
	
	public void insertParent(String childGoogleFileId, String parentGoogleFileId) throws IOException
	{
		ParentReference newParent = new ParentReference();
		newParent.setId(parentGoogleFileId);
		remote.parents().insert(childGoogleFileId, newParent).execute();
	}
	
	public void deleteParent(String childGoogleFileId, String parentGoogleFileId) throws IOException
	{
		remote.parents().delete(childGoogleFileId, parentGoogleFileId).execute();
	}
	
	public com.google.api.services.drive.model.File insertFile(com.google.api.services.drive.model.File file) throws IOException
	{
		return remote.files().insert(file).execute();
	}
}
