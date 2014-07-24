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

public class DriveSample {

  /**
   * Be sure to specify the name of your application. If the application name is {@code null} or
   * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
   */
  private static final String APPLICATION_NAME = "GDrive";

  /**
   * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
   * globally shared instance across your application.
   */
  private static FileDataStoreFactory dataStoreFactory;

  /** Global instance of the HTTP transport. */
  private static HttpTransport httpTransport;

  /** Global instance of the JSON factory. */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /** Global Drive API client. */
  private static Drive drive;

  /** Authorizes the installed application to access user's protected data. */
  private static Credential authorize() throws Exception {
    // set up authorization code flow
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
        httpTransport, JSON_FACTORY, "930897891601-4mbqrmuu5osvk7j3vlkv8k59liot620f.apps.googleusercontent.com", "v18DcOoqIvmVgPVtisCijpTV",
        Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory)
        .build();
    // authorize
    return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
  }

  public static void main(String[] args) {

    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();

      dataStoreFactory = new FileDataStoreFactory(new java.io.File(System.getProperty("user.home"), ".googlefs"));
      // authorization
      Credential credential = authorize();
      // set up the global Drive instance
      drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(
          APPLICATION_NAME).build();


/*	  List request = drive.files().list();
      do {
          try {
              FileList files = request.execute();
              for(File f : files.getItems())
            	  if(f.getTitle().equals("TV Shows"))
            		  System.out.println(f.getId());
              request.setPageToken(files.getNextPageToken());
          } catch (IOException e) {
              System.out.println("An error occurred: " + e);
              request.setPageToken(null);
          }
      } while (request.getPageToken() != null
              && request.getPageToken().length() > 0);
  */    
      
      for(String file : args)
    	  uploadFile(new java.io.File(file));
      // run commands
//      File uploadedFile = uploadFile(false);
//      File updatedFile = updateFileWithTestSuffix(uploadedFile.getId());
//      downloadFile(false, updatedFile);
//      uploadedFile = uploadFile(true);
//      downloadFile(true, uploadedFile);
      return;
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.exit(1);
  }

  /** Uploads a file using either resumable or direct media upload. */
  private static File uploadFile(java.io.File fileToUpload) throws IOException {
	String type = Files.probeContentType(Paths.get(fileToUpload.getAbsolutePath()));
	System.out.println("Uploading "+fileToUpload+" of type "+type);
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

  /** Updates the name of the uploaded file to have a "drivetest-" prefix. */
/*  private static File updateFileWithTestSuffix(String id) throws IOException {
    File fileMetadata = new File();
    fileMetadata.setTitle("drivetest-" + UPLOAD_FILE.getName());

    Drive.Files.Update update = drive.files().update(id, fileMetadata);
    return update.execute();
  }
*/
  /** Downloads a file using either resumable or direct media download. */
/*  private static void downloadFile(boolean useDirectDownload, File uploadedFile)
      throws IOException {
    // create parent directory (if necessary)
    java.io.File parentDir = new java.io.File(DIR_FOR_DOWNLOADS);
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      throw new IOException("Unable to create parent directory");
    }
    OutputStream out = new FileOutputStream(new java.io.File(parentDir, uploadedFile.getTitle()));

    MediaHttpDownloader downloader =
        new MediaHttpDownloader(httpTransport, drive.getRequestFactory().getInitializer());
    downloader.setDirectDownloadEnabled(useDirectDownload);
    downloader.download(new GenericUrl(uploadedFile.getDownloadUrl()), out);
  }
*/
}