package com.gdrivefs.simplecache;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.jimsproch.sql.DatabaseRow;

public class FileContent {
	public static final int FRAGMENT_BOUNDARY = 1<<25; //32 MiB

	private final Drive drive;
	private final UUID localFileId;
	private final ReentrantLock scratchSpaceLock; // MUST ALWAYS BE ACQUIRED BEFORE WRITELOCK IF ACQUIRED IN SUCCESSION
	private final AtomicReference<SimpleFileMetadata> metadata;

	public FileContent(Drive drive,
			UUID localFileId,
			AtomicReference<SimpleFileMetadata> metadata) {
		this.drive = drive;
		this.localFileId = localFileId;
		this.metadata = metadata;
		scratchSpaceLock = new ReentrantLock();
	}
	
	public String storeTruncatedFileToUploadFile(long truncateOffset) throws IOException {
		if (!scratchSpaceLock.isHeldByCurrentThread()) {
			throw new Error("must be holding scratch space lock");
		}
		java.io.File preTruncated = getUploadFile();
		if (!preTruncated.exists()) {
			throw new Error("storeTruncatedFileToUploadFile assumes we have an existing upload file to truncate");
		}
		FileUtils.copyFile(preTruncated, getScratchFile());
		try (FileOutputStream fos = new FileOutputStream(getScratchFile(), true)) {
			fos.getChannel().truncate(truncateOffset);
		}
		String fileMd5 = null;
		try (FileInputStream fis = new FileInputStream(getScratchFile())) {
			fileMd5 = DigestUtils.md5Hex(fis);
		}
		getCacheFile(fileMd5).delete();
		FileUtils.moveFile(getScratchFile(), getCacheFile(fileMd5));
		return fileMd5;
	}

	private java.io.File getScratchFile() throws IOException
	{
		java.io.File uploadFile = new java.io.File(
				new java.io.File(
						new java.io.File(
								System.getProperty("user.home"), ".googlefs"),
						"upload_scratch"),
					localFileId.toString());
		uploadFile.getParentFile().mkdirs();
		return uploadFile;
	}

	String storeFragmentsToUploadFile() throws IOException {
		if (!scratchSpaceLock.isHeldByCurrentThread()) {
			throw new Error("this method expects us to be holding the scratch space lock");
		}
		java.io.File scratchFile = getScratchFile();
    	List<DatabaseRow> rows = drive.getDatabase().getRows(
    			"SELECT * FROM FRAGMENTS "
    			+ "WHERE LOCALID=? "
    			+ "ORDER BY STARTBYTE ASC, ENDBYTE DESC",
    			localFileId);

    	try (FileOutputStream out = new FileOutputStream(scratchFile)) {
    		long position = 0;
    		for (DatabaseRow row : rows) {
    			long startByte = row.getLong("STARTBYTE");
    			if (startByte > position) {
    				throw new Error("unexpected gap");
    			}
    			long endByte = row.getLong("ENDBYTE");
    			if (endByte <= position) {
    				continue;
    			}
    			byte[] chunk = FileUtils.readFileToByteArray(getCacheFile(row.getString("CHUNKMD5")));
    			// sanity check
    			if (chunk.length != endByte-startByte) {
    				throw new Error("unexpected byte array from file");
    			}
    			out.write(chunk, (int)(position-startByte), (int)(endByte-position));
    			position += (endByte - position);
    			if (position == metadata.get().size) { // TODO (smacke): query dynamically?
    				break;
    			}
    		}
    		if (position != metadata.get().size) {
    			throw new Error("something went wrong reading upload file from fragments");
    		}
    	}
    	String fileMd5;
    	try (FileInputStream scratchInputStream = new FileInputStream(scratchFile)) {
    		fileMd5 = DigestUtils.md5Hex(scratchInputStream);
    	}
    	java.io.File uploadFile = getCacheFile(fileMd5);
    	scratchFile.renameTo(uploadFile);
    	return fileMd5;
    	// TODO (smacke): perhaps rather than returning the fileMd5,
    	// we should just insert it into the DB here. This would force us
    	// to rely on the assumption that nobody else updates this DB field
    	// unless they're behind the scratch space lock, though. :(
	}

	public byte[] getBytesByAnyMeans(long start, long end) throws IOException
	{
		// this will be holding a read lock
		if (drive.lock.getReadHoldCount() <= 0) {
			throw new Error("need a read lock to do reads!");
		}
		fillInGapsBetween(start, end);
		byte[] output = new byte[(int)(end-start)];
		List<DatabaseRow> fragments = drive.getDatabase().getRows(
				"SELECT * FROM FRAGMENTS "
				+ "WHERE LOCALID=? AND STARTBYTE < ? AND ENDBYTE > ? "
				+ "ORDER BY STARTBYTE ASC", localFileId, end, start);

		long currentPosition = start;
		for(DatabaseRow fragment : fragments)
		{
			long startbyte = fragment.getInteger("STARTBYTE");
			long endbyte = fragment.getInteger("ENDBYTE");
			String chunkMd5 = fragment.getString("CHUNKMD5");
			java.io.File cachedChunkFile = getCacheFile(chunkMd5);

			if(!cachedChunkFile.exists() || cachedChunkFile.length() != endbyte-startbyte)
			{
				drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE CHUNKMD5=?", chunkMd5);
				continue;
			}

			if (startbyte > currentPosition) {
				throw new Error("should not have gaps");
			}

			// Consume the fragment
			int copyStart = (int)(currentPosition-startbyte);
			System.out.println("endbyte"+endbyte+" "+"currentPosition"+currentPosition+" "+"startbyte"+startbyte+" "+"readend"+end);
			int copyEnd = Math.min((int)(endbyte-startbyte), (int)(end-startbyte));
			System.out.println("readstart"+start+" "+"readend"+end+" "+"copyStart"+copyStart+" "+"copyEnd"+copyEnd+" "+"destpos"+(currentPosition-start)+" "+"length"+(copyEnd-copyStart)+" ");
			System.out.println("outputlength"+output.length+" "+"fragmentlength"+FileUtils.readFileToByteArray(cachedChunkFile).length);
			System.arraycopy(FileUtils.readFileToByteArray(cachedChunkFile), copyStart, output, (int)(currentPosition-start), copyEnd-copyStart);
			currentPosition += copyEnd-copyStart;

			if (currentPosition >= end) {
				if (currentPosition > end) {
					throw new Error("should not be reading past end");
				}
				break;
			}
		}

		if (currentPosition < end) {
			throw new Error("unexpected gap at end");
		}

		return output;
	}

	void fillInGapsBetween(long start, long end) throws IOException
	{
		List<DatabaseRow> fragments = drive.getDatabase().getRows("SELECT * FROM FRAGMENTS "
				+ "WHERE LOCALID=? AND STARTBYTE < ? AND ENDBYTE > ? "
				+ "ORDER BY STARTBYTE ASC",
				localFileId, end, start);

		long currentPosition = start;
		for(DatabaseRow fragment : fragments)
		{
			long startbyte = fragment.getLong("STARTBYTE");
			long endbyte = fragment.getLong("ENDBYTE");
			String chunkMd5 = fragment.getString("CHUNKMD5");
			java.io.File cachedChunkFile = getCacheFile(chunkMd5);

			if(!cachedChunkFile.exists() || cachedChunkFile.length() != endbyte-startbyte)
			{
				drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE CHUNKMD5=?", chunkMd5);
				continue;
			}

			// If the fragment starts after the byte we need, download the piece we still need
			// TODO (smacke): If the gap is larger than 32 MiB, need to break it up into chunks.
			// I think this will never happen with reads, only with updates for files
			// with > 32 MiB gaps.
			if(startbyte > currentPosition) {
				long gapEnd = Math.min(startbyte, end);
//				logger.info("gap discovered for {} ({}) between bytes {} and {}",
//						getGoogleId(), getTitle(), currentPosition, gapEnd);
				currentPosition += downloadFragment(currentPosition, gapEnd);
			}

			// Consume the fragment
			currentPosition += Math.min(endbyte - currentPosition, end - currentPosition);
		}
		currentPosition += downloadFragment(currentPosition, end);
	}

    /**
     *
     * @param startPosition
     * @param endPosition
     * @return Number of bytes downloaded.
     * @throws IOException
     */
    private int downloadFragment(long startPosition, long endPosition) throws IOException
	{
    	String md5 = metadata.get().fileMd5;
    	
    	System.out.println("Downloading for " + metadata.get().title + " " +md5+" "+startPosition+" "+endPosition);
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition > endPosition) throw new IllegalArgumentException("startPosition (" + startPosition + ") must not be greater than endPosition (" + endPosition + ")");
		if(startPosition == endPosition) return 0;

        ByteArrayOutputStream out = new ByteArrayOutputStream();

		HttpRequestFactory requestFactory = drive.getTransport().createRequestFactory(drive.getRemote().getRequestFactory().getInitializer());

		HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(metadata.get().downloadUrl)); // TODO: query dynamically
		request.getHeaders().setRange("bytes=" + (startPosition) + "-" + (endPosition - 1));
		HttpResponse response = request.execute();
		try
		{
			IOUtils.copy(response.getContent(), out);
		}
		finally
		{
			response.disconnect();
		}

        byte[] bytes = out.toByteArray();

        storeFragmentNoMerges(md5, startPosition, bytes);

        return bytes.length;
    }

    /**
     * Ideally, this method should only do destructive database ops when write-lock protected.
     * E.g. anything inserted during a download should not overlap any existing fragments.
     *
     * @param fileMd5
     * @param fragmentStartByte
     * @param fragment
     * @throws IOException
     */
    public void storeFragment(
    		@Nullable String fileMd5, // null file md5 indicates that this op is coming in locally and is most up-to-date
    		long fragmentStartByte,
    		byte[] fragment) throws IOException {
    	long start = fragmentStartByte;
    	long end = fragmentStartByte + fragment.length;
    	if (fragment.length==0) {
    		return;
    	}
    	// get overlapping fragments
    	// sort by startbyte asc
    	// then endbyte desc
    	// this allows for a slight optimization during merging --
    	// we use the fragment that spans more data when possible,
    	// if two fragments start in the same place (fewer I/O calls)
    	List<DatabaseRow> rows = drive.getDatabase().getRows(
    			"SELECT * FROM FRAGMENTS "
    			+ "WHERE LOCALID=? AND "
    			+ "((ENDBYTE > ? AND ENDBYTE <= ?) "
    			+ "OR (STARTBYTE >= ? AND STARTBYTE < ?) "
    			+ "OR (STARTBYTE <= ? AND ENDBYTE >= ?)) "
    			+ "ORDER BY STARTBYTE ASC, ENDBYTE DESC",
    			localFileId, start, end, start, end, start, end);

    	long globalStartByte = start;
    	long globalEndByte = end;
    	for (DatabaseRow row : rows) {
    		globalEndByte = Math.max(globalEndByte, row.getLong("ENDBYTE"));
    	}

    	byte[] merged;
    	if (rows.size() > 0) {
    		globalStartByte = Math.min(globalStartByte, rows.get(0).getLong("STARTBYTE"));
    		merged = new byte[(int)(globalEndByte - globalStartByte)];

    		System.arraycopy(fragment, 0, merged, (int)(start-globalStartByte), fragment.length);

    		long position = globalStartByte;

    		// skip the new fragment; we've already done an arraycopy
    		if (position == start) {
    			position = end;
    		}
    		for (int chunk=0; chunk<rows.size(); chunk++) {
    			long chunkEnd = rows.get(chunk).getLong("ENDBYTE");
    			if (position >= chunkEnd) {
    				continue;
    			}
    			String chunkMd5 = rows.get(chunk).getString("CHUNKMD5");
    			long chunkStart = rows.get(chunk).getLong("STARTBYTE");
    			if (position < chunkStart) {
    				throw new Error("inexplicable gap");
    			}
    			if (chunkEnd - chunkStart > FRAGMENT_BOUNDARY) {
    				throw new Error("chunk larger than max google fragment size!");
    			}
    			byte[] fileBytes = FileUtils.readFileToByteArray(getCacheFile(chunkMd5));
    			while (position < chunkEnd) {
    				merged[(int)(position-globalStartByte)] = fileBytes[(int)(position-chunkStart)];
    				position++;
    				// skip the new fragment; we've already done an arraycopy
    				if (position == start) {
    					position = end;
    				}
    			}
    		}

    		drive.getDatabase().execute(
    				"DELETE FROM FRAGMENTS "
    				+ "WHERE LOCALID=? AND "
    				+ "((ENDBYTE > ? AND ENDBYTE <= ?) OR "
    				+ "(STARTBYTE >= ? AND STARTBYTE < ?) OR "
    				+ "(STARTBYTE <= ? AND ENDBYTE >= ?))",
    				localFileId.toString(), start, end, start, end, start, end);
    	} else {
    		merged = fragment;
    	}
    	storeFragmentNoMerges(fileMd5, globalStartByte, merged);
    }

    public void storeFragmentNoMerges(
    		@Nullable String fileMd5, // null file md5 indicates that this op is coming in locally and is most up-to-date
    		long fragmentStartByte,
    		byte[] fragment) throws IOException {

    	String chunkMd5 = DigestUtils.md5Hex(fragment);
    	FileUtils.writeByteArrayToFile(getCacheFile(chunkMd5), fragment);
    	drive.getDatabase().execute(
    			"INSERT INTO FRAGMENTS"
    			+ "(LOCALID, FILEMD5, CHUNKMD5, STARTBYTE, ENDBYTE)"
    			+ "VALUES(?,?,?,?,?)",
    			localFileId, fileMd5, chunkMd5, fragmentStartByte, fragmentStartByte + fragment.length);
    }

    public void dropFragmentsStartingAtOrAfter(long offset) throws IOException {
    	if (!drive.lock.writeLock().isHeldByCurrentThread()) {
    		throw new Error("need write lock to do writes");
    	}
    	drive.getDatabase().execute("DELETE FROM FRAGMENTS "
    			+ "WHERE LOCALID=? AND STARTBYTE>=?",
    			localFileId, offset);
    }

	void dropFragmentsFromDb() throws IOException {
		drive.lock.writeLock().lock();
		try {
			drive.getDatabase().execute("DELETE FROM FRAGMENTS WHERE LOCALID=?", localFileId);
		} finally {
			drive.lock.writeLock().unlock();
		}
	}
	
	java.io.File getUploadFile() {
		return getCacheFile(metadata.get().fileMd5);
	}

	private static java.io.File getCacheFile(String chunkMd5) {
		java.io.File cacheFile = new java.io.File(new java.io.File(System.getProperty("user.home"), ".googlefs"), "cache");
		for(byte c : chunkMd5.getBytes()) {
			cacheFile = new java.io.File(cacheFile, Character.toString((char) c));
		}
		cacheFile = new java.io.File(cacheFile, chunkMd5);
		return cacheFile;
	}
	
	ReentrantLock scratchSpaceLock() {
		return scratchSpaceLock;
	}
}
