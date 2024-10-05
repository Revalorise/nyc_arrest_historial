package org.helper_utility;

import java.nio.file.Paths;
import java.util.ArrayList;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class BucketUtil {
    private final String projectId;
    private static final Logger logger = LogManager.getLogger(BucketUtil.class);

    public BucketUtil(String projectId) {
        this.projectId = projectId;
    }

    public void createBucket(String bucketName) throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        if (storage.get(bucketName).exists()) {
            logger.info("Bucket already exists: " + bucketName);
        } else {
            Bucket bucket = storage.create(BucketInfo.of(bucketName));
            logger.info("Bucket: {} created.", bucket.getName());
        }
    }

    public ArrayList<String> listBuckets() throws Exception {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Page<Bucket> buckets = storage.list();
        ArrayList<String> bucketList = new ArrayList<>();

        for (Bucket bucket : buckets.iterateAll()) {
            bucketList.add(bucket.getName());
        }

        return bucketList;
    }

    public ArrayList<String> listBucketObjects(String bucketName) throws Exception {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Page<Blob> blobs = storage.list(bucketName);
        ArrayList<String> blobList = new ArrayList<>();

        for (Blob blob : blobs.iterateAll()) {
            blobList.add(blob.getName());
        }
        return blobList;
    }

    public void uploadObject(
            String bucketName, String objectName, String filePath) throws Exception {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        Storage.BlobWriteOption precondition;
        if (storage.get(bucketName, objectName) == null) {
            // For a target object that does not yet exist, set the DoesNotExist precondition.
            // This will cause the request to fail if the object is created before the request runs.
            precondition = Storage.BlobWriteOption.doesNotExist();
        } else {
            // If the destination already exists in your bucket, instead set a generation-match
            // precondition. This will cause the request to fail if the existing object's generation
            // changes before the request runs.
            precondition =
                    Storage.BlobWriteOption.generationMatch(
                            storage.get(bucketName, objectName).getGeneration());
        }
        storage.createFrom(blobInfo, Paths.get(filePath), precondition);

        logger.info("File {} uploaded to bucket {} as {}", filePath, bucketName, objectName);
    }

    public boolean checkIfBucketExists(String bucketName) throws Exception {
        ArrayList<String> bucketList = listBuckets();
        for (String bucket : bucketList) {
            if (bucket.equals(bucketName)) {
                return true;
            }
        }
        return false;
    }

    public void deleteBucket(String bucketName) {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        if (!storage.get(bucketName).exists()) {
            logger.info("Bucket does not exist: {}", bucketName);
        } else {
            Bucket bucket = storage.get(bucketName);
            bucket.delete();
            logger.info("Bucket: {} deleted.", bucket.getName());
        }
    }
}
