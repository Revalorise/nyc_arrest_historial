package org.helper_utility;

import java.util.ArrayList;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;


public class BucketUtil {
    private final String projectId;

    public BucketUtil(String projectId) {
        this.projectId = projectId;
    }

    public void createBucket(String bucketName) throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
        System.out.println("Bucket: " + bucket.getName() + " created.");
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

    public void deleteBucket(String bucketName) {
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        Bucket bucket = storage.get(bucketName);
        bucket.delete();

        System.out.println("Bucket: " + bucket.getName() + " deleted.");
    }
}
