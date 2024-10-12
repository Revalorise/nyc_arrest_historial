package org.get_cloud_data;

import org.helper_utility.BucketUtil;

public class GetBucketZip {
    public static void downloadBucketObject(String projectId, String bucketName,
                                            String objectName, String destFilePath)
    {
        final BucketUtil bucketUtil = new BucketUtil(projectId, bucketName);
        bucketUtil.downloadObject(objectName, destFilePath);
    }
}
