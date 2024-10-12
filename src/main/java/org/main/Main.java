package org.main;

import org.get_cloud_data.GetBucketZip;
import org.helper_utility.BucketUtil;


public class Main {
    public static void main(String[] args) {
        final String projectId = "gcp-data-engineering-426405";
        final String bucketName = "gcp-data-engineering-426405-nyc-crime-historic";
        final BucketUtil bucketUtil = new BucketUtil(projectId, bucketName);

        final String objectName = "NYPD_Arrests_Data_Historic.zip";
        final String destFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\NYPD_Arrests_Data_Historic.zip";


        bucketUtil.downloadObject(objectName, destFilePath);

    }
}
