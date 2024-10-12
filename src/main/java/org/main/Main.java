package org.main;

import org.helper_utility.BucketUtil;
import org.helper_utility.ZipSerialize;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        final String projectId = "gcp-data-engineering-426405";
        final String bucketName = "gcp-data-engineering-426405-nyc-crime-historic";
        final BucketUtil bucketUtil = new BucketUtil(projectId, bucketName);

        final String objectName = "NYPD_Arrests_Data_Historic.zip";
        final String zipFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\NYPD_Arrests_Data_Historic.zip";
        final String unzipFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\";

        // bucketUtil.downloadObject(objectName, zipFilePath);
        // ZipSerialize.unzipFile(zipFilePath, unzipFilePath);


    }
}
