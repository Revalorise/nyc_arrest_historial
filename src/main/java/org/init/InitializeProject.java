package org.init;

import org.helper_utility.BucketUtil;
import org.helper_utility.ZipSerialize;

import java.util.ArrayList;

final class InitializeProject {
    public static void main(String[] args) throws Exception {
        BucketUtil bucketUtil =
                new BucketUtil("gcp-data-engineering-426405");

        String filePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Arrests_Data_Historic.csv";
        String outputFilePath
                = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Arrests_Data_Historic.zip";

        String bucketName = "gcp-data-engineering-426405-nyc-crime-historic";
        String objectName = "NYPD_Arrests_Data_Historic.zip";

        ZipSerialize.serializeToZIP(filePath, outputFilePath);

        bucketUtil.createBucket(bucketName);

        assert bucketUtil.checkIfBucketExists(
                "gcp-data-engineering-426405-nyc-crime-historic"
        );

        bucketUtil.uploadObject(bucketName, objectName, outputFilePath);

        ArrayList<String> objectList = bucketUtil.listBucketObjects(bucketName);
        assert !objectList.isEmpty();
        assert objectList.contains(objectName);

        ZipSerialize.deleteZipFile(outputFilePath);
    }
}
