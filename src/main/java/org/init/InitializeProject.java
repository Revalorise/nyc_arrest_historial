package org.init;

import org.helper_utility.BucketUtil;
import org.helper_utility.ZipSerialize;

import java.io.IOException;

final class InitializeProject {
    public static void main(String[] args) throws IOException {
        ZipSerialize serializer = new ZipSerialize();
        BucketUtil bucketUtil =
                new BucketUtil("gcp-data-engineering-426405");
        String filePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Arrests_Data_Historic.csv";
        String outputFilePath
                = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\NYPD_Arrests_Data_Historic.zip";

        serializer.serializeToZIP(filePath, outputFilePath);
    }
}
