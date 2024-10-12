package org.main;

import org.apache.spark.sql.SparkSession;
import org.helper_utility.BucketUtil;
import org.helper_utility.ZipSerialize;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        final String projectId = "gcp-data-engineering-426405";
        final String bucketName = "gcp-data-engineering-426405-nyc-crime-historic";
        final BucketUtil bucketUtil = new BucketUtil(projectId, bucketName);

        final String objectName = "NYPD_Arrests_Data_Historic.zip";
        final String zipFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\NYPD_Arrests_Data_Historic.zip";
        final String unzipFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\";

        final String host = "localhost";
        final String port = "5432";
        final String databaseName = "nyc_arrest_historic";
        final String username = "postgres";
        final String password = "Thehungryshark1";

        bucketUtil.downloadObject(objectName, zipFilePath);
        ZipSerialize.unzipFile(zipFilePath, unzipFilePath);

        var spark = SparkSession.builder()
                .master("local[*]")
                .appName(projectId)
                .getOrCreate();

        final String csvFile =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\NYPD_Arrests_Data_Historic.csv";

        var dataFrame = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load(csvFile);
        dataFrame.createOrReplaceTempView("nypd_arrests_data_historic");

        /*
        spark.sql("SELECT ARREST_KEY, ARREST_DATE, ARREST_BORO, AGE_GROUP, PERP_SEX, PERP_RACE " +
                "FROM nypd_arrests_data_historic LIMIT 10").show();
        */

        final String transformedCSVOutput
                = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\Transformed_NYPD_Arrests_Data_Historic.csv";

        var dataFrame2 = spark.sql("SELECT ARREST_KEY, ARREST_DATE, ARREST_BORO, AGE_GROUP, PERP_SEX, PERP_RACE " +
                "FROM nypd_arrests_data_historic");

        dataFrame2.coalesce(1)
                .write()
                .format("csv")
                .mode("ignore")
                .save(transformedCSVOutput);


    }
}
