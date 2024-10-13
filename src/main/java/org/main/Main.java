package org.main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.SparkSession;

import org.helper_utility.BucketUtil;
import org.helper_utility.DatabaseUtil;
import org.helper_utility.ZipSerialize;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {
        final Properties gcsProperties = new Properties();
        gcsProperties.load(
                new FileInputStream("C:\\Users\\Ball\\Desktop\\Weather_pipeline\\GCS.properties"));

        final String projectId = gcsProperties.getProperty("projectId");
        final String bucketName = gcsProperties.getProperty("bucketName");
        final BucketUtil bucketUtil = new BucketUtil(projectId, bucketName);

        final String objectName = "NYPD_Arrests_Data_Historic.zip";
        final String zipFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\NYPD_Arrests_Data_Historic.zip";
        final String unzipFilePath =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\";
        final String extractedCsvFile =
                "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\NYPD_Arrests_Data_Historic.csv";
        final String transformedCSVOutput
                = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\Transformed_NYPD_Arrests_Data_Historic";

        final Properties PG_Properties = new Properties();
        PG_Properties.load(new FileInputStream("C:\\Users\\Ball\\Desktop\\Weather_pipeline\\Postgres.properties"));
        final String host = PG_Properties.getProperty("host");
        final String port = PG_Properties.getProperty("port");
        final String databaseName = PG_Properties.getProperty("databaseName");
        final String postgresURL = DatabaseUtil.getPostgresURL(host, port, databaseName);
        final String username = PG_Properties.getProperty("username");
        final String password = PG_Properties.getProperty("password");

        // bucketUtil.downloadObject(objectName, zipFilePath);
        // ZipSerialize.unzipFile(zipFilePath, unzipFilePath);

        final var spark = SparkSession.builder()
                .master("local[*]")
                .appName(projectId)
                .getOrCreate();

        final var dataFrame = spark.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load(extractedCsvFile);
        dataFrame.createOrReplaceTempView("nypd_arrests_data_historic");

        final var dataFrame2 = spark.sql("SELECT ARREST_KEY, ARREST_DATE, ARREST_BORO, AGE_GROUP, PERP_SEX, PERP_RACE " +
                "FROM nypd_arrests_data_historic");

        dataFrame2.coalesce(1)
                .write()
                .format("csv")
                .mode("ignore")
                .save(transformedCSVOutput);

        final File transformedCSVDir = new File(transformedCSVOutput);
        final File[] files = transformedCSVDir.listFiles(file -> file.getName().endsWith(".csv"));
        assert files != null;
        for (File file : files) {
            if (file.getName().startsWith("Transformed") && file.getName().endsWith(".csv")) {
                System.out.println("File name has already been renamed.");
                break;
            } else {
                boolean success =
                        file.renameTo(new File(transformedCSVDir + ".csv"));
                if (success) {
                    System.out.println("Successfully renamed a file from: "
                            + file.getName() + " to: " + transformedCSVDir.getName() + ".csv");
                } else {
                    System.out.println("Failed to rename a file");
                }
            }
        }

        PGDatabase.createNYCArrestTable(postgresURL, username, password);
        PGDatabase.insertNYCArrestData(postgresURL, username, password);

        var dataFrame3 = spark.read()
                .format("jdbc")
                .option("url", postgresURL)
                .option("dbtable", "nypd_arrest_data_historic")
                .option("user", username)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .load();
        dataFrame3.createOrReplaceTempView("nypd_arrest_data_historic");

        var crimeByRace = spark.sql("SELECT COUNT(*) as total_crimes, perp_race "
                + "FROM nypd_arrest_data_historic "
                + "GROUP BY perp_race "
                + "ORDER BY 1 DESC;");

        PGDatabase.writeToPostgres(
                crimeByRace, postgresURL, username, password, "crime_by_race");

        var crimeByDistrict = spark.sql("SELECT COUNT(*) as total_crimes, arrest_boro "
                + "FROM nypd_arrest_data_historic "
                + "GROUP BY arrest_boro "
                + "ORDER BY 1 DESC;");

        PGDatabase.writeToPostgres(
                crimeByDistrict, postgresURL, username, password, "crime_by_district");

    }
}
