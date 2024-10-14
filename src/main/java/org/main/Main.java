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

        String cleanNYCArrestData1 = "UPDATE nypd_arrest_data_historic "
                + "SET age_group = '0-18' "
                + "WHERE age_group LIKE '<%';";
        String cleanNYCArrestData2 = "UPDATE nypd_arrest_data_historic "
                + "SET arrest_boro = "
                + "CASE "
                + "WHEN arrest_boro = 'B' THEN 'Bronx' "
                + "WHEN arrest_boro = 'S' THEN 'Staten Island' "
                + "WHEN arrest_boro = 'K' THEN 'Brooklyn' "
                + "WHEN arrest_boro = 'M' THEN 'Manhattan' "
                + "WHEN arrest_boro = 'Q' THEN 'Queens' "
                + "ELSE arrest_boro "
                + "END;";

        PGDatabase.applyUpdateStatement(
                postgresURL, username, password, cleanNYCArrestData1);

        PGDatabase.applyUpdateStatement(
                postgresURL, username, password, cleanNYCArrestData2);

        var dataFrame3 = spark.read()
                .format("jdbc")
                .option("url", postgresURL)
                .option("dbtable", "nypd_arrest_data_historic")
                .option("user", username)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .load();
        dataFrame3.createOrReplaceTempView("nypd_arrest_data_historic");

        var crimeByDistrict = spark.sql("SELECT COUNT(*) as total_crimes, arrest_boro "
                + "FROM nypd_arrest_data_historic "
                + "GROUP BY arrest_boro "
                + "ORDER BY 1 DESC;");

        var crimeByAgeRangeAndSex =
                spark.sql("WITH crime_by_age_and_sex AS ( "
                        + "SELECT COUNT(*) as total_crimes, perp_sex, age_group "
                        + "FROM nypd_arrest_data_historic "
                        + "GROUP BY age_group, perp_sex "
                        + "ORDER BY age_group) "
                        + "SELECT * "
                        + "FROM crime_by_age_and_sex "
                        + "WHERE total_crimes > 20 AND perp_sex IN ('M', 'F');");

        var crimeByRaceAndSex = spark.sql("SELECT COUNT(*) as total_crimes, perp_race, perp_sex "
                + "FROM nypd_arrest_data_historic "
                + "GROUP BY perp_sex, perp_race "
                + "ORDER BY 1 DESC;");

        var crimeOverTime = spark.sql("SELECT COUNT(*) as total_crimes, arrest_date "
                + "FROM nypd_arrest_data_historic "
                + "GROUP BY arrest_date "
                + "ORDER BY 2 DESC;");


        PGDatabase.writeToPostgres(
                crimeByDistrict, postgresURL, username, password, "crime_by_district");

        PGDatabase.writeToPostgres(
                crimeByAgeRangeAndSex, postgresURL, username, password, "crime_by_age_range_and_sex");

        PGDatabase.writeToPostgres(
                crimeByRaceAndSex, postgresURL, username, password, "crime_by_race_and_sex");

        PGDatabase.writeToPostgres(
                crimeOverTime, postgresURL, username, password, "crime_over_time");

    }
}