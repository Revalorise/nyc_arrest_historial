package org.main;

import org.apache.spark.sql.SparkSession;
import org.helper_utility.BucketUtil;
import org.helper_utility.DatabaseUtil;
import org.helper_utility.ZipSerialize;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

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

        // bucketUtil.downloadObject(objectName, zipFilePath);
        // ZipSerialize.unzipFile(zipFilePath, unzipFilePath);

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

        final String transformedCSVOutput
                = "C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\Transformed_NYPD_Arrests_Data_Historic";

        final var dataFrame2 = spark.sql("SELECT ARREST_KEY, ARREST_DATE, ARREST_BORO, AGE_GROUP, PERP_SEX, PERP_RACE " +
                "FROM nypd_arrests_data_historic");

        dataFrame2.coalesce(1)
                .write()
                .format("csv")
                .mode("ignore")
                .save(transformedCSVOutput);

        final String host = "localhost";
        final String port = "5432";
        final String databaseName = "nyc_arrest_historic";
        final String postgresURL = DatabaseUtil.getPostgresURL(host, port, databaseName);
        final String username = "postgres";
        final String password = "Thehungryshark1";

        Connection conn = null;
        try {
            conn = DatabaseUtil.getConnection(postgresURL, username, password);

            String createTable = "CREATE TABLE IF NOT EXISTS nypd_arrests_data_historic ( "
                    + "arrest_key VARCHAR(50), "
                    + "arrest_date DATE, "
                    + "arrest_date VARCHAR(50), "
                    + "age_group VARCHAR(50), "
                    + "perp_sex VARCHAR(50), "
                    + "perp_race VARCHAR(50)) ";

            Statement statement = null;
            try {
                statement = conn.createStatement();
                statement.executeUpdate(createTable);
            } finally {
                DatabaseUtil.closeStatement(statement);
            }

            DatabaseUtil.commit(conn);
            System.out.println("createTable statement executed.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            DatabaseUtil.rollback(conn);
        } finally {
            DatabaseUtil.closeConnection(conn);
        }

        /*
        try {
            conn = DatabaseUtil.getConnection(postgresURL, username, password);

            String insertCsvData = "COPY nypd_arrests_data_historic"
                    + "(arrest_key, arrest_date, age_group, perp_sex, perp_race) "
                    + "FROM 'C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\'";
        }
         */
    }
}
