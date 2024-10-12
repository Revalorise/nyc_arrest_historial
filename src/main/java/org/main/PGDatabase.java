package org.main;

import org.helper_utility.DatabaseUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PGDatabase {
    final static String host = "localhost";
    final static String port = "5432";
    final static String databaseName = "nyc_arrest_historic";
    final static String postgresURL = DatabaseUtil.getPostgresURL(host, port, databaseName);
    final static String username = "postgres";
    final static String password = "Thehungryshark1";
    static Connection conn = null;


    public static void createNYCArrestTable() {
        try {
            conn = DatabaseUtil.getConnection(postgresURL, username, password);

            String createTable = "CREATE TABLE IF NOT EXISTS nypd_arrest_data_historic ( "
                    + "arrest_key VARCHAR(50), "
                    + "arrest_date DATE, "
                    + "arrest_boro CHAR, "
                    + "age_group VARCHAR(50), "
                    + "perp_sex VARCHAR(50), "
                    + "perp_race VARCHAR(50)"
                    + "); ";

            Statement statement = null;
            try {
                statement = conn.createStatement();
                statement.executeUpdate(createTable);
            } finally {
                DatabaseUtil.closeStatement(statement);
            }

            DatabaseUtil.commit(conn);
            System.out.println("createTable statement executed.");

        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
            DatabaseUtil.rollback(conn);
        } finally {
            DatabaseUtil.closeConnection(conn);
        }
    }

    public static void insertNYCArrestData() {
        try {
            conn = DatabaseUtil.getConnection(postgresURL, username, password);

            String insertCsvData = "COPY nypd_arrest_data_historic"
                    + "(arrest_key, arrest_date, arrest_boro, age_group, perp_sex, perp_race) "
                    + "FROM 'C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\Transformed_NYPD_Arrests_Data_Historic.csv' "
                    + "DELIMITER ',' "
                    + "CSV HEADER;";

            Statement statement = null;
            try {
                statement = conn.createStatement();
                statement.executeUpdate(insertCsvData);
            } finally {
                DatabaseUtil.closeStatement(statement);
            }

            DatabaseUtil.commit(conn);
            System.out.println("insertCsvData statement executed.");

        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
            DatabaseUtil.rollback(conn);
        } finally {
            DatabaseUtil.closeConnection(conn);
        }
    }
}
