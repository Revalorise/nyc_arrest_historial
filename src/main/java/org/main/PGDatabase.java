package org.main;

import org.helper_utility.DatabaseUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PGDatabase {

    static Connection conn;

    public static void createNYCArrestTable(String postgresURL, String username, String password) {
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

    private static boolean tableIsEmpty(String postgresURL, String username, String password, String tableName) {
        boolean flag;
        try {
            conn = DatabaseUtil.getConnection(postgresURL, username, password);

            String getRowCount = "SELECT COUNT(*) FROM " + tableName;
            Statement statement = conn.createStatement();

            try (ResultSet rs = statement.executeQuery(getRowCount)) {
                rs.next();
                int count = rs.getInt(1);
                flag = count == 0;
            }

            return flag;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return false;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void insertNYCArrestData(String postgresURL, String username, String password) {
        try {
            if (tableIsEmpty(postgresURL, username, password, "nypd_arrest_data_historic")) {
                conn = DatabaseUtil.getConnection(postgresURL, username, password);

                String insertCsvData = "COPY nypd_arrest_data_historic"
                        + "(arrest_key, arrest_date, arrest_boro, age_group, perp_sex, perp_race) "
                        + "FROM 'C:\\Users\\Ball\\Desktop\\Weather_pipeline\\temp\\Transformed_NYPD_Arrests_Data_Historic.csv' "
                        + "DELIMITER ',' "
                        + "CSV HEADER ";

                Statement statement = null;
                try {
                    statement = conn.createStatement();
                    statement.executeUpdate(insertCsvData);
                } finally {
                    DatabaseUtil.closeStatement(statement);
                }

                DatabaseUtil.commit(conn);
                System.out.println("insertCsvData statement executed.");
            } else {
                System.out.println("Table already exists or isn't empty.");
            }
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
            DatabaseUtil.rollback(conn);
        } finally {
            DatabaseUtil.closeConnection(conn);
        }
    }
}
