package org.helper_utility;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseUtil {
    private static final Logger logger = LogManager.getLogger(ZipSerialize.class);

    public static String getPostgresURL(String host, String port, String databaseName)
    {
        return "jdbc:postgresql://" + host + ":" + port + "/" + databaseName;
    }

    public static Connection getConnection(String jdbcURL, String username, String password)
            throws ClassNotFoundException, SQLException
    {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(jdbcURL, username, password);
    }

    public static void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void commit(Connection conn) {
        try {
            if (conn != null) {
                conn.commit();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void rollback(Connection conn) {
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

}

