package com.mrxu.datasource;

import com.mrxu.mysql.ConnectConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

@Slf4j
public class MySQLConnection implements AutoCloseable {

    private Connection connection;

    private static final String url_format = "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&allowLoadLocalInfile=true";

    public MySQLConnection(ConnectConfig config) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            this.connection = DriverManager.getConnection(String.format(url_format, config.getUrl(), config.getDatabase()), config.getUsername(), config.getPassword());
            this.connection.setAutoCommit(true);
        } catch (Exception e) {
            log.error("Failed to initialize DataSource", e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) {
        String url = "localhost:3306";
        String database = "test";
        System.out.println(String.format(url_format, url, database));
    }
}
