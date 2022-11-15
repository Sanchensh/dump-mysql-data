package com.mrxu.datasource;


import com.alibaba.druid.pool.DruidDataSource;
import com.mrxu.mysql.ConnectConfig;


public class DataSourceConfig implements AutoCloseable {

    private DruidDataSource dataSource;
    private ConnectConfig config;

    private static final String url_format = "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&allowLoadLocalInfile=true";

    public DataSourceConfig(ConnectConfig config) {
        this.dataSource = new DruidDataSource();
        this.dataSource.setUrl(String.format(url_format, config.getUrl(), config.getDatabase()));
        this.dataSource.setUsername(config.getUsername());
        this.dataSource.setPassword(config.getPassword());
        this.dataSource.setKeepAlive(true);
        this.config = config;
    }

    public DruidDataSource getDataSource() {
        return dataSource;
    }

    public ConnectConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    public static void main(String[] args) {
        String url = "localhost:3306";
        String database = "test";
        System.out.println(String.format(url_format, url, database));
    }
}
