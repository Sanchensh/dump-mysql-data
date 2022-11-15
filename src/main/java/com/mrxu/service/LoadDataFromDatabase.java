package com.mrxu.service;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.mrxu.datasource.DataSourceConfig;
import com.mrxu.util.Utils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 15:17
 */
public class LoadDataFromDatabase {
    private DataSourceConfig sourceDataSource;

    private static final String sql_template = "select %s from %s where %s >= %d and %s < %d";

    public LoadDataFromDatabase(DataSourceConfig sourceDataSource) {
        this.sourceDataSource = sourceDataSource;
    }

    public ResultSet load(long startId, long endId) throws SQLException {
        DruidPooledConnection connection = sourceDataSource.getDataSource().getConnection();
        PreparedStatement statement = connection.prepareStatement(getSQL(sourceDataSource.getConfig().getTable(), sourceDataSource.getConfig().getIndexName(), startId, endId));
        return statement.executeQuery();
    }

    public Long getMaxId(){
        return sourceDataSource.getConfig().getMaxId();
    }

    public Long getMinId(){
        return sourceDataSource.getConfig().getMinId();
    }

    public Integer getBatchSize(){
        return sourceDataSource.getConfig().getBatchSize();
    }

    private String getSQL(String tableName, String indexName, long startId, long endId) {
        return String.format(sql_template, Utils.getFieldStr(sourceDataSource.getConfig().getFields()), tableName, indexName, startId, indexName, endId);
    }

    public static void main(String[] args) throws SQLException {
        System.out.println(new LoadDataFromDatabase(null).getSQL("test", "id", 0L, 100L));
    }
}
