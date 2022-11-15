package com.mrxu.service;

import com.mrxu.datasource.MySQLConnection;
import com.mrxu.disruptor.producer.DataEventProducer;
import com.mrxu.mysql.ConnectConfig;
import com.mrxu.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 15:17
 */
@Slf4j
public class LoadDataFromDatabase {
    private ConnectConfig sourceConnectConfig;

    private static final String sql_template = "select %s from %s where %s >= %d and %s < %d";

    public LoadDataFromDatabase(ConnectConfig sourceConnectConfig) {
        this.sourceConnectConfig = sourceConnectConfig;
    }

    public void load(long startId, long endId, ConnectConfig targetConfig, DataEventProducer producer) {
        try (MySQLConnection sourceDataSource = new MySQLConnection(sourceConnectConfig); Connection connection = sourceDataSource.getConnection();) {
            PreparedStatement statement = connection.prepareStatement(getSQL(sourceConnectConfig.getTable(), sourceConnectConfig.getIndexName(), startId, endId));
            ResultSet resultSet = statement.executeQuery();
            List<String> fields = targetConfig.getFields();
            while (resultSet.next()) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < fields.size(); j++) {
                    Object object = resultSet.getObject(fields.get(j));
                    sb.append(object);
                    if (j != fields.size() - 1) {
                        sb.append("\t");
                    }
                }
                producer.onData(sb.toString());
            }
        } catch (Exception e) {
            log.error("Failed to load data from database,", e);
        }
    }

    public Long getMaxId() {
        return sourceConnectConfig.getMaxId();
    }

    public Long getMinId() {
        return sourceConnectConfig.getMinId();
    }

    public Integer getBatchSize() {
        return sourceConnectConfig.getBatchSize();
    }

    private String getSQL(String tableName, String indexName, long startId, long endId) {
        return String.format(sql_template, Utils.getFieldStr(sourceConnectConfig.getFields()), tableName, indexName, startId, indexName, endId);
    }
}
