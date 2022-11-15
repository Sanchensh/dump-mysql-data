package com.mrxu.service;

import com.mrxu.datasource.MySQLConnection;
import com.mrxu.mysql.ConnectConfig;
import com.mrxu.util.Utils;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcStatement;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;

@Slf4j
public class DumpDataToDatabase {

    private ConnectConfig targetConnectConfig;

    private static final String sql_template = "load data local infile 'temp_file.csv' ignore into table %s.%s(%s)";

    public DumpDataToDatabase(ConnectConfig targetConnectConfig) {
        this.targetConnectConfig = targetConnectConfig;
    }

    public int loadLocalFile(InputStream dataStream) throws IOException {
        if (dataStream == null) {
            return 0;
        }
        MySQLConnection targetDataSource = new MySQLConnection(targetConnectConfig);
        try (Connection connection = targetDataSource.getConnection(); PreparedStatement prepareStatement = connection.prepareStatement(getSQL());) {
            if (prepareStatement.isWrapperFor(JdbcStatement.class)) {
                JdbcPreparedStatement mysqlStatement = prepareStatement.unwrap(JdbcPreparedStatement.class);
                mysqlStatement.setLocalInfileInputStream(dataStream);
                return mysqlStatement.executeUpdate();
            }
        } catch (Exception e) {
            log.error("Error while loading Data , error:" + e);
        } finally {
            dataStream.close();
        }
        return 0;
    }

    private String getSQL() {
        return String.format(sql_template, targetConnectConfig.getDatabase(), targetConnectConfig.getTable(), Utils.getFieldStr(targetConnectConfig.getFields()));
    }

    public static void main(String[] args) throws Exception {
        ConnectConfig config = new ConnectConfig();
        config.setUrl("localhost:3306");
        config.setDatabase("dump");
        config.setTable("source_table");
        config.setUsername("root");
        config.setPassword("123456");
        config.setFields(Arrays.asList("id", "name", "age"));
        DumpDataToDatabase database = new DumpDataToDatabase(config);
        int id = 0;
        long time = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            StringBuffer sb = new StringBuffer();
            for (int j = 0; j < 10000; j++) {
                sb.append(id);
                sb.append("\t");

                sb.append("xujl_").append(id % 10);
                sb.append("\t");

                sb.append(28);
                sb.append("\t");

                if (j != 9999) {
                    sb.append("\n");
                }
                id++;
            }
            System.out.println("import：" + id);
            database.loadLocalFile(Utils.getDataStream(sb));
        }
        System.out.println(System.currentTimeMillis() - time);
    }
}