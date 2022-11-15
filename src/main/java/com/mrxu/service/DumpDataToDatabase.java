package com.mrxu.service;

import com.mrxu.datasource.DataSourceConfig;
import com.mrxu.mysql.ConnectConfig;
import com.mrxu.util.Utils;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcStatement;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DumpDataToDatabase {

    private DataSourceConfig targetDataSource;

    private static final String sql_template = "load data local infile 'temp_file.csv' ignore into table %s.%s(%s)";

    public DumpDataToDatabase(DataSourceConfig targetDataSource) {
        this.targetDataSource = targetDataSource;
    }

    public int loadLocalFile(InputStream dataStream) throws SQLException {
        if (dataStream == null) {
            return 0;
        }
        PreparedStatement prepareStatement = targetDataSource.getDataSource().getConnection().prepareStatement(getSQL());
        if (prepareStatement.isWrapperFor(JdbcStatement.class)) {
            JdbcPreparedStatement mysqlStatement = prepareStatement.unwrap(JdbcPreparedStatement.class);
            mysqlStatement.setLocalInfileInputStream(dataStream);
            return mysqlStatement.executeUpdate();
        }
        return 0;
    }

    private String getSQL() {
        ConnectConfig config = targetDataSource.getConfig();
        return String.format(sql_template, config.getDatabase(), config.getTable(), Utils.getFieldStr(config.getFields()));
    }

}