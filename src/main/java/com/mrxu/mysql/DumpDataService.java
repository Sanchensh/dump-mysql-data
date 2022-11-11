package com.mrxu.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcStatement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Service
public class DumpDataService implements ApplicationRunner {

    @Autowired
    private DruidDataSource dataSource;


    public InputStream getTestDataInputStream() {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= 10; i++) {
            for (int j = 0; j <= 10000; j++) {

                builder.append(4);
                builder.append("\t");
                builder.append(4 + 1);
                builder.append("\t");
                builder.append(4 + 2);
                builder.append("\t");
                builder.append(4 + 3);
                builder.append("\t");
                builder.append(4 + 4);
                builder.append("\t");
                builder.append(4 + 5);
                builder.append("\n");
            }
        }
        byte[] bytes = builder.toString().getBytes();
        InputStream is = new ByteArrayInputStream(bytes);
        return is;
    }

    public int loadLocalFile(String loadDataSql, InputStream dataStream) throws SQLException {
        if (dataStream == null) {
            return 0;
        }
        PreparedStatement prepareStatement = dataSource.getConnection().prepareStatement(loadDataSql);
        int result = 0;
        if (prepareStatement.isWrapperFor(JdbcStatement.class)) {
            JdbcPreparedStatement mysqlStatement = prepareStatement.unwrap(JdbcPreparedStatement.class);
            mysqlStatement.setLocalInfileInputStream(dataStream);
            result = mysqlStatement.executeUpdate();
        }
        return result;
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println(dataSource.getConnection());
        System.out.println("hello");
    }
}