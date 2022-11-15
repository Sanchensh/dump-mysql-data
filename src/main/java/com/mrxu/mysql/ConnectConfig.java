package com.mrxu.mysql;

import lombok.Data;

import java.util.List;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/13 11:12
 */
@Data
public class ConnectConfig {
    private String username;
    private String password;
    private String url;
    private String database;
    private String table;
    private Long minId = 0L;
    private Long maxId = 0L;
    private Integer batchSize = 1000;
    private String indexName = "id";
    private List<String> fields;
}
