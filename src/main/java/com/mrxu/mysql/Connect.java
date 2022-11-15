package com.mrxu.mysql;

import lombok.Data;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/13 11:13
 */
@Data
public class Connect {
    private ConnectConfig source;
    private ConnectConfig target;
}
