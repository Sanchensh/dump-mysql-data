package com.mrxu.util;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 16:33
 */
public class Utils {
    private static final Integer MAXIMUM_CAPACITY = 10240;

    public static InputStream getDataStream(StringBuffer buffer) {
        byte[] bytes = buffer.toString().getBytes();
        return new ByteArrayInputStream(bytes);
    }

    public static int getBatchSize(int cap) {
        int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    public static String getFieldStr(List<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append(fields.get(i));
            if (i != fields.size() - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(getBatchSize(10));
    }
}
