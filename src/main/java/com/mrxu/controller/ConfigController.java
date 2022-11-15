package com.mrxu.controller;

import com.mrxu.mysql.Connect;
import com.mrxu.mysql.ConnectConfig;
import com.mrxu.service.DumpService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

@RestController
public class ConfigController {

    @Autowired
    private DumpService service;

    @PostMapping("/dump")
    public String dumpData(@RequestBody Connect connect) {
        check(connect);
        service.start(connect);
        return "Dump MySQL Data Job Start Successful";
    }

    private void check(Connect connect) throws IllegalArgumentException {
        check(connect.getSource());
        check(connect.getTarget());
        if (Objects.isNull(connect.getSource().getMinId())) {
            throw new IllegalArgumentException("Source MySQL minId can not be null");
        }
        if (Objects.isNull(connect.getSource().getMaxId())) {
            throw new IllegalArgumentException("Source MySQL minId can not be null");
        }
    }

    private void check(ConnectConfig config){
        if (!StringUtils.hasLength(config.getUrl())) {
            throw new IllegalArgumentException("Source or Target MySQL url can not be null");
        }

        if (!StringUtils.hasLength(config.getTable())) {
            throw new IllegalArgumentException("Source or Target MySQL table can not be null");
        }

        if (!StringUtils.hasLength(config.getDatabase())) {
            throw new IllegalArgumentException("Source or Target MySQL database can not be null");
        }

        if (!StringUtils.hasLength(config.getUsername())) {
            throw new IllegalArgumentException("Source or Target MySQL username can not be null");
        }

        if (!StringUtils.hasLength(config.getPassword())) {
            throw new IllegalArgumentException("Source or Target MySQL password can not be null");
        }

        if (CollectionUtils.isEmpty(config.getFields())) {
            throw new IllegalArgumentException("Source or Target MySQL fields can not be null");
        }

        if (config.getBatchSize() <= 0) {
            throw new IllegalArgumentException("Source or Target MySQL batchSize can not be null");
        }
    }
}
