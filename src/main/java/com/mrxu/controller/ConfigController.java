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
    }

    private void check(ConnectConfig config){
        if (!StringUtils.hasLength(config.getUrl())) {
            throw new IllegalArgumentException("MySQL rul can not be null");
        }

        if (!StringUtils.hasLength(config.getDatabase())) {
            throw new IllegalArgumentException("MySQL database can not be null");
        }

        if (!StringUtils.hasLength(config.getUsername())) {
            throw new IllegalArgumentException("MySQL username can not be null");
        }

        if (!StringUtils.hasLength(config.getPassword())) {
            throw new IllegalArgumentException("MySQL password can not be null");
        }

        if (CollectionUtils.isEmpty(config.getFields())) {
            throw new IllegalArgumentException("MySQL fields can not be null");
        }
    }
}
