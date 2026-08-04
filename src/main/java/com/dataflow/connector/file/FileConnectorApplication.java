package com.dataflow.connector.file;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FileConnectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileConnectorApplication.class, args);
    }
}