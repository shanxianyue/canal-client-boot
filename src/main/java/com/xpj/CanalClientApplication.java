package com.xpj;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class CanalClientApplication {
    public static void main(String[] args) {
        SpringApplication.run(CanalClientApplication.class, args);
    }
}
