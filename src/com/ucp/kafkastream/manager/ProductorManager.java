package com.ucp.kafkastream.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


public class ProductorManager {
    private final Logger LOG = LoggerFactory.getLogger(ProductorManager.class) ;


    public ProductorManager(String PROPERTIES_PATH ) throws Exception {
        File file = new File(PROPERTIES_PATH) ;
        InputStream input = new FileInputStream(file) ;
        Properties pros = new Properties() ;
        pros.load(input) ;
    }
}
