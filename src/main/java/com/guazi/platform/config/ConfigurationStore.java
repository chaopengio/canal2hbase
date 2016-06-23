package com.guazi.platform.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chaopeng on 16/6/2016.
 */
public class ConfigurationStore {
    private static final ConfigurationStore instance = buildConfigurationStore();

    final public static String KEYFIELD = "id";
    
    private static String namespace;
    
    public static Map<String, String> tableMapping;

    public static ConfigurationStore buildConfigurationStore() {
        ConfigurationStore store = new ConfigurationStore();
        PropertiesReader propertiesReader = PropertiesReader.getInstance();
        
        
        namespace = propertiesReader.getString("namespace");
        
        tableMapping = new HashMap<String, String>();
        for (String table : propertiesReader.getList("tables")) {
        	tableMapping.put(table, namespace + ":" + table);
        }
        
        return store;
    }

    public String getHbaseTable(String table) {
    	return tableMapping.get(table);
    }

    public static ConfigurationStore getInstance() {
        return instance;
    }

}
