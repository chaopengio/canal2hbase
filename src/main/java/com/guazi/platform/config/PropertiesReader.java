package com.guazi.platform.config;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by chaopeng on 16/6/2016.
 */
public class PropertiesReader {


    private Properties prop = null;
    private static PropertiesReader instance = null;

    final Logger logger = LoggerFactory.getLogger(PropertiesReader.class);

    protected PropertiesReader() {
        prop = new Properties();
        try {
            URL resource = getClass().getClassLoader().getResource("config.properties");
            prop.load(new InputStreamReader(resource.openStream(), "UTF8"));
        } catch (IOException ex) {
            logger.error("could find config:" + ex.getMessage());
            prop = null;
        }
    }

    public static PropertiesReader getInstance() {
        if (instance == null) {
            instance = new PropertiesReader();
        }
        return instance;
    }

    public String getString(String key) {
        String ans = prop.getProperty(key);
        if (ans != null) {
            ans = ans.trim();
        }
        
        return ans;
    }

    public String getString(String key, String defaultVal) {
        String ans = getString(key);
        return (ans == null || ans.isEmpty()) ? defaultVal : ans;
    }

    public float getFloat(String key, float defaultVal) {
        String ans = getString(key);
        return (ans == null || ans.isEmpty()) ? defaultVal
                : Float.valueOf(ans);
    }

    public int getInt(String key, int defaultVal) {
        String ans = getString(key);
        return (ans == null || ans.isEmpty()) ? defaultVal
                : Integer.valueOf(ans);
    }

    public String[] getArray(String key, String spliter) {
        String ans = getString(key);
        return (ans == null || ans.isEmpty()) ? null
                : ans.split(spliter);
    }

    public List<String> getList(String field) {
        List<String> list = null;
        String[] attrs = getArray(field, ",");
        if (attrs != null) {
            list = new ArrayList<String>();
            for (String attr : attrs) {
                if (!attr.isEmpty()) {
                    list.add(attr);
                }
            }
        }
        return list;
    }

    public Map<String, String> getStrStrMap(String key) {
        Map<String, String> map = new HashMap<String, String>();
        String[] values = getArray(key, ",");
        if (values != null) {
            for (String v : values) {
                String[] datas = v.split(":");
                if (datas != null && datas.length > 1) {
                    map.put(datas[0], datas[1]);
                }
            }
        }
        return map;
    }

    public void setProperty(String key, String value) {
        prop.setProperty(key, value);
    }
}
