package com.guazi.platform;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.guazi.platform.common.Action;
import com.guazi.platform.config.ConfigurationStore;
import com.guazi.platform.hbase.HbaseClient;

import net.minidev.json.JSONObject;

/**
 * Created by chaopeng on 16/6/2016.
 */
public class Operator {
    private static HbaseClient hbaseClient;
    private ConfigurationStore config;

    public Operator(Configuration conf) throws IOException {
        hbaseClient = new HbaseClient(conf);
        config = ConfigurationStore.getInstance();
    }

    public Operator() throws IOException {
        this(null);
    }

    public void process(Action action) throws IOException {
        if (action.getActionType() == Action.ActionType.DELETE) {
            delete(action);
        } else if (action.getActionType() == Action.ActionType.INSERT || action.getActionType() == Action.ActionType.UPDATE) {
            put(action);
        }
    }

    public void delete(Action action) throws IOException {
        String table = config.getHbaseTable(action.getTable());
        for (Object obj: action.getData()) {
            JSONObject json  = (JSONObject) obj;
            String row = json.getAsString(ConfigurationStore.KEYFIELD);
            hbaseClient.delete(table, row);
        }
    }

    public void put(Action action) throws IOException {
        String table = config.getHbaseTable(action.getTable());
        for (Object obj: action.getData()) {
            JSONObject json  = (JSONObject) obj;
            String row = json.getAsString(ConfigurationStore.KEYFIELD);
            hbaseClient.put(table, row, json);
        }
    }
    
    public void flush() throws IOException {
    	hbaseClient.flushAndClose();
    }
}
