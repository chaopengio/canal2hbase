package com.guazi.platform.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.minidev.json.JSONObject;

/**
 * Created by chaopeng on 16/6/2016.
 */
public class HbaseClient extends Configured {
    final static Logger logger = LoggerFactory.getLogger(HbaseClient.class);

    final static byte[] COLUMNFAMILY = Bytes.toBytes("d");

    private Connection connection;

    final BufferedMutator.ExceptionListener listener;
    
    private ConcurrentMap<String, BufferedMutator> tableCache;

    public HbaseClient(Configuration conf) throws IOException {
        if (conf == null) {
            connection = ConnectionFactory.createConnection();
        } else {
            connection = ConnectionFactory.createConnection(conf);
        }

        listener = new BufferedMutator.ExceptionListener() {
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        
        tableCache = new ConcurrentHashMap<String, BufferedMutator>();
    }

    public HbaseClient() throws IOException {
        this(null);
    }

    public void put(String tableName, String row, Map<String, Object> data) throws IOException {
        Put put = new Put(Bytes.toBytes(row));

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object value = entry.getValue();
            byte[] val = object2byte(value);
            put.addColumn(COLUMNFAMILY, Bytes.toBytes(entry.getKey()), val);
        }

        put(tableName, put);
    }

    public void put(String tableName, String row, JSONObject data) throws IOException {
        Put put = new Put(Bytes.toBytes(row));

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object value = entry.getValue();
            byte[] val = object2byte(value);
            put.addColumn(COLUMNFAMILY, Bytes.toBytes(entry.getKey()), val);
        }

        put(tableName, put);
    }

    public void put(String tableName, Put put) throws IOException {
        BufferedMutator mutator = getTable(tableName);
        mutator.mutate(put);
    }

    public void delete(String tableName, String row) throws IOException {
        delete(tableName, Bytes.toBytes(row));
    }

    public void delete(String tableName, byte[] row) throws IOException {
        Delete del = new Delete(row);
        BufferedMutator mutator = getTable(tableName);
        mutator.mutate(del);
    }

    public byte[] object2byte(Object obj) {
        byte[] b;
        if (obj instanceof byte[]) {
            b = (byte[]) obj;
        } else if (obj instanceof Integer) {
            b = Bytes.toBytes((Integer) obj);
        } else if (obj instanceof String) {
            b = Bytes.toBytes((String) obj);
        } else if (obj instanceof Double) {
            b = Bytes.toBytes((Double) obj);
        } else if (obj instanceof Float) {
            b = Bytes.toBytes((Float) obj);
        } else if (obj instanceof Short) {
            b = Bytes.toBytes((Short) obj);
        } else {
            logger.warn("got unexpected type " + obj.getClass().toString());
            b = Bytes.toBytes((String) obj);
        }

        return b;
    }

    public BufferedMutator getTable(String tableName) throws IOException {
    	if (tableCache.containsKey(tableName)) {
    		return tableCache.get(tableName);
    	}
    	
        TableName table = TableName.valueOf(tableName);
        BufferedMutatorParams params = new BufferedMutatorParams(table);
        BufferedMutator mutator = connection.getBufferedMutator(params);
        tableCache.put(tableName, mutator);
        return mutator;
    }
    
    public void flushAndClose() throws IOException {
    	Collection<BufferedMutator> mutators = tableCache.values();
    	tableCache = new ConcurrentHashMap<String, BufferedMutator>();
    	for (BufferedMutator mutator : mutators) {
    		mutator.flush();
    		mutator.close();
    	}
    }

    public Connection getConnection() {
        return connection;
    }
    
    public void setConnection(Connection connection) {
        this.connection = connection;
    }
    
    public static void main(String[] args) throws IOException {
    	Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181");
        HbaseClient client = new HbaseClient(conf);
        
        JSONObject obj = new JSONObject();
        obj.put("col1", "val1");
        obj.put("col2", "val2");
        client.put("test:canaltest", "testrow1", obj);
        
    }
}
