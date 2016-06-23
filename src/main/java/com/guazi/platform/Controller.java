package com.guazi.platform;

import java.io.IOException;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.guazi.platform.common.Action;
import com.guazi.platform.config.PropertiesReader;


/**
 * Created by chaopeng on 15/6/2016.
 */
public class Controller extends TimerTask {

    final static Logger logger = LoggerFactory.getLogger(Controller.class);
    private CanalConnector connector;
    private Operator operator;
    
    final static int BATCH_SIZE = 10000;


    public Controller() throws IOException {
        this(null);
    }

    public Controller(CanalConnector connector) throws IOException {
        this.connector = connector;

        Configuration conf = HBaseConfiguration.create();
        PropertiesReader propertiesReader = PropertiesReader.getInstance();
    	String zk = propertiesReader.getString("zookeeper.quorum");
        conf.set("hbase.zookeeper.quorum", zk);
        this.operator = new Operator(conf);
        
        this.connector.connect();
        this.connector.subscribe();
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

	@Override
	public void run() {
		Message message = connector.getWithoutAck(BATCH_SIZE);
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
        	return;
        } 
        
        for (CanalEntry.Entry entry : message.getEntries()) {
            EntryExtractor extractor = new EntryExtractor(entry);
//            logger.info(extractor.toString());
            Action action = extractor.getAction();
            if (action == null || !action.isValid()) {
                continue;
            }

            try {
                operator.process(action);
            } catch (IOException e) {
                logger.error("operator process action error, ", e);
                break;
            }
        }
        
        try {
        	operator.flush();
        	connector.ack(batchId);
        } catch (IOException e) {
            logger.error("operator flush error", e);
            connector.rollback(batchId);
        }
        logger.info("batch id: " + String.valueOf(batchId));
    }
	

}
