package com.guazi.platform;

import java.io.IOException;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.guazi.platform.config.PropertiesReader;

/**
 * Created by chaopeng on 15/6/2016.
 */
public class Main {
    final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String args[]) throws IOException {
    	PropertiesReader propertiesReader = PropertiesReader.getInstance();
    	String zk = propertiesReader.getString("zookeeper.quorum");
    	String des = propertiesReader.getString("destination");
        CanalConnector connector = CanalConnectors.newClusterConnector(zk, des, "", "");

        long period = 1000L;
        
        final Controller controller = new Controller(connector);
        
        Timer timer = new Timer();  
		timer.schedule(controller, 1000, period);

    }
}
