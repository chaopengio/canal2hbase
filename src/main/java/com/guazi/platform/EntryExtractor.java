package com.guazi.platform;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.guazi.platform.common.Action;
import com.guazi.platform.config.ConfigurationStore;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 * Created by chaopeng on 15/6/2016.
 */
public class EntryExtractor {
    final static Logger logger = LoggerFactory.getLogger(EntryExtractor.class);
    private CanalEntry.Entry entry;

    private String entryType;
    private String logFile;
    private String offset;
    private String table;
    private String executeTime;
    private String delayTime;
    
    private ConfigurationStore config = ConfigurationStore.getInstance();

    private Action action = new Action();

    private CanalEntry.RowChange rowChange = null;

    public EntryExtractor(CanalEntry.Entry entry) {
        this.entry = entry;
        this.extract();
    }

    public void extract() {
//        logger.info("begin extract");
        logFile = entry.getHeader().getLogfileName();
        offset = String.valueOf(entry.getHeader().getLogfileOffset());
        executeTime = String.valueOf(entry.getHeader().getExecuteTime());

        long executeTime = entry.getHeader().getExecuteTime();
        long delay = new Date().getTime() - executeTime;
        delayTime = String.valueOf(delay);

        if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
            entryType = "TRANSACTIONBEGIN";
        } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
            entryType = "TRANSACTIONEND";
        } else if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            extractRowData();
        }
    }

    public void extractRowData() {
        table = entry.getHeader().getTableName();
        if (table == null || config.getHbaseTable(table) == null) {
        	return;
        }
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
        }
        CanalEntry.EventType eType = rowChange.getEventType();

        if (eType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
            entryType = "DDL";
            return;
        }

        Action.ActionType actionType = Action.ActionType.OTHERS;
        if (eType == CanalEntry.EventType.DELETE) {
            entryType = "DELETE";
            actionType = Action.ActionType.DELETE;
        } else if (eType == CanalEntry.EventType.INSERT) {
            entryType = "INSERT";
            actionType = Action.ActionType.INSERT;
        } else if (eType == CanalEntry.EventType.UPDATE) {
            actionType = Action.ActionType.UPDATE;
        } else {
            entryType = "OTHERS";
        }

        JSONArray data = new JSONArray();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            JSONObject d = null;
            if (eType == CanalEntry.EventType.DELETE) {
                d = setColumns(rowData.getBeforeColumnsList());
            } else if (eType == CanalEntry.EventType.INSERT) {
                d = setColumns(rowData.getAfterColumnsList());
            } else if (eType == CanalEntry.EventType.UPDATE) {
                d = setColumns(rowData.getAfterColumnsList());
            } else {
                d = setColumns(rowData.getAfterColumnsList());
            }
            data.add(d);
        }

        action = new Action(table, actionType, data);
    }

    public JSONObject setColumns(List<CanalEntry.Column> columns) {
        JSONObject data = new JSONObject();
        for (CanalEntry.Column column : columns) {
            data.put(column.getName(), column.getValue());
        }
        return data;
    }

    public String toString(){
        List<String> list = new ArrayList<String>();

        list.add(logFile);
        list.add(offset);
        list.add(executeTime);
        list.add(delayTime);
        list.add(entryType);
        if (table != null) {
            list.add(table);
        }
//        if (action != null && action.isValid()) {
//            list.add(action.getData().get);
//        }
        return String.join(" | ", list);
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

}
