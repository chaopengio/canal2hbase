package com.guazi.platform.common;


import net.minidev.json.JSONArray;

/**
 * Created by chaopeng on 16/6/2016.
 */
public class Action {
    public enum ActionType {
        INSERT, UPDATE, DELETE, OTHERS
    }

    private ActionType actionType = null;

    private String table = null;

    private JSONArray data;

    public Action(String table, ActionType actionType, JSONArray data) {
        this.table = table;
        this.actionType = actionType;
        this.data = data;
    }

    public Action() {
        this(null, null, null);
    }

    public Action(Action action) {
        this(action.getTable(), action.getActionType(), action.getData());
    }

    public boolean isValid() {
        if (actionType == null || table == null || data == null) {
            return false;
        }
        return true;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public void setActionType(ActionType actionType) {
        this.actionType = actionType;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public JSONArray getData() {
        return data;
    }

    public void setData(JSONArray data) {
        this.data = data;
    }
}
