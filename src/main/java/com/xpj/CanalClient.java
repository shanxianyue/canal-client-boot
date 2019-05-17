package com.xpj;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CanalClient extends AbstractCanalClient {
    @Override
    protected void handle(Message message){

        List<CanalEntry.Entry> entries = message.getEntries();
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();

                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    continue;
                }
                String tableName = entry.getHeader().getTableName();
                String dataBaseName = entry.getHeader().getSchemaName();
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.UPDATE) {
                        JSONObject beforeJson = handleColums(rowData.getBeforeColumnsList());
                        JSONObject afterJson = handleColums(rowData.getAfterColumnsList());
                        System.out.println(dataBaseName + "." + tableName + ": update before is " + beforeJson + ", update after is " + afterJson);
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        JSONObject afterJson = handleColums(rowData.getAfterColumnsList());
                        System.out.println(dataBaseName + "." + tableName + ": insert is " + afterJson);
                    }
                }
            }
        }
    }


    public JSONObject handleColums(List<CanalEntry.Column> columns){
        if (columns == null || columns.isEmpty()){
            throw new RuntimeException();
        }
        JSONObject json = new JSONObject();
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        return json;
    }
}
