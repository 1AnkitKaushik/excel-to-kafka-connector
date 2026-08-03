package com.dataflow.connector.file.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class RowMessage {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String fileName;
    private String sheetName;
    private int rowIndex;
    private Map<String, String> data;
    private String changeType; // "ADDED", "UPDATED", or "UNCHANGED"
    private long timestamp;

    public RowMessage() {}

    public RowMessage(String fileName, String sheetName, int rowIndex, Map<String, String> data, String changeType, long timestamp) {
        this.fileName = fileName;
        this.sheetName = sheetName;
        this.rowIndex = rowIndex;
        this.data = data;
        this.changeType = changeType;
        this.timestamp = timestamp;
    }

    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }

    public String getSheetName() { return sheetName; }
    public void setSheetName(String sheetName) { this.sheetName = sheetName; }

    public int getRowIndex() { return rowIndex; }
    public void setRowIndex(int rowIndex) { this.rowIndex = rowIndex; }

    public Map<String, String> getData() { return data; }
    public void setData(Map<String, String> data) { this.data = data; }

    public String getChangeType() { return changeType; }
    public void setChangeType(String changeType) { this.changeType = changeType; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    // Guarantees safe and valid JSON output
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting RowMessage to JSON", e);
        }
    }
}
//Added changeType (ADDED, UPDATED, UNCHANGED) so consumers receiving the JSON payload know the delta operation type.

//Added timestamp for auditing when the file modification was caught and streamed.