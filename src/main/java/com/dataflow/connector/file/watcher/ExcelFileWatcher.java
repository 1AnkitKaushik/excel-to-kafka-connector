package com.dataflow.connector.watcher;

import com.dataflow.connector.file.model.RowMessage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ExcelFileWatcher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    
    // In-memory state store to track previous row states: Map<"filename:rowIndex", "RowContentHash">
    private final Map<String, String> fileStateStore = new ConcurrentHashMap<>();

    public ExcelFileWatcher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Parses the Excel file and streams only ADDED or UPDATED rows to Kafka.
     * Calculates delta stats (Added, Updated, Unchanged).
     */
    public void processFile(Path filePath) {
        String filename = filePath.getFileName().toString();
        
        int addedCount = 0;
        int updatedCount = 0;
        int unchangedCount = 0;

        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            String sheetName = sheet.getSheetName();
            Row headerRow = sheet.getRow(0);

            if (headerRow == null) {
                System.out.println("⚠️ Warning: Empty or headerless sheet in file: " + filename);
                return;
            }

            // Extract dynamic headers from Row 0
            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(cell.getStringCellValue().trim());
            }

            // Iterate data rows (Row index 1 onwards)
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                Map<String, String> rowData = new LinkedHashMap<>();
                for (int j = 0; j < headers.size(); j++) {
                    Cell cell = row.getCell(j, Row.MissingCellPolicy.CREATE_NULL_AS_EMPTY);
                    rowData.put(headers.get(j), cell.toString().trim());
                }

                String rowKey = filename + ":" + i;
                String currentHash = Integer.toHexString(rowData.hashCode());
                String previousHash = fileStateStore.get(rowKey);

                String changeType;
                if (previousHash == null) {
                    changeType = "ADDED";
                    addedCount++;
                } else if (!previousHash.equals(currentHash)) {
                    changeType = "UPDATED";
                    updatedCount++;
                } else {
                    changeType = "UNCHANGED";
                    unchangedCount++;
                }

                // Update row hash in state memory
                fileStateStore.put(rowKey, currentHash);

                // Publish to Kafka only if the row is ADDED or UPDATED
                if (!changeType.equals("UNCHANGED")) {
                    RowMessage message = new RowMessage(
                        filename,
                        sheetName,
                        i,
                        rowData,
                        changeType,
                        System.currentTimeMillis()
                    );
                    
                    // Route serialized JSON to Kafka topic using composite key for partition ordering
                    kafkaTemplate.send("excel-file-updates", rowKey, message.toJson());
                }
            }

            // Output execution summary to console
            System.out.println(String.format(
                "📊 [%s] Delta Summary: %d Added, %d Updated, %d Unchanged", 
                filename, addedCount, updatedCount, unchangedCount
            ));

        } catch (Exception e) {
            System.err.println("❌ Error processing file " + filename + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
