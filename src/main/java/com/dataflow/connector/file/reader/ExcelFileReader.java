package com.dataflow.connector.file.reader;

import com.dataflow.connector.file.model.RowMessage;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

@Component
public class ExcelFileReader {

    // FIX: Replaced System.err.println with SLF4J Logger
    private static final Logger logger = LoggerFactory.getLogger(ExcelFileReader.class);

    @Value("${file-connector.sheet-index:0}")
    private int sheetIndex;

    public List<RowMessage> readExcel(File file) {
        List<RowMessage> rows = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = WorkbookFactory.create(fis)) {

            Sheet sheet = workbook.getSheetAt(sheetIndex);
            Iterator<Row> rowIterator = sheet.iterator();

            if (!rowIterator.hasNext()) return rows;

            // Header row
            Row headerRow = rowIterator.next();
            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(getCellValueAsString(cell));
            }

            int rowIndex = 1;
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Map<String, String> rowData = new LinkedHashMap<>();

                boolean isRowEmpty = true;
                for (int c = 0; c < headers.size(); c++) {
                    Cell cell = row.getCell(c, MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    String val = getCellValueAsString(cell);
                    if (val != null && !val.trim().isEmpty()) {
                        isRowEmpty = false;
                    }
                    rowData.put(headers.get(c), val);
                }

                // FIX: Skip completely blank rows
                if (!isRowEmpty) {
                    rows.add(new RowMessage(file.getName(), sheet.getSheetName(), rowIndex, rowData));
                }
                rowIndex++;
            }

        } catch (Exception e) {
            logger.error("Error reading Excel file {}: {}", file.getName(), e.getMessage(), e);
        }

        return rows;
    }

    private String getCellValueAsString(Cell cell) {
        DataFormatter formatter = new DataFormatter();
        return formatter.formatCellValue(cell);
    }
}