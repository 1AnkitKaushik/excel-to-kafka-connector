package com.dataflow.connector.file.watcher;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Component
public class DirectoryWatcher {

    private final ExcelFileWatcher excelFileWatcher;
    // Map<FileName, LastModifiedTimestamp>
    private final Map<String, Long> lastModifiedMap = new HashMap<>();

    public DirectoryWatcher(ExcelFileWatcher excelFileWatcher) {
        this.excelFileWatcher = excelFileWatcher;
    }

    // Checks the directory every 3 seconds
    @Scheduled(fixedDelay = 3000)
    public void pollDirectory() {
        File folder = new File("data/input");
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".xlsx") && !name.startsWith("~$"));

        if (files == null) return;

        for (File file : files) {
            String fileName = file.getName();
            long lastModified = file.lastModified();

            Long previousModified = lastModifiedMap.get(fileName);

            // If file is new or modified timestamp changed, process it
            if (previousModified == null || previousModified != lastModified) {
                lastModifiedMap.put(fileName, lastModified);
                
                Path filePath = Paths.get(file.getAbsolutePath());
                excelFileWatcher.processFile(filePath);
            }
        }
    }
}