package com.dataflow.connector.file.watcher;

import com.dataflow.connector.file.reader.ExcelFileReader;
import com.dataflow.connector.file.publisher.KafkaRowPublisher;
import com.dataflow.connector.file.model.RowMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class ExcelFileWatcher {

    private static final Logger logger = LoggerFactory.getLogger(ExcelFileWatcher.class);

    @Value("${file-connector.watch-path:./data/input}")
    private String watchPathStr;

    private final ExcelFileReader fileReader;
    private final KafkaRowPublisher publisher;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    // FIX: Field reference so @PreDestroy can close it explicitly
    private WatchService watchService;

    public ExcelFileWatcher(ExcelFileReader fileReader, KafkaRowPublisher publisher) {
        this.fileReader = fileReader;
        this.publisher = publisher;
    }

    @PostConstruct
    public void startWatching() {
        executor.submit(this::watchDirectory);
    }

    private void watchDirectory() {
        try {
            Path path = Paths.get(watchPathStr);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }

            this.watchService = FileSystems.getDefault().newWatchService();
            path.register(this.watchService, StandardWatchEventKinds.ENTRY_CREATE);

            logger.info("Watching directory for file changes: {}", path.toAbsolutePath());

            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        Path filename = (Path) event.context();
                        File file = path.resolve(filename).toFile();

                        if (file.getName().endsWith(".xlsx") || file.getName().endsWith(".xls")) {
                            logger.info("New Excel file detected: {}", file.getName());
                            List<RowMessage> rows = fileReader.readExcel(file);
                            rows.forEach(publisher::publish);
                        }
                    }
                }
                boolean valid = key.reset();
                if (!valid) break;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Directory watcher thread interrupted.");
        } catch (Exception e) {
            logger.error("Error watching directory: {}", e.getMessage(), e);
        }
    }

    @PreDestroy
    public void stop() {
        logger.info("Stopping ExcelFileWatcher...");
        // FIX: Explicitly close WatchService on shutdown
        try {
            if (watchService != null) {
                watchService.close();
            }
        } catch (Exception e) {
            logger.error("Error closing WatchService: {}", e.getMessage(), e);
        }
        executor.shutdownNow();
    }
}