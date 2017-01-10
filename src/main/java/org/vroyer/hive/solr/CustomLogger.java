package org.vroyer.hive.solr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class CustomLogger {
    private static final AtomicLong id = new AtomicLong(System.currentTimeMillis());

    private static final File logFile = new File("/dmp/log/hive-solr-" + id.incrementAndGet() + ".log");

    private static final BufferedWriter bw;
    static {
        try {
            logFile.setExecutable(true, false);
            logFile.setReadable(true, false);
            logFile.setWritable(true, false);
            bw = new BufferedWriter(new FileWriter(logFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void info(String message) {
        try {
            bw.write(new Date() + ": " + message);
            bw.write('\n');
            bw.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
