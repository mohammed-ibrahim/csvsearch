package org.tool.csearch.boot;

import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvSearchMain {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {

            throw new RuntimeException("Needs 1 parameter to run..");
        }

        Path inputCsvFilePath = Paths.get(args[0]);
        CsvSourceManager csvSourceManager = new CsvSourceManager(inputCsvFilePath);
        List<String> headers = getColumnNamesOfCsv(inputCsvFilePath);

        if (csvSourceManager.getIndexingRequired()) {

            log.info("Creating new index...");
            new Indexer(inputCsvFilePath, csvSourceManager.getDropPath());
        } else {

            log.info("Reusing existing index...");
        }

        new Communicator().start(csvSourceManager.getDropPath(), headers);
    }

    public static List<String> getColumnNamesOfCsv(Path path) {

        List<String> columnNames = null;

        try (CSVReader reader = new CSVReader(new FileReader(path.toFile()))) {

            columnNames = Arrays.asList(reader.readNext());

        } catch (Exception e) {

            throw new RuntimeException(e);
        }

        return columnNames;
    }
}
