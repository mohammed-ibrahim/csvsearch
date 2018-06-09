package org.tool.csearch.boot;

import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.tool.csearch.common.AppConfig;
import org.tool.csearch.common.CsvColumnAnalyzer;
import org.tool.csearch.common.Timer;
import org.tool.csearch.factory.FormatterFactory;
import org.tool.csearch.formatter.IFormatter;
import org.tool.csearch.indexer.Indexer;
import org.tool.csearch.search.IndexSearchDelegator;

import au.com.bytecode.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvSearchMain {

    public static void main(String[] argsv) throws Exception {

        AppConfig appConfig = new AppConfig(argsv);
        /*
         * TODO:
         * 1. Fix logging.
         */

        CsvColumnAnalyzer csvColumnAnalyzer = new CsvColumnAnalyzer();
        csvColumnAnalyzer.compile(appConfig.getInputCsvFilePath(), appConfig.getColumnExpression());

        Timer srcMgrTimer = new Timer();
        CsvSourceManager csvSourceManager = new CsvSourceManager(appConfig.getInputCsvFilePath());
        log.info("Timer taken to index/validate: {}", srcMgrTimer.end().toString());

        if (csvSourceManager.getIndexingRequired()) {

            log.info("Creating new index...");
            new Indexer(appConfig.getInputCsvFilePath(), csvSourceManager.getDropPath());
        } else {

            log.info("Reusing existing index: {}", csvSourceManager.getDropPath());
        }

        IFormatter formatter = new FormatterFactory().getFormatter(appConfig.getFormatterName());
        IndexSearchDelegator searchDelegator = new IndexSearchDelegator();
        searchDelegator.search(appConfig.getLuceneQuery(),
                csvSourceManager.getDropPath(),
                formatter,
                csvColumnAnalyzer.getSelectedColumnNames(),
                appConfig.getLimit());
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
