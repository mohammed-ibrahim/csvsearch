package org.tool.csearch.boot;

import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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

    public static void main(String[] args) throws Exception {

        /*
         * TODO:
         * 1. Timer
         * 2. Limit in input
         * 3. Package refactor - done
         */
        if (args.length < 2) {

            //usage: java -jar cs.jar inputcsv.csv lucene_query [list_of_columns] [table|vtable|simple]
            throw new RuntimeException("usage: java -jar cs.jar inputcsv.csv lucene_query [list_of_columns] [table|vtable|simple]");
        }

        Path inputCsvFilePath = Paths.get(args[0]);
        String columnExpression = (args.length > 2 ? args[2] : null);
        CsvColumnAnalyzer csvColumnAnalyzer = new CsvColumnAnalyzer();
        csvColumnAnalyzer.compile(inputCsvFilePath, columnExpression);

        String formatterName = "simple";

        if (args.length > 3) {
            List<String> allowedFormatters = Arrays.asList("table", "vtable", "simple");

            String requestedFormat = args[3];
            if (!allowedFormatters.contains(requestedFormat)) {

                throw new RuntimeException("Only allowed formats are: " + StringUtils.join(allowedFormatters));
            }

            formatterName = requestedFormat;
        }

        Timer srcMgrTimer = new Timer();
        CsvSourceManager csvSourceManager = new CsvSourceManager(inputCsvFilePath);
        log.info("Timer taken to index/validate: {}", srcMgrTimer.end().toString());

        if (csvSourceManager.getIndexingRequired()) {

            log.info("Creating new index...");
            new Indexer(inputCsvFilePath, csvSourceManager.getDropPath());
        } else {

            log.info("Reusing existing index: {}", csvSourceManager.getDropPath());
        }

        IFormatter formatter = new FormatterFactory().getFormatter(formatterName);
        IndexSearchDelegator searchDelegator = new IndexSearchDelegator();
        searchDelegator.search(args[1], csvSourceManager.getDropPath(), formatter, csvColumnAnalyzer.getSelectedColumnNames());
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
