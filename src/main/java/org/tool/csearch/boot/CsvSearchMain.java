package org.tool.csearch.boot;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.tool.csearch.common.AppConfig;
import org.tool.csearch.common.CsvColumnAnalyzer;
import org.tool.csearch.common.Timer;
import org.tool.csearch.factory.FormatterFactory;
import org.tool.csearch.formatter.IFormatter;
import org.tool.csearch.indexer.Indexer;
import org.tool.csearch.log.Logger;
import org.tool.csearch.search.IndexSearchDelegator;

import au.com.bytecode.opencsv.CSVReader;

public class CsvSearchMain {

    public static void main(String[] argsv) throws Exception {

        //TODO:
        //2. filter necessary logging.
        //3. Summary details: num rows/total rows/time taken.. -1\n +1\n

        Timer opTimer = new Timer();
        AppConfig appConfig = new AppConfig(argsv);
        Logger.debug = appConfig.getDebug();

        CsvColumnAnalyzer csvColumnAnalyzer = new CsvColumnAnalyzer();
        csvColumnAnalyzer.compile(appConfig.getInputCsvFilePath(), appConfig.getColumnExpression());

        Timer srcMgrTimer = new Timer();
        CsvSourceManager csvSourceManager = new CsvSourceManager(appConfig.getInputCsvFilePath());
        Logger.info(String.format("Timer taken to index/validate: %s", srcMgrTimer.end().toString()));

        if (csvSourceManager.getIndexingRequired()) {

            Logger.info("Creating new index...");
            new Indexer(appConfig.getInputCsvFilePath(), csvSourceManager.getDropPath());
        } else {

            Logger.info(String.format("Reusing existing index: %s", csvSourceManager.getDropPath()));
        }

        if (csvColumnAnalyzer.getSelectedColumnNames() == null) {
            throw new RuntimeException("IS NULL");
        }
        IFormatter formatter = new FormatterFactory().getFormatter(appConfig.getFormatterName());
        IndexSearchDelegator searchDelegator = new IndexSearchDelegator();
        Integer totalHits = searchDelegator.search(appConfig.getLuceneQuery(),
                csvSourceManager.getDropPath(),
                formatter,
                csvColumnAnalyzer.getSelectedColumnNames(),
                appConfig.getLimit());

        Logger.info(String.format("\nTotal Hits: %d Limit: %d: Time taken: %s", totalHits, appConfig.getLimit(), opTimer.end().toString()));
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
