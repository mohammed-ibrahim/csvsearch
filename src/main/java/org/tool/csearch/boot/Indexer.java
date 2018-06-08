package org.tool.csearch.boot;

import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;

import au.com.bytecode.opencsv.CSVReader;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class Indexer {

    private List<String> headers;

    public Indexer(Path filePath, Path dropPath) throws Exception {
        index(filePath, dropPath);
    }

    private Path index(Path filePath, Path dropPath) throws Exception {

        log.info("Dropping to locations: {}", dropPath);

        FSDirectory indexDir = FSDirectory.open(dropPath);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer())
                .setOpenMode(OpenMode.CREATE_OR_APPEND);

        Integer numDocuments = 0;

        try (CSVReader reader = new CSVReader(new FileReader(filePath.toFile()));
                IndexWriter indexWriter = new IndexWriter(indexDir, indexWriterConfig)) {

            String[] line = reader.readNext();
            this.headers = Arrays.asList(line);
            int numColumns = this.headers.size();

            while ((line = reader.readNext()) != null) {
                Document document = new Document();

                for (int i=0; i<numColumns; i++) {

                    String value = (line[i] == null || line[i].trim() == "" ? "empty" : line[i]);

                    FieldType stringFieldType = new FieldType(StringField.TYPE_STORED);
                    stringFieldType.setOmitNorms(false);
                    Field stringField = new Field(headers.get(i), value, stringFieldType);
                    document.add(stringField);
                }

                indexWriter.addDocument(document);
                numDocuments++;
                
                if (numDocuments > 0 && numDocuments % 500 == 0) {
                    log.info("Added document: {}", numDocuments);
                }
            }

            Path waterMarkFile = Paths.get(dropPath.toString(), Constants.WATER_MARK_FILE);
            Files.write(waterMarkFile, Arrays.asList(Constants.WATER_MARK_FILE_CONTENT), Charset.forName("UTF-8"));

            log.info("Indexed all documents to: {}", dropPath);
            return dropPath;
        }
    }
}
