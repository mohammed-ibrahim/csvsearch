package org.tool.csearch.boot;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Communicator {

    public void start(Path path, List<String> headers) throws Exception {

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(path))) {

            IndexSearcher searcher = new IndexSearcher(reader);

            String cmd;
            while (!((cmd = next()).toLowerCase().equals("exit"))) {

                try {

                    Query query = new QueryParser("title", new StandardAnalyzer()).parse(cmd);
                    TopDocs topDocs = searcher.search(query, 100);

                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

                        List<String> values = new ArrayList<String>();

                        for (String header : headers) {
                            IndexableField field = reader.document(scoreDoc.doc).getField(header);

                            if (field != null) {
                                values.add(header + ":" + field.stringValue());
                            }

                        }

                        log.info(StringUtils.join(values, ","));

                    }

                } catch (ParseException e) {

                    log.info("Invalid query");
                    e.printStackTrace();
                }
            }
        }
    }

    private String next() {
        Scanner sc = new Scanner(System.in);
        return sc.nextLine();
    }
}
