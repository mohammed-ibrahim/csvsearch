package org.tool.csearch.search;

import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.tool.csearch.common.Timer;
import org.tool.csearch.formatter.IFormatter;
import org.tool.csearch.log.Logger;

public class IndexSearchDelegator {

    public void search(String luceneQuery,
            Path path,
            IFormatter formatter,
            List<String> headers,
            int limit) throws Exception {

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(path))) {
            IndexSearcher searcher = new IndexSearcher(reader);

            Query query = new QueryParser(headers.get(0), new StandardAnalyzer()).parse(luceneQuery);
            Logger.debug(query.toString());
            Timer searchTimer = new Timer();
            TopDocs topDocs = searcher.search(query, limit);
            Logger.debug(String.format("Time taken to perform query: %s", searchTimer.end().toString()));

            Timer fetchTimer = new Timer();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {

                formatter.printDoc(reader.document(scoreDoc.doc), headers);
            }

            Logger.debug(String.format("Time taken to perform fetch: %s", fetchTimer.end().toString()));
        }
    }
}
