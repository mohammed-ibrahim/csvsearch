package org.tool.csearch.formatter;

import java.util.List;

import org.apache.lucene.document.Document;

public interface IFormatter {

    void printDoc(Document document, List<String> headers);

    void commit();
}
