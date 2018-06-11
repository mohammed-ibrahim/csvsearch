package org.tool.csearch.formatter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.tool.csearch.log.Logger;

public class TableFormatter implements IFormatter {

    @Override
    public void printDoc(Document document, List<String> headers) {

        List<String> values = new ArrayList<String>();

        for (String header : headers) {
            IndexableField field = document.getField(header);

            if (field != null) {
                values.add(field.stringValue());
            } else {
                values.add("MISSING");
            }
        }

        Logger.info(StringUtils.join(values, ","));
    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

}
