package org.tool.csearch.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;

@Data
public class AppConfig {

    private Path inputCsvFilePath;

    private String luceneQuery;

    private String columnExpression;

    private String formatterName;

    private int limit;

    public AppConfig(String[] args) {
        if (args.length < 2) {

            //usage: java -jar cs.jar inputcsv.csv lucene_query [list_of_columns] [table|vtable|simple]
            throw new RuntimeException("usage: java -jar cs.jar inputcsv.csv lucene_query [list_of_columns] [table|vtable|simple] [limit]");
        }

        this.inputCsvFilePath = Paths.get(args[0]);
        this.luceneQuery = args[1];

        //Column Expr
        this.columnExpression = (args.length > 2 ? args[2] : null);

        //Formatter
        this.formatterName = "simple";
        if (args.length > 3) {
            List<String> allowedFormatters = Arrays.asList("table", "vtable", "simple");

            String requestedFormat = args[3];
            if (!allowedFormatters.contains(requestedFormat)) {

                throw new RuntimeException("Only allowed formats are: " + StringUtils.join(allowedFormatters));
            }

            this.formatterName = requestedFormat;
        }

        //Limit
        if (args.length > 4) {

            Double dval = Double.parseDouble(args[4]);
            this.limit = dval.intValue();
        } else {

            this.limit = Constants.DEFAULT_RESULT_LIMIT;
        }
    }
}
