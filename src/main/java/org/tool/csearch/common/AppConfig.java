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

    private Boolean debug;

    public AppConfig(String[] args) {
        if (args.length < 2) {

            //usage: java -jar cs.jar inputcsv.csv lucene_query [list_of_columns] [simple|detailed] [limit] [debug]
            throw new RuntimeException("usage: java -jar cs.jar inputcsv.csv lucene_query [list_of_columns] [simple|detailed] [limit] [true,yes,t,y,1 (i.e. debug logs)]");
        }

        this.inputCsvFilePath = Paths.get(args[0]);
        this.luceneQuery = args[1];

        //Column Expr
        this.columnExpression = (args.length > 2 ? args[2] : null);

        //Formatter
        this.formatterName = "simple";

        if (args.length > 3) {
            List<String> allowedFormatters = Arrays.asList("simple", "detailed");

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

        this.debug = false;
        //Debug
        if (args.length > 5) {

            String value = args[5].toLowerCase();
            List<String> trueValues = Arrays.asList("true", "yes", "t", "y", "1");

            if (trueValues.contains(value)) {

                this.debug = true;
            }
        }
    }
}
