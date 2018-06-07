package org.tool.csearch.boot;

import java.nio.file.Paths;

public class CsvSearchMain {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {

            throw new RuntimeException("Needs 1 parameter to run..");
        }

        new Indexer().index(Paths.get(args[0]));
    }
}
