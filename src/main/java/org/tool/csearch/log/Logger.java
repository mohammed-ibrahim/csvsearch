package org.tool.csearch.log;

public class Logger {

    public static boolean debug = true;

    public static void log(String string) {
        System.out.println(string);
    }

    public static void debug(String string) {

        if (debug) {
            log(string);
        }
    }
}
