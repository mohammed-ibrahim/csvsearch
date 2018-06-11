package org.tool.csearch.factory;

import org.tool.csearch.formatter.DetailedFormatter;
import org.tool.csearch.formatter.IFormatter;
import org.tool.csearch.formatter.TableFormatter;

public class FormatterFactory {

    public IFormatter getFormatter(String name) {

        name = name.toLowerCase();

        if ("simple".equals(name)) {
            return new TableFormatter();
        }

        if ("detailed".equals(name)) {
            return new DetailedFormatter();
        }

        throw new RuntimeException("Formatter not configured: " + name);
    }
}
