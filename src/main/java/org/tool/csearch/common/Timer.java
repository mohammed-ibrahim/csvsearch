package org.tool.csearch.common;

import java.util.Date;

public class Timer {

    private Date startAt;

    public Timer() {

        this.startAt = new Date();
    }

    public Timeunit end() {

        return new Timeunit((new Date().getTime()) - startAt.getTime());
    }
}
