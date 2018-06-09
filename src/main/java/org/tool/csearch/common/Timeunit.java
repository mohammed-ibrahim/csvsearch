package org.tool.csearch.common;

import org.apache.commons.lang3.time.DurationFormatUtils;

import lombok.Getter;

public class Timeunit {

    @Getter
    private long duration;

    public Timeunit(long durationMs) {
        this.duration = durationMs;
    }

    @Override
    public String toString() {

        return DurationFormatUtils.formatDuration(this.duration, "HH:mm:ss:SS");
    }
}
