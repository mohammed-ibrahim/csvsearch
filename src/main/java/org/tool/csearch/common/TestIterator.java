package org.tool.csearch.common;

import java.util.Iterator;

public class TestIterator implements Iterator<Integer> {

    @Override
    public boolean hasNext() {
        
        if (i < 10) {
            i++;
            return true;
        }
        
        throw new RuntimeException("ha ha ha ha...");
    }

    private Integer i = 1;

    @Override
    public Integer next() {

        return 100;
    }

}
