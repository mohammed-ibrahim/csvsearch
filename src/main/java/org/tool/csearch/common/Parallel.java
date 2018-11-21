package org.tool.csearch.common;

import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Parallel implements IParallelTarget<Integer, String> {

    public static void main(String[] args) throws Exception {

        Parallel p = new Parallel();
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        Parallelizer<Integer, String> pz  = new Parallelizer<Integer, String>(p, input.iterator(), 4, false);
        pz.start();
        log.info("<<<<<<<<<<<<Completed>>>>>>>>>>>>");
    }

    @Override
    public String execute(Integer q) throws Exception {
        log.info("In progress: {}", q);
        if (q % 2 == 0) {
            throw new RuntimeException("even message");
        }
        Thread.currentThread().sleep(3000);
        return q.toString();
    }

    @Override
    public void onSuccess(Integer q, String s) {

        log.info("Completed: {} ::: {}", q, s);
    }

    @Override
    public void onFailure(Integer q, Throwable t) {

        log.info("Failed: {} ::: {}", q, t);

    }
}
