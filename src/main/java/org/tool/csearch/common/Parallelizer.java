package org.tool.csearch.common;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.Data;

@Data
public class Parallelizer<M, N> {

    //Following args to be taken as input.
    private IParallelTarget<M, N> target;

    private Iterator<M> iterator;

    private Integer numberOfParallelTasks;

    private Boolean exitIfEvenOneRecordFails;

    //Following args to be used internally.
    private boolean hasFailed;

    private ExecutorService executorService;

    private Integer numFailedRecords;

    public Parallelizer(
        IParallelTarget<M, N> target1,
        Iterator<M> iterator1,
        Integer numberOfParallelTasks1,
        Boolean exitOnFailure1) {

        this.target = target1;
        this.iterator = iterator1;
        this.numberOfParallelTasks = numberOfParallelTasks1;
        this.exitIfEvenOneRecordFails = exitOnFailure1;

        this.hasFailed = false;
        this.numFailedRecords = 0;
    }

    public void start() throws Exception {
        this.executorService = Executors.newFixedThreadPool(this.numberOfParallelTasks);

        for (int i = 0; i < this.numberOfParallelTasks; i++) {
            this.executorService.submit(new TaskHandler());
        }

        this.executorService.shutdown();
        this.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    }

    synchronized M getNextRecord() {

        if (this.hasFailed) {

            return null;
        }

        if (this.numFailedRecords > 0 && this.exitIfEvenOneRecordFails) {

            return null;
        }

        if (this.iterator.hasNext()) {

            try {

                return this.iterator.next();
            } catch (Exception e) {

                this.onFailure();
            }

        }

        return null;
    }

    synchronized void submitProcessedRecord(M m, N n) {
        try {

            this.target.onSuccess(m, n);

        } catch (Exception e) {

            this.onFailure();
        }
    }

    synchronized void submitFailedRecord(M m, Throwable t) {
        try {

            this.numFailedRecords++;
            this.target.onFailure(m, t);
        } catch (Exception e) {

            this.onFailure();
        }
    }

    synchronized void onFailure() {

        this.hasFailed = true;
    }

    private N executeRecord(M m) throws Exception {
        return this.target.execute(m);
    }

    private class TaskHandler implements Runnable {

        @Override
        public void run() {

            M m;

            while ((m = getNextRecord()) != null) {
                try {

                    N n = executeRecord(m);
                    submitProcessedRecord(m, n);

                } catch (Exception e) {

                    submitFailedRecord(m, e);
                }
            }
        }
    }
}
