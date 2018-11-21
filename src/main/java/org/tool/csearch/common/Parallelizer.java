package org.tool.csearch.common;

import java.io.PrintWriter;
import java.io.StringWriter;
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
    private boolean fatalError;

    private String failureMessage;

    private Integer numSuccessfulRecords;

    private Integer numFailedRecords;

    private ExecutorService executorService;

    public Parallelizer(
        IParallelTarget<M, N> target1,
        Iterator<M> iterator1,
        Integer numberOfParallelTasks1,
        Boolean exitOnFailure1) {

        this.target = target1;
        this.iterator = iterator1;
        this.numberOfParallelTasks = numberOfParallelTasks1;
        this.exitIfEvenOneRecordFails = exitOnFailure1;

        this.fatalError = false;
        this.failureMessage = null;
        this.numSuccessfulRecords = 0;
        this.numFailedRecords = 0;
    }

    public ExecutionResponse start() throws Exception {
        this.executorService = Executors.newFixedThreadPool(this.numberOfParallelTasks);

        for (int i = 0; i < this.numberOfParallelTasks; i++) {
            this.executorService.submit(new TaskHandler());
        }

        this.executorService.shutdown();
        this.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);

        return new ExecutionResponse(!this.fatalError, this.failureMessage, this.numSuccessfulRecords, this.numFailedRecords);
    }

    synchronized M getNextRecord() {

        if (this.fatalError) {

            return null;
        }

        if (this.numFailedRecords > 0 && this.exitIfEvenOneRecordFails) {

            return null;
        }


        boolean hasNext = false;

        try {

            hasNext = this.iterator.hasNext();
        } catch (Exception e) {

            String message = String.format("%s.hasNext() has failed with error: %s", this.iterator.getClass(), exceptionToString(e));
            this.onFatalError(message);
        }

        if (hasNext) {

            try {

                return this.iterator.next();
            } catch (Exception e) {

                String message = String.format("%s.next() has failed with error: %s", this.iterator.getClass(), exceptionToString(e));
                this.onFatalError(message);
            }

        }

        return null;
    }

    synchronized void submitProcessedRecord(M m, N n) {
        try {

            this.target.onSuccess(m, n);
            this.numSuccessfulRecords++;

        } catch (Exception e) {

            String message = String.format("%s.onSuccess(...) has failed with error: %s", this.target.getClass(), exceptionToString(e));
            this.onFatalError(message);
        }
    }

    synchronized void submitFailedRecord(M m, Throwable t) {
        try {

            this.numFailedRecords++;
            this.target.onFailure(m, t);

        } catch (Exception e) {

            String message = String.format("%s.onFailure(...) has failed with error: %s", this.target.getClass(), exceptionToString(e));
            this.onFatalError(message);
        }
    }

    synchronized void onFatalError(String message) {

        this.fatalError = true;
        this.failureMessage = message;

    }

    private N executeRecord(M m) throws Exception {

        return this.target.execute(m);
    }

    private String exceptionToString(Exception exception) {

        StringWriter stringWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stringWriter));

        return stringWriter.toString();
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
