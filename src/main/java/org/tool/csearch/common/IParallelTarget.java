package org.tool.csearch.common;

public interface IParallelTarget<Q, S> {

    S execute(Q q) throws Exception;

    void onSuccess(Q q, S s);

    void onFailure(Q q, Throwable t);
}
