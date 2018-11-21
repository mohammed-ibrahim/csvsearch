package org.tool.csearch.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ExecutionResponse {

    private Boolean successful;

    private String failureReason;

    private Integer numSuccessfulRecords;

    private Integer numFailedRecords;
}
