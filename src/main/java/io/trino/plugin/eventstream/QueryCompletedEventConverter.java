/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.eventstream;

import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.RoutineInfo;

import java.util.stream.Collectors;

class QueryCompletedEventConverter {

    static QueryCompletedEventV1 convert(QueryCompletedEvent queryCompletedEvent) {

        QueryCompletedEventV1.Builder queryCompleted = QueryCompletedEventV1.newBuilder()
                .setCreateTime(queryCompletedEvent.getCreateTime().toString())
                .setUser(queryCompletedEvent.getContext().getUser())
                .setEnvironment(queryCompletedEvent.getContext().getEnvironment())
                .setQueryType(
                        queryCompletedEvent.getContext().getQueryType().isPresent() ?
                                queryCompletedEvent.getContext().getQueryType().get().toString() : null
                )
                .setQuery(queryCompletedEvent.getMetadata().getQuery())
                .setQueryID(queryCompletedEvent.getMetadata().getQueryId())
                .setPrincipal(queryCompletedEvent.getContext().getPrincipal().orElse(null))
                .setUserAgent(queryCompletedEvent.getContext().getUserAgent().orElse(null))
                .setRemoteClientAddress(queryCompletedEvent.getContext().getRemoteClientAddress().orElse(null))
                .setClientInfo(queryCompletedEvent.getContext().getClientInfo().orElse(null))
                .setSource(queryCompletedEvent.getContext().getSource().orElse(null))
                .setCatalog(queryCompletedEvent.getContext().getCatalog().orElse(null))
                .setSchema$(queryCompletedEvent.getContext().getSchema().orElse(null))
                .setEstimatedExecutionTime(
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ?
                                queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toString() : null
                )
                .setEstimatedCpuTime(
                        queryCompletedEvent.getContext().getResourceEstimates().getCpuTime().isPresent() ?
                                queryCompletedEvent.getContext().getResourceEstimates().getCpuTime().get().toString() : null
                )
                .setEstimatedPeakMemory(
                        queryCompletedEvent.getContext().getResourceEstimates().getPeakMemoryBytes().isPresent() ?
                                queryCompletedEvent.getContext().getResourceEstimates().getPeakMemoryBytes().get().toString() : null
                )
                .setTransactionId(queryCompletedEvent.getMetadata().getTransactionId().orElse(null))
                .setUpdateType(queryCompletedEvent.getMetadata().getUpdateType().orElse(null))
                .setPreparedQuery(queryCompletedEvent.getMetadata().getPreparedQuery().orElse(null))
                .setQueryState(queryCompletedEvent.getMetadata().getQueryState())
                .setTables(
                        queryCompletedEvent.getMetadata().getTables().stream()
                                .map(tableInfo -> String.join(".", tableInfo.getCatalog(), tableInfo.getSchema(), tableInfo.getTable()))
                                .collect(Collectors.toList())
                )
                .setRoutines(
                        queryCompletedEvent.getMetadata().getRoutines().stream()
                                .map(RoutineInfo::getRoutine)
                                .collect(Collectors.toList())
                )
                .setQueryStartTime(queryCompletedEvent.getCreateTime().toString())
                .setQueryEndTime(queryCompletedEvent.getEndTime().toString())
                .setCpuTime(queryCompletedEvent.getStatistics().getCpuTime().toString())
                .setWallTime(queryCompletedEvent.getStatistics().getWallTime().toString())
                .setQueuedTime(queryCompletedEvent.getStatistics().getQueuedTime().toString())
                .setTotalBytes(queryCompletedEvent.getStatistics().getTotalBytes())
                .setTotalRows(queryCompletedEvent.getStatistics().getTotalRows())
                .setOutputBytes(queryCompletedEvent.getStatistics().getOutputBytes())
                .setOutputRows(queryCompletedEvent.getStatistics().getOutputRows())
                .setWrittenBytes(queryCompletedEvent.getStatistics().getWrittenBytes())
                .setWrittenRows(queryCompletedEvent.getStatistics().getWrittenRows())
                .setCompletedSplits(queryCompletedEvent.getStatistics().getCompletedSplits())
                .setIsFailed(queryCompletedEvent.getFailureInfo().isPresent())
                .setFailureErrorCode(
                        queryCompletedEvent.getFailureInfo().isPresent() ?
                                queryCompletedEvent.getFailureInfo().get().getErrorCode().getCode() : null
                )
                .setWarningCodes(
                        queryCompletedEvent.getWarnings().stream()
                                .map(trinoWarning -> trinoWarning.getWarningCode().getCode())
                                .collect(Collectors.toList())
                );

        return queryCompleted.build();
    }
}
