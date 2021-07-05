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

import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.RoutineInfo;

import java.util.stream.Collectors;

class QueryCreatedEventConverter {

    static QueryCreatedEventV1 convert(QueryCreatedEvent queryCreatedEvent) {

        QueryCreatedEventV1.Builder queryCreated = QueryCreatedEventV1.newBuilder()
                .setCreateTime(queryCreatedEvent.getCreateTime().toString())
                .setUser(queryCreatedEvent.getContext().getUser())
                .setEnvironment(queryCreatedEvent.getContext().getEnvironment())
                .setQueryType(
                        queryCreatedEvent.getContext().getQueryType().isPresent() ?
                                queryCreatedEvent.getContext().getQueryType().get().toString() : null
                )
                .setQuery(queryCreatedEvent.getMetadata().getQuery())
                .setQueryID(queryCreatedEvent.getMetadata().getQueryId())
                .setPrincipal(queryCreatedEvent.getContext().getPrincipal().orElse(null))
                .setUserAgent(queryCreatedEvent.getContext().getUserAgent().orElse(null))
                .setRemoteClientAddress(queryCreatedEvent.getContext().getRemoteClientAddress().orElse(null))
                .setClientInfo(queryCreatedEvent.getContext().getClientInfo().orElse(null))
                .setSource(queryCreatedEvent.getContext().getSource().orElse(null))
                .setCatalog(queryCreatedEvent.getContext().getCatalog().orElse(null))
                .setSchema$(queryCreatedEvent.getContext().getSchema().orElse(null))
                .setEstimatedExecutionTime(
                        queryCreatedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ?
                                queryCreatedEvent.getContext().getResourceEstimates().getExecutionTime().get().toString() : null
                )
                .setEstimatedCpuTime(
                        queryCreatedEvent.getContext().getResourceEstimates().getCpuTime().isPresent() ?
                                queryCreatedEvent.getContext().getResourceEstimates().getCpuTime().get().toString() : null
                )
                .setEstimatedPeakMemory(
                        queryCreatedEvent.getContext().getResourceEstimates().getPeakMemoryBytes().isPresent() ?
                                queryCreatedEvent.getContext().getResourceEstimates().getPeakMemoryBytes().get().toString() : null
                )
                .setTransactionId(queryCreatedEvent.getMetadata().getTransactionId().orElse(null))
                .setUpdateType(queryCreatedEvent.getMetadata().getUpdateType().orElse(null))
                .setPreparedQuery(queryCreatedEvent.getMetadata().getPreparedQuery().orElse(null))
                .setQueryState(queryCreatedEvent.getMetadata().getQueryState())
                .setTables(
                        queryCreatedEvent.getMetadata().getTables().stream()
                                .map(tableInfo -> String.join(".", tableInfo.getCatalog(), tableInfo.getSchema(), tableInfo.getTable()))
                                .collect(Collectors.toList())
                )
                .setRoutines(
                        queryCreatedEvent.getMetadata().getRoutines().stream()
                                .map(RoutineInfo::getRoutine)
                                .collect(Collectors.toList())
                );

        return queryCreated.build();
    }
}
