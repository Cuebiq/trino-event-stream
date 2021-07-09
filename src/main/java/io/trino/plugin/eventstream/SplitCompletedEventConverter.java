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

import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitFailureInfo;

class SplitCompletedEventConverter {

    static SplitCompletedEventV1 convert(SplitCompletedEvent splitCompletedEvent) {

        SplitCompletedEventV1.Builder splitCompleted = SplitCompletedEventV1.newBuilder()
                .setQueryID(splitCompletedEvent.getQueryId())
                .setStageID(splitCompletedEvent.getStageId())
                .setTaskID(splitCompletedEvent.getTaskId())
                .setSplitStartTime(splitCompletedEvent.getStartTime().toString())
                .setSplitEndTime(splitCompletedEvent.getEndTime().toString())
                .setCreateTime(splitCompletedEvent.getCreateTime().toString())
                .setCatalog(splitCompletedEvent.getCatalogName().orElse(null))
                .setCpuTime(splitCompletedEvent.getStatistics().getCpuTime().toSeconds())
                .setWallTime(splitCompletedEvent.getStatistics().getWallTime().toSeconds())
                .setQueuedTime(splitCompletedEvent.getStatistics().getQueuedTime().toSeconds());

        if (splitCompletedEvent.getFailureInfo().isPresent()) {
            SplitFailureInfo splitFailureInfo = splitCompletedEvent.getFailureInfo().get();
            splitCompleted
                    .setIsFailed(true)
                    .setFailureType(splitFailureInfo.getFailureType())
                    .setFailureMessage(splitFailureInfo.getFailureMessage());
        }
        else {
            splitCompleted.setIsFailed(false);
        }

        return splitCompleted.build();
    }
}
