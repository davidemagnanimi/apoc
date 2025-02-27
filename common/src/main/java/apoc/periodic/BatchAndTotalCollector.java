/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.periodic;

import apoc.util.Util;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.procedure.TerminationGuard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BatchAndTotalCollector {
    private final int failedParams;
    private long start = System.nanoTime();
    private AtomicLong batches = new AtomicLong();
    private long successes = 0;
    private AtomicLong count = new AtomicLong();
    private AtomicLong failedOps = new AtomicLong();
    private AtomicLong retried = new AtomicLong();
    private Map<String, Long> operationErrors = new ConcurrentHashMap<>();
    private AtomicInteger failedBatches = new AtomicInteger();
    private Map<String, Long> batchErrors = new HashMap<>();
    private Map<String, List<Map<String, Object>>> failedParamsMap = new ConcurrentHashMap<>();
    private final boolean wasTerminated;

    private AtomicLong nodesCreated = new AtomicLong();
    private AtomicLong nodesDeleted = new AtomicLong();
    private AtomicLong relationshipsCreated = new AtomicLong();
    private AtomicLong relationshipsDeleted = new AtomicLong();
    private AtomicLong propertiesSet = new AtomicLong();
    private AtomicLong labelsAdded = new AtomicLong();
    private AtomicLong labelsRemoved = new AtomicLong();

    public BatchAndTotalCollector(TerminationGuard terminationGuard, int failedParams) {
        this.failedParams = failedParams;
        wasTerminated = Util.transactionIsTerminated(terminationGuard);
    }

    public BatchAndTotalResult getResult() {
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        Map<String, Long> updateStatistics = new HashMap<>();
        updateStatistics.put("nodesCreated", nodesCreated.get());
        updateStatistics.put("nodesDeleted", nodesDeleted.get());
        updateStatistics.put("relationshipsCreated", relationshipsCreated.get());
        updateStatistics.put("relationshipsDeleted", relationshipsDeleted.get());
        updateStatistics.put("propertiesSet", propertiesSet.get());
        updateStatistics.put("labelsAdded", labelsAdded.get());
        updateStatistics.put("labelsRemoved", labelsRemoved.get());

        return new BatchAndTotalResult(batches.get(), count.get(), timeTaken, successes, failedOps.get(),
                failedBatches.get(), retried.get(), operationErrors, batchErrors, wasTerminated,
                failedParamsMap, updateStatistics);
    }

    public long getCount() {
        return count.get();
    }

    public void incrementFailedOps(long size) {
        failedOps.addAndGet(size);
    }

    public void incrementBatches() {
        batches.incrementAndGet();
    }

    public void incrementSuccesses(long increment) {
        successes += increment;
    }

    public void incrementCount(long currentBatchSize) {
        count.addAndGet(currentBatchSize);
    }

    public Map<String, Long> getBatchErrors() {
        return batchErrors;
    }

    public Map<String, Long> getOperationErrors() {
        return operationErrors;
    }

    public void amendFailedParamsMap(List<Map<String, Object>> batch) {
        if (failedParams >= 0) {
            failedParamsMap.put(
                    Long.toString(batches.get()),
                    new ArrayList<>(batch.subList(0, Math.min(failedParams + 1, batch.size())))
            );
        }
    }

    public AtomicInteger getFailedBatches() {
        return failedBatches;
    }

    public void incrementRetried() {
        retried.incrementAndGet();
    }

    public void updateStatistics(QueryStatistics stats) {
        nodesCreated.addAndGet(stats.getNodesCreated());
        nodesDeleted.addAndGet(stats.getNodesDeleted());
        relationshipsCreated.addAndGet(stats.getRelationshipsCreated());
        relationshipsDeleted.addAndGet(stats.getRelationshipsDeleted());
        propertiesSet.addAndGet(stats.getPropertiesSet());
        labelsAdded.addAndGet(stats.getLabelsAdded());
        labelsRemoved.addAndGet(stats.getLabelsRemoved());
    }
}
