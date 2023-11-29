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
package apoc.trigger;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.MapUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;

public class TriggerHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private enum Phase {before, after, rollback, afterAsync}
    private static final Map<String, Object> TRIGGER_META = Map.of("apoc.trigger", true);

    public static final String TRIGGER_REFRESH = "apoc.trigger.refresh";

    private final ConcurrentHashMap<String, Map<String,Object>> activeTriggers = new ConcurrentHashMap();
    private final ConcurrentHashMap<String, ReactiveTrigger> activeReactiveTriggers = new ConcurrentHashMap();
    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    private final Pools pools;
    private final JobScheduler jobScheduler;

    private long lastUpdate;

    private JobHandle restoreTriggerHandler;

    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled." +
            " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public TriggerHandler(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                          ApocConfig apocConfig, Log log, Pools pools, JobScheduler jobScheduler) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
        this.log = log;
        this.pools = pools;
        this.jobScheduler = jobScheduler;
    }

    private boolean isEnabled() {
        return apocConfig.getBoolean(APOC_TRIGGER_ENABLED);
    }

    public void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    private void updateCache() {
        activeTriggers.clear();

        lastUpdate = System.currentTimeMillis();

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                    SystemPropertyKeys.database.name(), db.databaseName()).forEachRemaining(
                    node -> {
                        activeTriggers.put(
                                (String) node.getProperty(SystemPropertyKeys.name.name()),
                                MapUtil.map(
                                        "statement", node.getProperty(SystemPropertyKeys.statement.name()),
                                        "selector", Util.fromJson((String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class),
                                        "params", Util.fromJson((String) node.getProperty(SystemPropertyKeys.params.name()), Map.class),
                                        "paused", node.getProperty(SystemPropertyKeys.paused.name()),
                                        "eventsToProcess", new ArrayList<>()
                                )
                        );
                    }
            );
            return null;
        });

        reconcileKernelRegistration();
    }

    /**
     * There is substantial memory overhead to the kernel event system, so if a user has enabled apoc triggers in
     * config, but there are no triggers set up, unregister to let the kernel bypass the event handling system.
     *
     * For most deployments this isn't an issue, since you can turn the config flag off, but in large fleet deployments
     * it's nice to have uniform config, and then the memory savings on databases that don't use triggers is good.
     */
    private synchronized void reconcileKernelRegistration() {
        // Register if there are triggers
        if (activeTriggers.size() > 0) {
            // This gets called every time triggers update; only register if we aren't already
            if(registeredWithKernel.compareAndSet(false, true)) {
                databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
            }
        } else {
            // This gets called every time triggers update; only unregister if we aren't already
            if(registeredWithKernel.compareAndSet(true, false)) {
                databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
            }
        }
    }

    public Map<String, Object> add(String name, String statement, Map<String,Object> selector, Map<String,Object> params) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.get(name);

        withSystemDb(tx -> {
            Node node = Util.mergeNode(tx, SystemLabels.ApocTrigger, null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), name));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);
            setLastUpdate(tx);
            return null;
        });

        updateCache();
        return previous;
    }

    public Map<String, Object> remove(String name) {
        checkEnabled();
        Map<String, Object> previous = activeTriggers.remove(name);

        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                            SystemPropertyKeys.database.name(), db.databaseName(),
                            SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node -> node.delete());
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return previous;
    }

    public Map<String, Object> updatePaused(String name, boolean paused) {
        checkEnabled();
        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                            SystemPropertyKeys.database.name(), db.databaseName(),
                            SystemPropertyKeys.name.name(), name)
                    .forEachRemaining(node -> node.setProperty(SystemPropertyKeys.paused.name(), paused));
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return activeTriggers.get(name);
    }

    public Map<String, Object> removeAll() {
        checkEnabled();
        Map<String, Object> previous = activeTriggers
                .entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        withSystemDb(tx -> {
            tx.findNodes(SystemLabels.ApocTrigger,
                            SystemPropertyKeys.database.name(), db.databaseName() )
                    .forEachRemaining(node -> node.delete());
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return previous;
    }

    public Map<String,Map<String,Object>> list() {
        checkEnabled();
        return Map.copyOf(activeTriggers);
    }
    
    public <T> Set<T> toSet(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        Set<T> set = new HashSet<>();
        while (iterator.hasNext())
            set.add(iterator.next());
        return set;
    }

    public void dispatchNewEvents(Set<Entity> newEvents) {
        activeReactiveTriggers.forEachValue(1, (trigger -> {
            Set<Entity> newNodes = newEvents.stream().filter(event -> event instanceof Node).filter(node -> ((Node) node).hasLabel(Label.label(trigger.getEvent()))).collect(Collectors.toSet());
            Set<Entity> newRels = newEvents.stream().filter(event -> event instanceof Relationship).filter(node -> ((Relationship) node).isType(RelationshipType.withName(trigger.getEvent()))).collect(Collectors.toSet());
            trigger.enqueueNewEntitiesToProcess(newNodes);
            trigger.enqueueNewEntitiesToProcess(newRels);
        }));
    }

    @Override
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.before)) {

            // Creating auxiliary trigger structures
            for (Map.Entry<String, Map<String,Object>> trigger: activeTriggers.entrySet()) {
                ReactiveTrigger reactiveTrigger = new ReactiveTrigger(
                        trigger.getKey(),
                        (String) trigger.getValue().get("statement"),
                        (Map<String, Object>) trigger.getValue().get("params"),
                        (Map<String, Object>) trigger.getValue().get("selector"),
                        (Boolean) trigger.getValue().get("paused")
                );
                log.info(reactiveTrigger.getName());
                activeReactiveTriggers.put(reactiveTrigger.name, reactiveTrigger);
            }

            // Identifying and sorting (ascending) trigger strata
            List<Long> sortedStrata = identifyAndSortStrata();
            log.info("Strata: " + sortedStrata);

            // Creating set that is going to keep all the new entities added by the user first and by triggers later
            log.info("Collecting initial events...");
            Set<Entity> newEntitiesInTx = new HashSet<>();
            txData.createdRelationships().forEach(newEntitiesInTx::add);
            txData.createdNodes().forEach(newEntitiesInTx::add);

            // Dispatching initial events (i.e. created relationships and nodes) inserted with user queries
            log.info("Dispatching events...");
            dispatchNewEvents(newEntitiesInTx);

            // Stratum by stratum...
            for (Long currentStratum : sortedStrata) {
                log.info("Processing stratum: " + currentStratum);

                Set<Entity> deltaSet;
                do {
                    // For each trigger sequentially activate it if possible
                    activeReactiveTriggers.forEachValue(999, (trigger) -> {
                        // Skip trigger activation if...
                        // ... the trigger is not part of the current stratum
                        if (!trigger.getStratum().equals(currentStratum)) return;
                        // ... or it is paused
                        if (trigger.getPaused()) return;
                        // ... or it has not the 'before' phase
                        if (!trigger.getPhase().equals("before")) return;
                        // ... or its queue of events to process is empty
                        if (trigger.entitiesToProcess.isEmpty()) {
                            log.info(trigger.getName() + ": no events to process.");
                            return;
                        }
                        // Otherwise, fire it
                        executeSingleTrigger(trigger, transaction, Phase.before);
                    });

                    // Collecting new events generated by the triggers activation
                    deltaSet = new HashSet<>();
                    txData.createdRelationships().forEach(deltaSet::add);
                    txData.createdNodes().forEach(deltaSet::add);

                    // Collecting only actual new events and add them the set of new entities generated in the transaction
                    deltaSet.removeAll(newEntitiesInTx);
                    newEntitiesInTx.addAll(deltaSet);

                    // Dispatch those new events
                    dispatchNewEvents(deltaSet);

                    //if the deltaSet is not empty it means we did not reach a quiescent state -> trigger in the current stratum must be activated at least once more
                } while (!deltaSet.isEmpty());
            }
        }
        return null;
    }

    private void executeSingleTrigger(ReactiveTrigger trigger, Transaction tx, Phase phase) {
        Map<String, Object> params = new HashMap<>();
        log.info("ReactiveTrigger - Executing trigger: " + trigger.getName());
        try {
            params.put("trigger", trigger.getName());
            params.put("createdNodes", trigger.getEntitiesToProcess().stream().filter(entity -> entity instanceof Node).map(node -> (Node) node));
            params.put("createdRelationships", trigger.getEntitiesToProcess().stream().filter(entity -> entity instanceof Relationship).map(rel -> (Relationship) rel));
            Result result = tx.execute(trigger.getStatement(), params);
            Iterators.count(result);
            trigger.emptyEntitiesToProcess();
        } catch (Exception e) {
            log.warn("Error executing trigger " + trigger.getName() + " in phase " + phase, e);
            throw new RuntimeException("Error executing trigger '" +trigger.getName()+"': " + e.getMessage());
        }
    }

    private List<Long> identifyAndSortStrata() {
        return activeReactiveTriggers.values().stream().map(ReactiveTrigger::getStratum).distinct().sorted().collect(Collectors.toList());
    }

    @Override
    public void afterCommit(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        // if `txData.metaData()` is equal to TRIGGER_META,
        // it means that the transaction comes from another TriggerHandler transaction,
        // therefore the execution must be blocked to prevent a deadlock due to cascading transactions
        if (isTransactionCreatedByTrigger(txData)) {
            return;
        }

        if (hasPhase(Phase.after)) {
            try (Transaction tx = db.beginTx()) {
                setTriggerMetadata(tx);
                executeTriggers(tx, txData, Phase.after);
                tx.commit();
            }
        }
        afterAsync(txData);
    }

    private static boolean isTransactionCreatedByTrigger(TransactionData txData) {
        Map<String, Object> metaData = txData.metaData();
        return metaData.equals(TRIGGER_META);
    }

    private void afterAsync(TransactionData txData) {
        if (hasPhase(Phase.afterAsync)) {
            TriggerMetadata triggerMetadata = TriggerMetadata.from(txData, true);
            Util.inTxFuture(pools.getDefaultExecutorService(), db, (inner) -> {
                setTriggerMetadata(inner);
                executeTriggers(inner, triggerMetadata.rebind(inner), Phase.afterAsync);
                return null;
            });
        }
    }

    private static void setTriggerMetadata(Transaction tx) {
        tx.execute("CALL tx.setMetaData($data)",
                Map.of("data", TRIGGER_META) );
    }

    @Override
    public void afterRollback(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.rollback)) {
            try (Transaction tx = db.beginTx()) {
                executeTriggers(tx, txData, Phase.rollback);
                tx.commit();
            }
        }
    }

    private boolean hasPhase(Phase phase) {
        return activeTriggers.values().stream()
                .map(data -> (Map<String, Object>) data.get("selector"))
                .anyMatch(selector -> when(selector, phase));
    }

    private void executeTriggers(Transaction tx, TransactionData txData, Phase phase) {
        executeTriggers(tx, TriggerMetadata.from(txData, false), phase);
    }

    private void executeTriggers(Transaction tx, TriggerMetadata triggerMetadata, Phase phase) {
        Map<String,String> exceptions = new LinkedHashMap<>();
        activeTriggers.forEach((name, data) -> {
            Map<String, Object> params = triggerMetadata.toMap();
            if (data.get("params") != null) {
                params.putAll((Map<String, Object>) data.get("params"));
            }
            Map<String, Object> selector = (Map<String, Object>) data.get("selector");
            if ((!(boolean)data.get("paused")) && when(selector, phase)) {
                try {
                    params.put("trigger", name);
                    Result result = tx.execute((String) data.get("statement"), params);
                    Iterators.count(result);
                } catch (Exception e) {
                    log.warn("Error executing trigger " + name + " in phase " + phase, e);
                    exceptions.put(name, e.getMessage());
                }
            }
        });
        if (!exceptions.isEmpty()) {
            throw new RuntimeException("Error executing triggers "+exceptions.toString());
        }
    }

    private boolean when(Map<String, Object> selector, Phase phase) {
        if (selector == null) return phase == Phase.before;
        return Phase.valueOf(selector.getOrDefault("phase", "before").toString()) == phase;
    }

    @Override
    public void start() {
        updateCache();
        long refreshInterval = apocConfig().getInt(TRIGGER_REFRESH, 60000);
        restoreTriggerHandler = jobScheduler.scheduleRecurring(Group.STORAGE_MAINTENANCE, () -> {
            if (getLastUpdate() > lastUpdate) {
                updateCache();
            }
        }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if(registeredWithKernel.compareAndSet(true, false)) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
        if (restoreTriggerHandler != null) {
            restoreTriggerHandler.cancel();
        }
    }

    private <T> T withSystemDb(Function<Transaction, T> action) {
        try (Transaction tx = apocConfig.getSystemDb().beginTx()) {
            T result = action.apply(tx);
            tx.commit();
            return result;
        }
    }

    private long getLastUpdate() {
        return withSystemDb( tx -> {
            Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
            return node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }

    private void setLastUpdate(Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), db.databaseName());
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }

}