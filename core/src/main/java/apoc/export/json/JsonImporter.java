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
package apoc.export.json;

import apoc.export.util.Reporter;
import apoc.util.Util;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

import java.io.Closeable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.json.ImportJsonConfig.WILDCARD_PROPS;

public class JsonImporter implements Closeable {
    private static final String UNWIND = "UNWIND $rows AS row ";
    private static final String CREATE_NODE = UNWIND +
            "CREATE (n%s {%s}) SET n += row.properties";
    private static final String CREATE_RELS = UNWIND +
            "MATCH (s%s {%s: row.start.id}) " +
            "MATCH (e%s {%2$s: row.end.id}) " +
            "CREATE (s)-[r:%s]->(e) SET r += row.properties";
    public static final String MISSING_CONSTRAINT_ERROR_MSG = "Missing constraint required for import. Execute this query: \n" +
            "CREATE CONSTRAINT FOR (n:%s) REQUIRE n.%s IS UNIQUE;";

    private final List<Map<String, Object>> paramList;
    private final int unwindBatchSize;
    private final int txBatchSize;
    private final GraphDatabaseService db;
    private final Reporter reporter;

    private String lastType;
    private List<String> lastLabels;
    private Map<String, Object> lastRelTypes;

    private final ImportJsonConfig importJsonConfig;

    public JsonImporter(ImportJsonConfig importJsonConfig,
                        GraphDatabaseService db,
                        Reporter reporter) {
        this.paramList = new ArrayList<>(importJsonConfig.getUnwindBatchSize());
        this.db = db;
        this.txBatchSize = importJsonConfig.getTxBatchSize();
        this.unwindBatchSize = Math.min(importJsonConfig.getUnwindBatchSize(), txBatchSize);
        this.reporter = reporter;
        this.importJsonConfig = importJsonConfig;
    }

    public void importRow(Map<String, Object> param) {
        final String type = (String) param.get("type");

        manageEntityType(type);

        final List<String> labels;
        final Map<String, List<String>> propFilter;
        switch (type) {
            case "node":
                manageNode(param);
                labels = lastLabels;
                propFilter = importJsonConfig.getNodePropFilter();
                break;
            case "relationship":
                manageRelationship(param);
                labels = Collections.singletonList((String) lastRelTypes.get("label"));
                propFilter = importJsonConfig.getRelPropFilter();
                break;
            default:
                throw new IllegalArgumentException("Current type not supported: " + type);
        }

        final Map<String, Object> properties = (Map<String, Object>) param.getOrDefault("properties", Collections.emptyMap());

        final List<String> defaultProps = propFilter.getOrDefault(WILDCARD_PROPS, Collections.emptyList());
        
        properties.keySet()
                .removeIf(name -> {
                    final Predicate<String> nameInPropFilter = (i) -> propFilter.getOrDefault(i, defaultProps).contains(name);
                    return labels.stream().anyMatch(nameInPropFilter);
                });

        updateReporter(type, properties);
        param.put("properties", convertProperties(type, properties));

        paramList.add(param);
        if (paramList.size() % txBatchSize == 0) {
            final Collection<List<Map<String, Object>>> results = chunkData();
            paramList.clear();
            // write
            writeUnwindBatch(results);
        }
    }

    private void writeUnwindBatch(Collection<List<Map<String, Object>>> results) {
        results.forEach(resultList -> {
            if (resultList.size() == unwindBatchSize) {
                write(resultList);
            } else {
                paramList.addAll(resultList);
            }
        });
    }

    private void manageEntityType(String type) {
        if (lastType == null) {
            lastType = type;
        }
        if (!type.equals(lastType)) {
            flush();
            lastType = type;
        }
    }

    private void manageRelationship(Map<String, Object> param) {
        final List<String> startLabels = getLabels((Map<String, Object>) param.get("start"));
        final List<String> endLabels = getLabels((Map<String, Object>) param.get("end"));
        Map<String, Object> relType = Util.map(
                "start", startLabels,
                "end", endLabels,
                "label", getType(param));
        List<String> allLabels = Stream.concat(startLabels.stream(), endLabels.stream()).collect(Collectors.toList());
        if (lastRelTypes == null) {
            checkUniquenessConstraints(allLabels);
            lastRelTypes = relType;
        }
        if (!relType.equals(lastRelTypes)) {
            checkUniquenessConstraints(allLabels);
            flush();
            lastRelTypes = relType;
        }
    }

    private void manageNode(Map<String, Object> param) {
        List<String> labels = getLabels(param);
        if (lastLabels == null) {
            checkUniquenessConstraints(labels);
            lastLabels = labels;
        }
        if (!labels.equals(lastLabels)) {
            checkUniquenessConstraints(labels);
            flush();
            lastLabels = labels;
        }
    }

    /**
     * The constraint for the import name should be unique to avoid duplicated imports.
     * Node keys are a combination of a uniqueness constraint and en existence constraint, so can also be used.
     * The constraint should not be composite.
     */
    private void checkUniquenessConstraints(List<String> labels) {
        if (labels.isEmpty()) {
            return;
        }
        try (final Transaction tx = db.beginTx()) {
            final Schema schema = tx.schema();
            final String importIdName = importJsonConfig.getImportIdName();
            final String missingConstraint = labels.stream().filter(label -> 
                    StreamSupport.stream(schema.getConstraints(Label.label(label)).spliterator(), false)
                            .filter(constraint -> constraint.isConstraintType(ConstraintType.UNIQUENESS) || constraint.isConstraintType(ConstraintType.NODE_KEY))
                            .noneMatch(constraint -> Iterables.contains(constraint.getPropertyKeys(), importIdName) && Iterables.size(constraint.getPropertyKeys()) == 1)
            ).findAny()
            .orElse(null);
            if (missingConstraint != null) {
                throw new RuntimeException(String.format(MISSING_CONSTRAINT_ERROR_MSG, missingConstraint, importIdName));
            }
        }
    }

    private void updateReporter(String type, Map<String, Object> properties) {
        final int size = properties.size() + 1; // +1 is for the "neo4jImportId"
        switch (type) {
            case "node":
                reporter.update(1, 0, size);
                break;
            case "relationship":
                reporter.update(0, 1, size);
                break;
            default:
                throw new IllegalArgumentException("Current type not supported: " + type);
        }
    }

    private Stream<Map.Entry<String, Object>> flatMap(Map<String, Object> map, String key) {
        final String prefix = key != null ? key : "";
        return map.entrySet().stream()
                .flatMap(e -> {
                    if (e.getValue() instanceof Map) {
                        return flatMap((Map<String, Object>) e.getValue(), prefix + "." + e.getKey());
                    } else {
                        return Stream.of(new AbstractMap.SimpleEntry<>(prefix + "." + e.getKey(), e.getValue()));
                    }
                });
    }

    private List<Object> convertList(Collection<Object> coll, String classType) {
        return coll.stream()
                .map(c -> {
                    if (c instanceof Collection) {
                        return convertList((Collection<Object>) c, classType);
                    }
                    return convertMappedValue(c, classType);
                })
                .collect(Collectors.toList());
    }

    private Map<String, Object> convertProperties(String type, Map<String, Object> properties) {
        return properties.entrySet().stream()
                .flatMap(e -> {
                     if (e.getValue() instanceof Map) {
                         Map<String, Object> map = (Map<String, Object>) e.getValue();
                         String classType = getClassType(type, e.getKey());
                         if (classType != null && "POINT".equals(classType.toUpperCase())) {
                             return Stream.of(e);
                         }
                         return flatMap(map, e.getKey());
                     } else {
                         return Stream.of(e);
                     }
                })
                .map(e -> {
                    String key = e.getKey();
                    final String classType = getClassType(type, key);
                    if (e.getValue() instanceof Collection) {
                        final List<Object> coll = convertList((Collection<Object>) e.getValue(), classType);
                        return new AbstractMap.SimpleEntry<>(e.getKey(), coll);
                    } else {
                        return new AbstractMap.SimpleEntry<>(e.getKey(),
                                convertMappedValue(e.getValue(), classType));
                    }
                })
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    private String getClassType(String type, String key) {
        final String classType;
        switch (type) {
            case "node":
                classType = importJsonConfig.typeForNode(lastLabels, key);
                break;
            case "relationship":
                classType = importJsonConfig.typeForRel((String) lastRelTypes.get("label"), key);
                break;
            default:
                classType = null;
                break;
        }
        return classType;
    }

    private Object convertMappedValue(Object value, String classType) {
        if (classType == null) {
           return value;
        }
        switch (classType.toUpperCase()) {
            case "POINT":
                value = toPoint((Map<String, Object>) value);
                break;
            case "LOCALDATE":
                value = LocalDate.parse((String) value);
                break;
            case "LOCALTIME":
                value = LocalTime.parse((String) value);
                break;
            case "LOCALDATETIME":
                value = LocalDateTime.parse((String) value);
                break;
            case "DURATION":
                value = DurationValue.parse((String) value);
                break;
            case "OFFSETTIME":
                value = OffsetTime.parse((String) value);
                break;
            case "ZONEDDATETIME":
                value = ZonedDateTime.parse((String) value);
                break;
            default:
                break;
        }
        return value;
    }

    public static PointValue toPoint(Map<String, Object> pointMap) {
        return Util.toPoint(pointMap, Collections.emptyMap());
    }

    private String getType(Map<String, Object> param) {
        return Util.quote((String) param.get("label"));
    }

    private List<String> getLabels(Map<String, Object> param) {
        return (List<String>) param.getOrDefault("labels", Collections.emptyList());
    }

    private String getLabelString(List<String> labels) {
        labels = labels == null ? Collections.emptyList() : labels;
        final String delimiter = ":";
        final String join = labels.stream()
                .map(Util::quote)
                .collect(Collectors.joining(delimiter));
        return join.isBlank() ? join : (delimiter + join);
    }

    private void write(List<Map<String, Object>> resultList) {
        if (resultList.isEmpty()) return;
        final String type = (String) resultList.get(0).get("type");
        String query;
        switch (type) {
            case "node":
                final String importId = importJsonConfig.isCleanup() 
                        ? StringUtils.EMPTY
                        : importJsonConfig.getImportIdName() + ": row.id";
                query = String.format(CREATE_NODE, getLabelString(lastLabels), importId);
                break;
            case "relationship":
                String rel = (String) lastRelTypes.get("label");
                query = String.format(CREATE_RELS, getLabelString((List<String>) lastRelTypes.get("start")),
                        importJsonConfig.getImportIdName(),
                        getLabelString((List<String>) lastRelTypes.get("end")),
                        rel);
                break;
            default:
                throw new IllegalArgumentException("Current type not supported: " + type);
        }
        if (StringUtils.isNotBlank(query)) {
            db.executeTransactionally(query, Collections.singletonMap("rows", resultList));
        }
    }

    private Collection<List<Map<String, Object>>> chunkData() {
        AtomicInteger chunkCounter = new AtomicInteger(0);
        return paramList.stream()
                .collect(Collectors.groupingBy(it -> chunkCounter.getAndIncrement() / unwindBatchSize))
                .values();
    }

    @Override
    public void close() {
        flush();
        reporter.done();
    }

    private void flush() {
        if (!paramList.isEmpty()) {
            final Collection<List<Map<String, Object>>> results = chunkData();
            results.forEach( this::write );
            paramList.clear();
        }
    }
}
