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
package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.export.json.ImportJson;
import apoc.graph.Graphs;
import apoc.meta.Meta;
import apoc.refactor.GraphRefactoring;
import apoc.refactor.rename.Rename;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;

public class BigGraphTest {
    private static final File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
            .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("1G"))
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Rename.class, ExportCSV.class, ExportJson.class, ExportCypher.class, ExportGraphML.class, Graphs.class, Meta.class, GraphRefactoring.class,
                ImportCsv.class, ImportJson.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        final String query = Util.readResourceFile("moviesMod.cypher");
        IntStream.range(0, 10000).forEach(__-> db.executeTransactionally(query));
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testTerminateExportCsv() {
        checkTerminationGuard(db, "CALL apoc.export.csv.all('testTerminate.csv',{bulkImport: true})");
    }

    @Test
    public void testTerminateExportGraphMl() {
        checkTerminationGuard(db, "CALL apoc.export.graphml.all('testTerminate.graphml', {})");
    }

    @Test
    public void testTerminateExportCypher() {
        checkTerminationGuard(db, "CALL apoc.export.cypher.all('testTerminate.cypher',{})");
    }

    @Test
    public void testTerminateExportJson() {
        checkTerminationGuard(db, "CALL apoc.export.json.all('testTerminate.json',{})");
    }

    @Test
    public void testTerminateRenameNodeProp() {
        checkTerminationGuard(db, "CALL apoc.refactor.rename.nodeProperty('name', 'nameTwo')");
    }

    @Test
    public void testTerminateRenameType() {
        checkTerminationGuard(db,  50L, "CALL apoc.refactor.rename.type('DIRECTED', 'DIRECTED_TWO')");
    }

    @Test
    public void testTerminateRefactorProcs() {
        List<Node> nodes = db.executeTransactionally("MATCH (n:Person) RETURN collect(n) as nodes", Collections.emptyMap(),
                r -> r.<List<Node>>columnAs("nodes").next());
        
        checkTerminationGuard(db, "CALL apoc.refactor.cloneNodes($nodes)", 
                Map.of("nodes", nodes));

        checkTerminationGuard(db, "CALL apoc.refactor.cloneSubgraph($nodes)",
                Map.of("nodes", nodes));
    }
}
