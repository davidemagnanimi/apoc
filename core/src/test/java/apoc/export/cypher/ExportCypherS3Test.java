package apoc.export.cypher;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.s3.S3TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.export.util.ExportFormat.*;
import static apoc.util.Util.map;
import static org.junit.Assert.*;

@Ignore("To use this test, you need to set the S3 bucket and region to a valid endpoint " +
        "and have your access key and secret key setup in your environment.")
public class ExportCypherS3Test {
    private static String S3_BUCKET_NAME = null;

    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @Rule
    public TestName testName = new TestName();

    private static final String OPTIMIZED = "Optimized";
    private static final String ODD = "OddDataset";

    private static String getS3Url(String key) {
        return String.format("s3://:@/%s/%s", S3_BUCKET_NAME, key);
    }

    private void verifyUpload(String fileName, String expected) throws IOException {
        String s3Url = getS3Url(fileName);
        S3TestUtil.readFile(s3Url, Paths.get(directory.toString(), fileName).toString());
        assertEquals(expected, readFile(fileName));
    }

    private static String readFile(String fileName) {
        return TestUtil.readFileToString(new File(directory, fileName));
    }

    private static String getEnvVar(String envVarKey) throws Exception {
        return Optional.ofNullable(System.getenv(envVarKey)).orElseThrow(
                () -> new Exception(String.format("%s is not set in the environment", envVarKey))
        );
    }

    @Before
    public void setUp() throws Exception {
        if (S3_BUCKET_NAME == null) {
            S3_BUCKET_NAME = getEnvVar("S3_BUCKET_NAME");
        }
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        db.executeTransactionally("CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name)");
        db.executeTransactionally("CREATE RANGE INDEX FOR (n:Foo) ON (n.name)");
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint FOR (b:Bar) REQUIRE b.name IS UNIQUE");
        if (testName.getMethodName().endsWith(OPTIMIZED)) {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:12})," +
                    " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})");
        } else if(testName.getMethodName().endsWith(ODD)) {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})," +
                    "(t:Foo {name:'foo2', born:date('2017-09-29')})," +
                    "(g:Foo {name:'foo3', born:date('2016-03-12')})," +
                    "(b:Bar {name:'bar',age:42})," +
                    "(c:Bar {age:12})," +
                    "(d:Bar {age:4})," +
                    "(e:Bar {name:'bar2',age:44})," +
                    "(f)-[:KNOWS {since:2016}]->(b)");
        } else {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})");
        }
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(fileName, EXPECTED_NEO4J_SHELL);
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$config)",
                map("s3", s3Url, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload(fileName, EXPECTED_CYPHER_SHELL);
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")), (r) -> {
                });
        verifyUpload(fileName, EXPECTED_NEO4J_SHELL);
    }

    @Test
    public void testExportGraphCypher() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload(fileName, EXPECTED_NEO4J_SHELL);
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload("all.nodes.cypher", EXPECTED_NODES);
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload("all.relationships.cypher", EXPECTED_RELATIONSHIPS);
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload("all.schema.cypher", EXPECTED_SCHEMA);
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        verifyUpload("all.cleanup.cypher", EXPECTED_CLEAN_UP);
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload("graph.nodes.cypher", EXPECTED_NODES);
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source, format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload("graph.relationships.cypher", EXPECTED_RELATIONSHIPS);
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload("graph.schema.cypher", EXPECTED_SCHEMA);
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        verifyUpload("graph.cleanup.cypher", EXPECTED_CLEAN_UP);
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "plain")), (r) -> {
                });
        verifyUpload(fileName, EXPECTED_PLAIN);
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        verifyUpload(fileName, EXPECTED_NEO4J_MERGE);
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        verifyUpload(fileName, EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY);
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        String fileName = "all.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        verifyUpload(fileName, EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY);
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        String fileName = "onlySchema.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($s3,$exportConfig)", map("s3", s3Url, "exportConfig", exportConfig), (r) -> {
        });
        verifyUpload(fileName, EXPECTED_ONLY_SCHEMA_NEO4J_SHELL);
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        String fileName = "onlySchema.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {});
        verifyUpload(fileName, EXPECTED_ONLY_SCHEMA_CYPHER_SHELL);
    }

    @Test
    public void testExportCypherNodePoint() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})");
        String fileName = "temporalPoint.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        verifyUpload(fileName, EXPECTED_CYPHER_POINT);
    }

    @Test
    public void testExportCypherNodeDate() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})");
        String fileName = "temporalDate.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        verifyUpload(fileName, EXPECTED_CYPHER_DATE);
    }

    @Test
    public void testExportCypherNodeTime() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})");
        String fileName = "temporalTime.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        verifyUpload(fileName, EXPECTED_CYPHER_TIME);
    }

    @Test
    public void testExportCypherNodeDuration() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})");
        String fileName = "temporalDuration.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        verifyUpload(fileName, EXPECTED_CYPHER_DURATION);
    }

    @Test
    public void testExportWithAscendingLabels() throws IOException {
        db.executeTransactionally("CREATE (f:User:User1:User0:User12 {name:'Alan'})");
        String fileName = "ascendingLabels.cypher";
        String s3Url = getS3Url(fileName);
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        verifyUpload(fileName, EXPECTED_CYPHER_LABELS_ASCENDEND);
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, format: 'neo4j-shell'})", map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE);
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3, $exportConfig)", map("s3", s3Url, "exportConfig", map("format", "neo4j-shell")),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_NEO4J_OPTIMIZED);
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3, $exportConfig)",
                map("s3", s3Url, "exportConfig", map("separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload("allDefaultOptimized.nodes.cypher", EXPECTED_NODES_OPTIMIZED);
        verifyUpload("allDefaultOptimized.relationships.cypher", EXPECTED_RELATIONSHIPS_OPTIMIZED);
        verifyUpload("allDefaultOptimized.schema.cypher", EXPECTED_SCHEMA);
        verifyUpload("allDefaultOptimized.cleanup.cypher", EXPECTED_CLEAN_UP);
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE);
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell'})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_CYPHER_SHELL_OPTIMIZED);
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE);
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainAddStructureOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND);
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        verifyUpload(fileName, EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND);
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_UPDATE_ALL_UNWIND);
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        verifyUpload(fileName, EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND);
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("s3", s3Url), (r) -> assertResultsOdd(s3Url, r));
        verifyUpload(fileName, EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD);
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("s3", s3Url),
                (r) -> assertResultsOdd(s3Url, r));
        verifyUpload(fileName, EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD);
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddBatchSizeOddDataset() throws Exception {
        db.executeTransactionally("CREATE (:Bar {name:'bar3',age:35}), (:Bar {name:'bar4',age:36})");
        String fileName = "allPlainOddNew.cypher";
        String s3Url = getS3Url(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:3})",
                map("s3", s3Url),
                (r) -> {});
        db.executeTransactionally("MATCH (n:Bar {name:'bar3',age:35}), (n1:Bar {name:'bar4',age:36}) DELETE n, n1");
        String expectedNodes = String.format(":begin%n" +
                ":param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                ":param rows => [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":param rows => [{name:\"bar\", properties:{age:42}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{name:\"bar2\", properties:{age:44}}, {name:\"bar3\", properties:{age:35}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":param rows => [{name:\"bar4\", properties:{age:36}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n");
        int expectedDropNum = 3;
        String expectedDrop = String.format(":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "DROP CONSTRAINT uniqueConstraint;%n" +
                ":commit%n", expectedDropNum, expectedDropNum);
        String expected = (EXPECTED_SCHEMA + expectedNodes + EXPECTED_RELATIONSHIPS_PARAMS_ODD + expectedDrop)
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());
        verifyUpload(fileName, expected);
    }

    private void assertResultsOptimized(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertResultsOdd(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }
}
