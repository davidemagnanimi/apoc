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
package apoc.path;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 19.02.18
 */
public class PathsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Paths.class);
        db.executeTransactionally("CREATE (a:A)-[:NEXT]->(b:B)-[:NEXT]->(c:C)-[:NEXT]->(d:D)");
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void createFull() {
        TestUtil.testCall(db, "MATCH path = (a:A)-[:NEXT*]->(d:D) RETURN apoc.path.create(a, relationships(path)) as p",(row) -> assertPath(row, 3, "A", "D"));
    }
    @Test
    public void createEmpty() {
        TestUtil.testCall(db, "RETURN apoc.path.create(null) as p",(row) -> assertNull(row.get("p")));

        TestUtil.testCall(db, "MATCH (a:A) RETURN apoc.path.create(a) as p",(row) -> assertPath(row, 0, "A", "A"));
        TestUtil.testCall(db, "MATCH (a:A) RETURN apoc.path.create(a,null) as p",(row) -> assertPath(row, 0, "A", "A"));
        TestUtil.testCall(db, "MATCH (a:A)-[r:NEXT]->() RETURN apoc.path.create(a,[r]) as p",(row) -> assertPath(row, 1, "A", "B"));
    }

    @Test
    public void slice() {
        TestUtil.testCall(db, "MATCH path = (a:A)-[:NEXT*]->(d:D) RETURN apoc.path.slice(path,1,1) as p",(row) -> assertPath(row, 1, "B", "C"));
        TestUtil.testCall(db, "MATCH path = (a:A)-[:NEXT*]->(d:D) RETURN apoc.path.slice(path,1) as p",(row) -> assertPath(row, 2, "B", "D"));
        TestUtil.testCall(db, "MATCH path = (a:A)-[:NEXT*]->(d:D) RETURN apoc.path.slice(path,1,100) as p",(row) -> assertPath(row, 2, "B", "D"));
        TestUtil.testCall(db, "MATCH path = (a:A)-[:NEXT*]->(d:D) RETURN apoc.path.slice(path,-1,0) as p",(row) -> assertPath(row, 0, "A", "A"));
        TestUtil.testCall(db, "MATCH path = (a:A)-[:NEXT*]->(d:D) RETURN apoc.path.slice(path,1,0) as p",(row) -> assertPath(row, 0, "B", "B"));
        TestUtil.testCall(db, "RETURN apoc.path.slice(null) as p",(row) -> assertNull(row.get("p")));
    }

    @Test
    public void combineFail() {
        QueryExecutionException e = assertThrows(QueryExecutionException.class,
                () -> TestUtil.testCall(db, "MATCH p1 = (a:A)-[:NEXT]->(b:B), p2 = (c:C)-[:NEXT]->() RETURN apoc.path.combine(p1,p2) AS p", (r) -> {})
        );
        assertTrue(e.getMessage().contains("Paths don't connect on their end and start-nodes"));
    }
    @Test
    public void combine() {
        TestUtil.testCall(db, "MATCH p1 = (a:A)-[:NEXT]->(b:B), p2 = (b)-[:NEXT]->() RETURN apoc.path.combine(p1,p2) as p",(row) -> assertPath(row, 2, "A", "C"));
        TestUtil.testCall(db, "MATCH p1 = (a:A)-[:NEXT]->(b:B) RETURN apoc.path.combine(p1,null) as p",(row) -> assertPath(row, 1, "A", "B"));
        TestUtil.testCall(db, "MATCH p1 = (a:A)-[:NEXT]->(b:B) RETURN apoc.path.combine(null,p1) as p",(row) -> assertPath(row, 1, "A", "B"));
    }

    @Test
    public void elements() {
        TestUtil.testCall(db, "MATCH p = (a:A) RETURN apoc.path.elements(p) as e",(row) -> {
            List<Entity> pc = (List<Entity>) row.get("e");
            assertEquals(1,pc.size());
            assertEquals(true,((Node)pc.get(0)).hasLabel(Label.label("A")));
        });
        TestUtil.testCall(db, "MATCH p = (a:A)-->() RETURN apoc.path.elements(p) as e",(row) -> {
            List<Entity> pc = (List<Entity>) row.get("e");
            assertEquals(3,pc.size());
            assertEquals(true,((Node)pc.get(0)).hasLabel(Label.label("A")));
            assertEquals(true,((Relationship)pc.get(1)).isType(RelationshipType.withName("NEXT")));
            assertEquals(true,((Node)pc.get(2)).hasLabel(Label.label("B")));
        });
    }

    private void assertPath(Map<String, Object> row, int length, String startLabel, String endLabel) {
        Path p = (Path) row.get("p");
        assertEquals(length,p.length());
        assertEquals(true,p.startNode().hasLabel(Label.label(startLabel)));
        assertEquals(true,p.endNode().hasLabel(Label.label(endLabel)));
        assertEquals(length > 0,p.relationships().iterator().hasNext());
        for (Relationship rel : p.relationships()) {
            assertEquals("NEXT",rel.getType().name());
        }
    }
}
