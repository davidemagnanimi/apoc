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
package apoc.export.csv;

import apoc.csv.CsvTestUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;

public class ImportCsvLdbcTest {

    private final String postfix = ".csv";

    private final Map<String, List<String>> nodeCsvTypes = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleEntry<>("comment",      Arrays.asList("Message", "Comment"   )),
            new AbstractMap.SimpleEntry<>("forum",        Arrays.asList("Forum"                )),
            new AbstractMap.SimpleEntry<>("organisation", Arrays.asList("Company", "University")),
            new AbstractMap.SimpleEntry<>("person",       Arrays.asList("Person"               )),
            new AbstractMap.SimpleEntry<>("place",        Arrays.asList("Place"                )),
            new AbstractMap.SimpleEntry<>("post",         Arrays.asList("Message", "Post"      )),
            new AbstractMap.SimpleEntry<>("tagclass",     Arrays.asList("TagClass"             )),
            new AbstractMap.SimpleEntry<>("tag",          Arrays.asList("Tag"                  ))
        ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    private final Map<String, String> nodeCsvs = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleEntry<>("comment",      "id:ID(Comment)|creationDate:LONG|locationIP:STRING|browserUsed:STRING|content:STRING|length:INT\n"),
            new AbstractMap.SimpleEntry<>("forum",        "id:ID(Forum)|title:STRING|creationDate:LONG\n" +
                    "0|Wall of Mahinda Perera|20100214153220447\n"),
            new AbstractMap.SimpleEntry<>("organisation", "id:ID(Organisation)|:LABEL|name:STRING|url:STRING\n"),
            new AbstractMap.SimpleEntry<>("person",       "id:ID(Person)|firstName:STRING|lastName:STRING|gender:STRING|birthday:LONG|creationDate:LONG|locationIP:STRING|browserUsed:STRING\n" +
                    "0|Mahinda|Perera|male|19891203|20100214153210447|119.235.7.103|Firefox\n"),
            new AbstractMap.SimpleEntry<>("place",        "id:ID(Place)|name:STRING|url:STRING|:LABEL\n" +
                    "0|India|http://dbpedia.org/resource/India|Country\n" +
                    "1|New Delphi|http://dbpedia.org/resource/New_Delhi|City\n"),
            new AbstractMap.SimpleEntry<>("post",         "id:ID(Post)|imageFile:STRING|creationDate:LONG|locationIP:STRING|browserUsed:STRING|language:STRING|content:STRING|length:INT\n" +
                    "0||20110817060540595|49.246.218.237|Firefox|uz|About Rupert Murdoch, t newer electronic publishing technoAbout George Frideric Handel,  concertos. Handel was born in 1685,About Kurt Vonne|140\n"),
            new AbstractMap.SimpleEntry<>("tagclass",     "id:ID(TagClass)|name:STRING|url:STRING\n"),
            new AbstractMap.SimpleEntry<>("tag",          "id:ID(Tag)|name:STRING|url:STRING\n")
    ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    private final Map<String, String> relationshipCsvTypes = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleEntry<>("comment_hasCreator_person",      "HAS_CREATOR"   ),
            new AbstractMap.SimpleEntry<>("comment_hasTag_tag",             "HAS_TAG"       ),
            new AbstractMap.SimpleEntry<>("comment_isLocatedIn_place",      "IS_LOCATED_IN" ),
            new AbstractMap.SimpleEntry<>("comment_replyOf_comment",        "REPLY_OF"      ),
            new AbstractMap.SimpleEntry<>("comment_replyOf_post",           "REPLY_OF"      ),
            new AbstractMap.SimpleEntry<>("forum_containerOf_post",         "CONTAINER_OF"  ),
            new AbstractMap.SimpleEntry<>("forum_hasMember_person",         "HAS_MEMBER"    ),
            new AbstractMap.SimpleEntry<>("forum_hasModerator_person",      "HAS_MODERATOR" ),
            new AbstractMap.SimpleEntry<>("forum_hasTag_tag",               "HAS_TAG"       ),
            new AbstractMap.SimpleEntry<>("organisation_isLocatedIn_place", "IS_LOCATED_IN" ),
            new AbstractMap.SimpleEntry<>("person_hasInterest_tag",         "HAS_INTEREST"  ),
            new AbstractMap.SimpleEntry<>("person_isLocatedIn_place",       "IS_LOCATED_IN" ),
            new AbstractMap.SimpleEntry<>("person_knows_person",            "KNOWS"         ),
            new AbstractMap.SimpleEntry<>("person_likes_comment",           "LIKES"         ),
            new AbstractMap.SimpleEntry<>("person_likes_post",              "LIKES"         ),
            new AbstractMap.SimpleEntry<>("person_studyAt_organisation",    "STUDY_OF"      ),
            new AbstractMap.SimpleEntry<>("person_workAt_organisation",     "WORK_AT"       ),
            new AbstractMap.SimpleEntry<>("place_isPartOf_place",           "IS_PART_OF"    ),
            new AbstractMap.SimpleEntry<>("post_hasCreator_person",         "HAS_CREATOR"   ),
            new AbstractMap.SimpleEntry<>("post_hasTag_tag",                "HAS_TAG"       ),
            new AbstractMap.SimpleEntry<>("post_isLocatedIn_place",         "IS_LOCATED_IN" ),
            new AbstractMap.SimpleEntry<>("tag_hasType_tagclass",           "HAS_TYPE"      ),
            new AbstractMap.SimpleEntry<>("tagclass_isSubclassOf_tagclass", "IS_SUBCLASS_OF")
        ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    private final Map<String, String> relationshipCsvs = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleEntry<>("comment_hasCreator_person",      ":START_ID(Comment)|:END_ID(Person)\n"),
            new AbstractMap.SimpleEntry<>("comment_hasTag_tag",             ":START_ID(Comment)|:END_ID(Tag)\n"),
            new AbstractMap.SimpleEntry<>("comment_isLocatedIn_place",      ":START_ID(Comment)|:END_ID(Place)\n"),
            new AbstractMap.SimpleEntry<>("comment_replyOf_comment",        ":START_ID(Comment)|:END_ID(Comment)\n"),
            new AbstractMap.SimpleEntry<>("comment_replyOf_post",           ":START_ID(Comment)|:END_ID(Post)\n"),
            new AbstractMap.SimpleEntry<>("forum_containerOf_post",         ":START_ID(Forum)|:END_ID(Post)\n" +
                    "0|0\n"),
            new AbstractMap.SimpleEntry<>("forum_hasMember_person",         ":START_ID(Forum)|:END_ID(Person)|joinDate:LONG\n" +
                    "0|0|20100313073731718\n"),
            new AbstractMap.SimpleEntry<>("forum_hasModerator_person",      ":START_ID(Forum)|:END_ID(Person)\n"),
            new AbstractMap.SimpleEntry<>("forum_hasTag_tag",               ":START_ID(Forum)|:END_ID(Tag)\n"),
            new AbstractMap.SimpleEntry<>("organisation_isLocatedIn_place", ":START_ID(Organisation)|:END_ID(Place)\n"),
            new AbstractMap.SimpleEntry<>("person_hasInterest_tag",         ":START_ID(Person)|:END_ID(Tag)\n"),
            new AbstractMap.SimpleEntry<>("person_isLocatedIn_place",       ":START_ID(Person)|:END_ID(Place)\n" +
                    "0|1\n"),
            new AbstractMap.SimpleEntry<>("person_knows_person",            ":START_ID(Person)|:END_ID(Person)|creationDate:LONG\n"),
            new AbstractMap.SimpleEntry<>("person_likes_comment",           ":START_ID(Person)|:END_ID(Comment)|creationDate:LONG\n"),
            new AbstractMap.SimpleEntry<>("person_likes_post",              ":START_ID(Person)|:END_ID(Post)|creationDate:LONG\n"),
            new AbstractMap.SimpleEntry<>("person_studyAt_organisation",    ":START_ID(Person)|:END_ID(Organisation)|classYear:INT\n"),
            new AbstractMap.SimpleEntry<>("person_workAt_organisation",     ":START_ID(Person)|:END_ID(Organisation)|workFrom:INT\n"),
            new AbstractMap.SimpleEntry<>("place_isPartOf_place",           ":START_ID(Place)|:END_ID(Place)\n" +
                    "1|0\n"),
            new AbstractMap.SimpleEntry<>("post_hasCreator_person",         ":START_ID(Post)|:END_ID(Person)\n" +
                    "0|0\n"),
            new AbstractMap.SimpleEntry<>("post_hasTag_tag",                ":START_ID(Post)|:END_ID(Tag)\n"),
            new AbstractMap.SimpleEntry<>("post_isLocatedIn_place",         ":START_ID(Post)|:END_ID(Place)\n" +
                    "0|1\n"),
            new AbstractMap.SimpleEntry<>("tag_hasType_tagclass",           ":START_ID(Tag)|:END_ID(TagClass)\n"),
            new AbstractMap.SimpleEntry<>("tagclass_isSubclassOf_tagclass", ":START_ID(TagClass)|:END_ID(TagClass)\n")
    ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, new File("src/test/resources/csv-inputs").toPath().toAbsolutePath())
            .withSetting(GraphDatabaseSettings.allow_file_urls, true);

    @Before
    public void setUp() throws Exception {
        for (Map.Entry<String, String> entry : nodeCsvs.entrySet()) {
            CsvTestUtil.saveCsvFile(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : relationshipCsvs.entrySet()) {
            CsvTestUtil.saveCsvFile(entry.getKey(), entry.getValue());
        }

        TestUtil.registerProcedure(db, ImportCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testLdbc() {
        final List<Map<String, Object>> nodes = new ArrayList<>();
        for (final Map.Entry<String, List<String>> nodeCsv : nodeCsvTypes.entrySet()) {
            final String fileName = nodeCsv.getKey();
            final List<String> labels = nodeCsv.getValue();

            final Map<String, Object> nodeMap = new HashMap<>();
            nodeMap.put("fileName", String.format("file:/%s%s", fileName, postfix));
            nodeMap.put("labels", labels);
            nodes.add(nodeMap);
        }

        final List<Map<String, Object>> relationships = new ArrayList<>();
        for (final Map.Entry<String, String> relationshipCsv : relationshipCsvTypes.entrySet()) {
            final String fileName = relationshipCsv.getKey();
            final String type = relationshipCsv.getValue();

            final Map<String, Object> relationshipMap = new HashMap<>();
            relationshipMap.put("fileName", String.format("file:/%s%s", fileName, postfix));
            relationshipMap.put("type", type);
            relationships.add(relationshipMap);
        }

        TestUtil.testCall(db,
            "CALL apoc.import.csv($nodes, $relationships, $config)",
            map(
                    "nodes", nodes,
                    "relationships", relationships,
                    "config", map(CsvLoaderConfig.DELIMITER, '|')
            ),
            (r) -> { }
        );

        long nodeCount = TestUtil.singleResultFirstColumn(db, "MATCH (n) RETURN count(n) AS nodeCount");
        long relationshipCount = TestUtil.singleResultFirstColumn(db, "MATCH ()-[r]->() RETURN count(r) AS relationshipCount");
        Assert.assertEquals(5, nodeCount);
        Assert.assertEquals(6, relationshipCount);
    }

}
