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
package apoc.number;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ArabicRomanTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ArabicRoman.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testToArabic() {
        testCall(db, "RETURN apoc.number.romanToArabic('MCMXXXII') AS value", row -> assertEquals(1932, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('C') AS value", row -> assertEquals(100, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('mmx') AS value", row -> assertEquals(2010, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('MXXIV') AS value", row -> assertEquals(1024, row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('aaa') AS value", row -> assertEquals(0,  row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic('') AS value", row -> assertEquals(0,  row.get("value")));
        testCall(db, "RETURN apoc.number.romanToArabic(null) AS value", row -> assertEquals(0,  row.get("value")));
    }

    @Test
    public void testToRoman() {
        testCall(db, "RETURN apoc.number.arabicToRoman(1932) AS value", row -> assertEquals("MCMXXXII", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(100) AS value", row -> assertEquals("C", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(2010) AS value", row -> assertEquals("MMX", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(1024) AS value", row -> assertEquals("MXXIV", row.get("value")));
        testCall(db, "RETURN apoc.number.arabicToRoman(null) AS value", row -> assertNull(row.get("value")));
    }
}
