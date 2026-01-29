/**
 * Copyright 2017-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.coral.spark.TestUtils.*;
import static org.testng.Assert.*;


/**
 * Tests for {@link TrinoToSparkConverter}.
 *
 * These tests convert Trino SQL to Spark SQL using the intermediate RelNode representation.
 * Test cases are derived from HiveToTrinoConverterTest outputs, using Trino SQL as input
 * and validating the Spark SQL output.
 *
 * V1 focuses on simple queries - complex queries like LATERAL VIEW, UDFs with dependencies,
 * and schema evolution are deferred to V2.
 */
public class TrinoToSparkConverterTest {

  private HiveConf conf;

  @BeforeClass
  public void beforeClass() throws HiveException, MetaException, IOException {
    conf = TestUtils.loadResourceHiveConf();
    TestUtils.initializeViews(conf);
  }

  @AfterTest
  public void afterClass() throws IOException {
    FileUtils.deleteDirectory(new File(conf.get(TestUtils.CORAL_SPARK_TEST_DIR)));
  }

  @Test
  public void testSimpleSelect() {
    // Trino SQL input (from HiveToTrinoConverterTest output pattern)
    String trinoSql = "SELECT * FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    // Spark SQL should have unquoted identifiers and proper syntax
    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("FROM default.foo"));
  }

  @Test
  public void testSelectWithColumns() {
    String trinoSql = "SELECT \"foo\".\"a\", \"foo\".\"b\" FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("foo.a"));
    assertTrue(sparkSql.contains("foo.b"));
  }

  @Test
  public void testSelectWithAlias() {
    String trinoSql = "SELECT \"foo\".\"a\" AS \"col_a\" FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("col_a"));
  }

  @Test
  public void testBasicJoin() {
    String trinoSql = "SELECT \"foo\".\"a\", \"bar\".\"x\" "
        + "FROM \"default\".\"foo\" AS \"foo\" "
        + "INNER JOIN \"default\".\"bar\" AS \"bar\" ON \"foo\".\"a\" = \"bar\".\"x\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("JOIN"));
    assertTrue(sparkSql.contains("default.foo"));
    assertTrue(sparkSql.contains("default.bar"));
  }

  @Test
  public void testLeftJoin() {
    String trinoSql = "SELECT \"foo\".\"a\", \"bar\".\"x\" "
        + "FROM \"default\".\"foo\" AS \"foo\" "
        + "LEFT JOIN \"default\".\"bar\" AS \"bar\" ON \"foo\".\"a\" = \"bar\".\"x\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("LEFT JOIN"));
  }

  @Test
  public void testWhereClause() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" AS \"foo\" WHERE \"foo\".\"a\" > 5";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("WHERE"));
    assertTrue(sparkSql.contains("> 5"));
  }

  @Test
  public void testGroupBy() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" AS \"foo\" GROUP BY \"foo\".\"a\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("GROUP BY"));
  }

  @Test
  public void testOrderBy() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" AS \"foo\" ORDER BY \"foo\".\"a\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("ORDER BY"));
  }

  @Test
  public void testUnion() {
    String trinoSql = "SELECT * FROM \"default\".\"foo\" UNION ALL SELECT * FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("UNION ALL"));
  }

  @Test
  public void testCastInteger() {
    String trinoSql = "SELECT CAST(\"foo\".\"a\" AS INTEGER) FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("CAST"));
  }

  @Test
  public void testCastDecimal() {
    String trinoSql = "SELECT CAST(\"foo\".\"a\" AS DECIMAL(6, 2)) FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("CAST"));
    assertTrue(sparkSql.contains("DECIMAL"));
  }

  @Test
  public void testCastString() {
    String trinoSql = "SELECT CAST(\"foo\".\"a\" AS VARCHAR) FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("CAST"));
  }

  @Test
  public void testArithmeticOperations() {
    String trinoSql = "SELECT \"foo\".\"a\" + 1, \"foo\".\"c\" * 2 FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("+ 1"));
    assertTrue(sparkSql.contains("* 2"));
  }

  @Test
  public void testComparisonOperators() {
    String trinoSql = "SELECT \"foo\".\"a\" > 1, \"foo\".\"a\" <= 10 FROM \"default\".\"foo\" WHERE \"foo\".\"a\" = 5";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("> 1"));
    assertTrue(sparkSql.contains("<= 10"));
    assertTrue(sparkSql.contains("= 5"));
  }

  @Test
  public void testInOperator() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" WHERE \"foo\".\"a\" IN (1, 2, 3)";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    // IN clause may be transformed - just verify the query contains WHERE and the values
    assertTrue(sparkSql.contains("WHERE"));
    assertTrue(sparkSql.contains("1") && sparkSql.contains("2") && sparkSql.contains("3"));
  }

  @Test
  public void testIsNull() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" WHERE \"foo\".\"b\" IS NULL";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("IS NULL"));
  }

  @Test
  public void testIsNotNull() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" WHERE \"foo\".\"b\" IS NOT NULL";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("IS NOT NULL"));
  }

  @Test
  public void testAndOr() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" WHERE \"foo\".\"a\" > 1 AND \"foo\".\"a\" < 10";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("AND"));
  }

  @Test
  public void testNotOperator() {
    String trinoSql = "SELECT NOT FALSE FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("NOT"));
  }

  @Test
  public void testSubquery() {
    String trinoSql = "SELECT * FROM (SELECT \"foo\".\"a\" FROM \"default\".\"foo\") AS \"t\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("SELECT"));
    assertTrue(sparkSql.contains("FROM"));
  }

  @Test
  public void testLimit() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" LIMIT 10";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("LIMIT 10"));
  }

  @Test
  public void testDistinct() {
    String trinoSql = "SELECT DISTINCT \"foo\".\"a\" FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    // DISTINCT may be preserved or converted to GROUP BY
    assertTrue(sparkSql.contains("DISTINCT") || sparkSql.contains("GROUP BY"));
  }

  @Test
  public void testCoralSparkAccess() {
    // Test that we can access the full CoralSpark object for metadata
    String trinoSql = "SELECT * FROM \"default\".\"foo\"";
    CoralSpark coralSpark = getTrinoToSparkConverter().getCoralSpark(trinoSql);

    assertNotNull(coralSpark);
    assertNotNull(coralSpark.getSparkSql());
    assertNotNull(coralSpark.getBaseTables());
    assertTrue(coralSpark.getBaseTables().contains("default.foo"));
  }

  // ==================== V2 Function Tests ====================

  // String Functions

  @Test
  public void testSubstr() {
    String trinoSql = "SELECT substr(\"foo\".\"b\", 1, 5) FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("substr") || sparkSql.toLowerCase().contains("substring"));
  }

  @Test
  public void testSubstring() {
    String trinoSql = "SELECT substring(\"foo\".\"b\", 1, 3) FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("substr") || sparkSql.toLowerCase().contains("substring"));
  }

  @Test
  public void testConcat() {
    String trinoSql = "SELECT concat(\"foo\".\"b\", '-suffix') FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("concat"));
  }

  @Test
  public void testConcatMultiple() {
    String trinoSql = "SELECT concat(\"foo\".\"b\", '-', 'test') FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("concat"));
  }

  @Test
  public void testLower() {
    String trinoSql = "SELECT lower(\"foo\".\"b\") FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("lower"));
  }

  @Test
  public void testUpper() {
    String trinoSql = "SELECT upper(\"foo\".\"b\") FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("upper"));
  }

  @Test
  public void testTrim() {
    String trinoSql = "SELECT trim(\"foo\".\"b\") FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("trim"));
  }

  @Test
  public void testLength() {
    String trinoSql = "SELECT length(\"foo\".\"b\") FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("length") || sparkSql.toLowerCase().contains("char_length"));
  }

  @Test
  public void testReplace() {
    String trinoSql = "SELECT replace(\"foo\".\"b\", 'old', 'new') FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("replace"));
  }

  // JSON Functions

  @Test
  public void testJsonExtract() {
    String trinoSql = "SELECT json_extract(\"foo\".\"b\", '$.name') FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    // json_extract should be converted to get_json_object
    assertTrue(sparkSql.toLowerCase().contains("get_json_object"));
  }

  @Test
  public void testJsonExtractScalar() {
    String trinoSql = "SELECT json_extract_scalar(\"foo\".\"b\", '$.id') FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    // json_extract_scalar should be converted to get_json_object
    assertTrue(sparkSql.toLowerCase().contains("get_json_object"));
  }

  // Note: regexp_like and coalesce transformations require additional type coercion handling
  // These functions work in standard SQL and are handled by Calcite directly
  // Tests for these are deferred to future iterations

  // Conditional Functions

  @Test
  public void testIfFunction() {
    String trinoSql = "SELECT if(\"foo\".\"a\" > 5, 'yes', 'no') FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("if") || sparkSql.toLowerCase().contains("case"));
  }

  // Math Functions

  @Test
  public void testAbs() {
    String trinoSql = "SELECT abs(\"foo\".\"a\") FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("abs"));
  }

  @Test
  public void testRound() {
    String trinoSql = "SELECT round(\"foo\".\"c\") FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("round"));
  }

  @Test
  public void testRoundWithScale() {
    String trinoSql = "SELECT round(\"foo\".\"c\", 2) FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.toLowerCase().contains("round"));
  }

  // Additional Join Types

  @Test
  public void testRightJoin() {
    String trinoSql = "SELECT \"foo\".\"a\", \"bar\".\"x\" "
        + "FROM \"default\".\"foo\" AS \"foo\" "
        + "RIGHT JOIN \"default\".\"bar\" AS \"bar\" ON \"foo\".\"a\" = \"bar\".\"x\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("RIGHT JOIN"));
  }

  @Test
  public void testFullOuterJoin() {
    String trinoSql = "SELECT \"foo\".\"a\", \"bar\".\"x\" "
        + "FROM \"default\".\"foo\" AS \"foo\" "
        + "FULL OUTER JOIN \"default\".\"bar\" AS \"bar\" ON \"foo\".\"a\" = \"bar\".\"x\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("FULL JOIN") || sparkSql.contains("FULL OUTER JOIN"));
  }

  // Note: CROSS JOIN causes ClassCastException in CoralSqlNodeToSparkSqlNodeConverter
  // This is a known limitation in the Coral-Spark module that needs to be addressed separately

  // Complex Expressions

  @Test
  public void testCaseWhen() {
    String trinoSql = "SELECT CASE WHEN \"foo\".\"a\" > 10 THEN 'big' ELSE 'small' END FROM \"default\".\"foo\"";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("CASE"));
    assertTrue(sparkSql.contains("WHEN"));
  }

  @Test
  public void testBetween() {
    String trinoSql = "SELECT \"foo\".\"a\" FROM \"default\".\"foo\" WHERE \"foo\".\"a\" BETWEEN 5 AND 10";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("BETWEEN") || (sparkSql.contains(">= 5") && sparkSql.contains("<= 10")));
  }

  @Test
  public void testLike() {
    String trinoSql = "SELECT \"foo\".\"b\" FROM \"default\".\"foo\" WHERE \"foo\".\"b\" LIKE 'test%'";
    String sparkSql = getTrinoToSparkConverter().toSparkSql(trinoSql);

    assertNotNull(sparkSql);
    assertTrue(sparkSql.contains("LIKE"));
  }
}
