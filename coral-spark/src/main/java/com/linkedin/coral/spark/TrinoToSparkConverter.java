/**
 * Copyright 2017-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark;

import org.apache.calcite.rel.RelNode;

import com.linkedin.coral.common.HiveMetastoreClient;
import com.linkedin.coral.trino.trino2rel.TrinoToRelConverter;

import static com.google.common.base.Preconditions.*;


/**
 * Converts Trino SQL to Spark SQL using Coral's intermediate representation (IR).
 *
 * This converter combines {@link TrinoToRelConverter} to parse Trino SQL into Calcite RelNode,
 * and {@link CoralSpark} to convert the RelNode into Spark-compatible SQL.
 *
 * Usage:
 * <pre>
 *   TrinoToSparkConverter converter = TrinoToSparkConverter.create(hiveMetastoreClient);
 *   String sparkSql = converter.toSparkSql(trinoSql);
 * </pre>
 */
public class TrinoToSparkConverter {

  private final TrinoToRelConverter trinoToRelConverter;
  private final HiveMetastoreClient hiveMetastoreClient;

  /**
   * Creates a TrinoToSparkConverter instance.
   *
   * @param hiveMetastoreClient client interface used to interact with the Hive Metastore service.
   * @return a new TrinoToSparkConverter instance
   */
  public static TrinoToSparkConverter create(HiveMetastoreClient hiveMetastoreClient) {
    checkNotNull(hiveMetastoreClient);
    TrinoToRelConverter trinoToRelConverter = new TrinoToRelConverter(hiveMetastoreClient);
    return new TrinoToSparkConverter(trinoToRelConverter, hiveMetastoreClient);
  }

  private TrinoToSparkConverter(TrinoToRelConverter trinoToRelConverter, HiveMetastoreClient hiveMetastoreClient) {
    this.trinoToRelConverter = trinoToRelConverter;
    this.hiveMetastoreClient = hiveMetastoreClient;
  }

  /**
   * Converts input Trino SQL to Spark SQL.
   *
   * @param trinoSql Trino SQL query string
   * @return Spark-compatible SQL string representing input Trino SQL
   */
  public String toSparkSql(String trinoSql) {
    RelNode relNode = trinoToRelConverter.convertSql(trinoSql);
    return toSparkSql(relNode);
  }

  /**
   * Converts input view definition to Spark SQL.
   *
   * @param dbName database name
   * @param viewName view name
   * @return Spark-compatible SQL matching input view definition
   */
  public String toSparkSql(String dbName, String viewName) {
    RelNode relNode = trinoToRelConverter.convertView(dbName, viewName);
    return toSparkSql(relNode);
  }

  /**
   * Converts a RelNode to Spark SQL.
   *
   * @param relNode Calcite RelNode representation
   * @return Spark-compatible SQL string
   */
  private String toSparkSql(RelNode relNode) {
    CoralSpark coralSpark = CoralSpark.create(relNode, hiveMetastoreClient);
    return coralSpark.getSparkSql();
  }

  /**
   * Gets the CoralSpark instance for a given Trino SQL.
   * This provides access to additional information like base tables and UDF info.
   *
   * @param trinoSql Trino SQL query string
   * @return CoralSpark instance containing Spark SQL and metadata
   */
  public CoralSpark getCoralSpark(String trinoSql) {
    RelNode relNode = trinoToRelConverter.convertSql(trinoSql);
    return CoralSpark.create(relNode, hiveMetastoreClient);
  }
}
