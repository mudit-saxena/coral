/**
 * Copyright 2017-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.trino2rel;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import com.linkedin.coral.common.functions.FunctionReturnTypes;

import static com.linkedin.coral.trino.trino2rel.Trino2CoralOperatorTransformerMapUtils.*;
import static org.apache.calcite.sql.type.OperandTypes.*;


public class Trino2CoralOperatorTransformerMap {
  private Trino2CoralOperatorTransformerMap() {
  }

  public static final Map<String, OperatorTransformer> TRANSFORMER_MAP = new HashMap<>();

  static {
    // String functions
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("strpos", ReturnTypes.INTEGER, STRING_STRING), 2,
        "instr");

    // Note: regexp_like -> rlike transformation requires additional type coercion handling
    // This will be addressed in a future iteration

    // substr/substring with 2 args: substr(string, start) -> substr(string, start)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("substr", ReturnTypes.VARCHAR_2000, family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)), 2,
        "substr");
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("substring", ReturnTypes.VARCHAR_2000, family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)), 2,
        "substr");

    // substr/substring with 3 args: substr(string, start, length) -> substr(string, start, length)
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("substr", ReturnTypes.VARCHAR_2000,
        family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)), 3, "substr");
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("substring", ReturnTypes.VARCHAR_2000,
        family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)), 3, "substr");

    // JSON functions
    // json_extract(json, path) -> get_json_object(json, path)
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("json_extract", ReturnTypes.VARCHAR_2000, STRING_STRING),
        2, "get_json_object");

    // json_extract_scalar(json, path) -> get_json_object(json, path)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("json_extract_scalar", ReturnTypes.VARCHAR_2000, STRING_STRING), 2, "get_json_object");

    // Date/Time functions
    // date_format(timestamp, format) -> date_format(timestamp, format)
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("date_format", ReturnTypes.VARCHAR_2000,
        family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING)), 2, "date_format");

    // from_unixtime with 1 arg: from_unixtime(seconds) -> from_unixtime(seconds)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("from_unixtime", FunctionReturnTypes.TIMESTAMP, family(SqlTypeFamily.NUMERIC)), 1,
        "from_unixtime");

    // Concat functions - 2 args
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("concat", ReturnTypes.VARCHAR_2000, STRING_STRING), 2,
        "concat");

    // Concat functions - 3 args
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("concat", ReturnTypes.VARCHAR_2000, family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
        3, "concat");

    // Concat functions - 4 args
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("concat", ReturnTypes.VARCHAR_2000,
        family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)), 4, "concat");

    // Conditional functions
    // Note: coalesce is standard SQL and handled by Calcite directly - no transformation needed

    // if(condition, true_value, false_value) - 3 args
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("if", ReturnTypes.ARG1_NULLABLE,
        family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY)), 3, "if");

    // replace(string, search, replacement) -> replace(string, search, replacement)
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("replace", ReturnTypes.VARCHAR_2000,
        family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)), 3, "replace");

    // lower(string) -> lower(string)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("lower", ReturnTypes.VARCHAR_2000, family(SqlTypeFamily.STRING)), 1, "lower");

    // upper(string) -> upper(string)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("upper", ReturnTypes.VARCHAR_2000, family(SqlTypeFamily.STRING)), 1, "upper");

    // trim(string) -> trim(string)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("trim", ReturnTypes.VARCHAR_2000, family(SqlTypeFamily.STRING)), 1, "trim");

    // length(string) -> length(string)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("length", ReturnTypes.INTEGER, family(SqlTypeFamily.STRING)), 1, "length");

    // split(string, delimiter) -> split(string, delimiter)
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("split", ReturnTypes.ARG0, STRING_STRING), 2, "split");

    // abs(number) -> abs(number)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("abs", ReturnTypes.ARG0, family(SqlTypeFamily.NUMERIC)), 1, "abs");

    // round(number) -> round(number) - 1 arg
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("round", ReturnTypes.ARG0_NULLABLE, family(SqlTypeFamily.NUMERIC)), 1, "round");

    // round(number, scale) -> round(number, scale) - 2 args
    createTransformerMapEntry(TRANSFORMER_MAP, createOperator("round", ReturnTypes.ARG0_NULLABLE,
        family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER)), 2, "round");

    // Date arithmetic functions with operand reordering
    // Trino: date_add(unit, value, date) -> Coral: date_add(date, value)
    // Reorder: take arg 3 (date), then arg 2 (value), drop arg 1 (unit)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("date_add", ReturnTypes.DATE,
            family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME)),
        3, "date_add", "[{\"input\": 3}, {\"input\": 2}]", null);

    // Trino: date_diff(unit, date1, date2) -> Coral: datediff(date2, date1)
    // Reorder: take arg 3 (date2), then arg 2 (date1), drop arg 1 (unit)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("date_diff", ReturnTypes.INTEGER,
            family(SqlTypeFamily.STRING, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME)),
        3, "datediff", "[{\"input\": 3}, {\"input\": 2}]", null);

    // Array functions
    // cardinality(array) -> size(array)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("cardinality", ReturnTypes.INTEGER, family(SqlTypeFamily.ARRAY)), 1, "size");

    // Aggregation functions
    // array_agg(col) -> collect_list(col)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("array_agg", FunctionReturnTypes.ARRAY_OF_ARG0_TYPE, family(SqlTypeFamily.ANY)), 1,
        "collect_list");

    // Regex functions
    // regexp_like(str, pattern) -> rlike(str, pattern)
    createTransformerMapEntry(TRANSFORMER_MAP,
        createOperator("regexp_like", ReturnTypes.BOOLEAN, STRING_STRING), 2, "rlike");
  }

  /**
   * Gets TrinoCalciteOperatorTransformer for a given Trino SQL Operator.
   *
   * @param trinoOpName Name of Trino SQL operator
   * @param numOperands Number of operands
   * @return {@link OperatorTransformer} object
   */
  public static OperatorTransformer getOperatorTransformer(String trinoOpName, int numOperands) {
    return TRANSFORMER_MAP.get(getKey(trinoOpName, numOperands));
  }
}
