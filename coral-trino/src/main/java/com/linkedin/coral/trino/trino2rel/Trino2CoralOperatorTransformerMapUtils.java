/**
 * Copyright 2017-2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.trino2rel;

import java.util.Map;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.Optionality;


public class Trino2CoralOperatorTransformerMapUtils {

  private Trino2CoralOperatorTransformerMapUtils() {
  }

  /**
   * Creates a mapping for Trino SqlOperator name to TrinoCalciteOperatorTransformer.
   *
   * @param transformerMap Map to store the result
   * @param trinoOp Trino SQL operator
   * @param numOperands Number of operands
   * @param calciteOperatorName Name of Calcite Operator
   */
  static void createTransformerMapEntry(Map<String, OperatorTransformer> transformerMap, SqlOperator trinoOp,
      int numOperands, String calciteOperatorName) {
    createTransformerMapEntry(transformerMap, trinoOp, numOperands, calciteOperatorName, null, null);
  }

  /**
   * Creates a mapping from Trino SqlOperator name to Calcite Operator with Calcite Operator name, operands transformer, and result transformers.
   * To construct Calcite SqlOperator from Calcite Operator name, this method reuses the return type inference from trinoOp,
   * assuming equivalence.
   *
   * @param transformerMap Map to store the result
   * @param trinoOp Trino SQL operator
   * @param numOperands Number of operands
   * @param calciteOperatorName Name of Calcite Operator
   * @param operandTransformer Operand transformers, null for identity transformation
   * @param resultTransformer Result transformer, null for identity transformation
   */
  static void createTransformerMapEntry(Map<String, OperatorTransformer> transformerMap, SqlOperator trinoOp,
      int numOperands, String calciteOperatorName, String operandTransformer, String resultTransformer) {
    createTransformerMapEntry(transformerMap, trinoOp, numOperands,
        createOperator(calciteOperatorName, trinoOp.getReturnTypeInference(), trinoOp.getOperandTypeChecker()),
        operandTransformer, resultTransformer);
  }

  /**
   * Creates a mapping from Trino SqlOperator name to Calcite UDF with Calcite SqlOperator, operands transformer, and result transformers.
   *
   * @param transformerMap Map to store the result
   * @param trinoOp Trino SQL operator
   * @param numOperands Number of operands
   * @param calciteSqlOperator The Calcite Sql Operator that is used as the target operator in the map
   * @param operandTransformer Operand transformers, null for identity transformation
   * @param resultTransformer Result transformer, null for identity transformation
   */
  static void createTransformerMapEntry(Map<String, OperatorTransformer> transformerMap, SqlOperator trinoOp,
      int numOperands, SqlOperator calciteSqlOperator, String operandTransformer, String resultTransformer) {

    transformerMap.put(getKey(trinoOp.getName(), numOperands),
        OperatorTransformer.of(trinoOp.getName(), calciteSqlOperator, operandTransformer, resultTransformer, null));
  }

  static SqlOperator createOperator(String functionName, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    return new SqlUserDefinedFunction(new SqlIdentifier(functionName, SqlParserPos.ZERO), returnTypeInference, null,
        operandTypeChecker, null, null);
  }

  /**
   * Creates an aggregate function operator.
   * This is used for functions like array_agg that need to be recognized as aggregate functions
   * by the SQL validator in GROUP BY contexts.
   *
   * @param functionName Name of the aggregate function
   * @param returnTypeInference Return type inference
   * @param operandTypeChecker Operand type checker
   * @return SqlAggFunction representing the aggregate function
   */
  static SqlAggFunction createAggFunction(String functionName, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    return new SqlAggFunction(functionName, new SqlIdentifier(functionName, SqlParserPos.ZERO), SqlKind.OTHER_FUNCTION,
        returnTypeInference, null, operandTypeChecker, SqlFunctionCategory.USER_DEFINED_FUNCTION, false, false,
        Optionality.FORBIDDEN) {
    };
  }

  /**
   * Creates a mapping for a Trino aggregate function to its Calcite equivalent.
   *
   * @param transformerMap Map to store the result
   * @param trinoOpName Name of the Trino aggregate function
   * @param returnTypeInference Return type inference for the function
   * @param operandTypeChecker Operand type checker for the function
   * @param numOperands Number of operands
   * @param calciteOperatorName Name of the target Calcite operator
   */
  static void createAggTransformerMapEntry(Map<String, OperatorTransformer> transformerMap, String trinoOpName,
      SqlReturnTypeInference returnTypeInference, SqlOperandTypeChecker operandTypeChecker, int numOperands,
      String calciteOperatorName) {
    SqlAggFunction trinoAggOp = createAggFunction(trinoOpName, returnTypeInference, operandTypeChecker);
    SqlAggFunction calciteAggOp = createAggFunction(calciteOperatorName, returnTypeInference, operandTypeChecker);
    transformerMap.put(getKey(trinoOpName, numOperands),
        OperatorTransformer.of(trinoOpName, calciteAggOp, null, null, null));
  }

  static String getKey(String trinoOpName, int numOperands) {
    return trinoOpName + "_" + numOperands;
  }
}
