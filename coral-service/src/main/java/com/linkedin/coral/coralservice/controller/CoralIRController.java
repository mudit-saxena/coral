/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.controller;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.linkedin.coral.coralservice.entity.CoralIRRequestBody;
import com.linkedin.coral.coralservice.entity.CoralIRResponseBody;
import com.linkedin.coral.hive.hive2rel.HiveToRelConverter;
import com.linkedin.coral.trino.trino2rel.TrinoToRelConverter;

import static com.linkedin.coral.coralservice.utils.CommonUtils.*;
import static com.linkedin.coral.coralservice.utils.CoralProvider.*;


@RestController
@RequestMapping("/api/coral-ir")
@CrossOrigin(origins = CORAL_SERVICE_FRONTEND_URL)
public class CoralIRController {

  @PostMapping("/text")
  public ResponseEntity getCoralIRText(@RequestBody CoralIRRequestBody coralIRRequestBody) {
    final String sourceLanguage = coralIRRequestBody.getSourceLanguage();
    final String query = coralIRRequestBody.getQuery();

    if (!isValidSourceLanguage(sourceLanguage)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
          "Currently, only Hive, Spark, and Trino SQL are supported as source languages for Coral IR.\n");
    }

    String relNodeText;
    try {
      RelNode relNode = getRelNode(query, sourceLanguage);
      relNodeText = RelOptUtil.toString(relNode);
    } catch (Throwable t) {
      t.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(t.getMessage());
    }

    CoralIRResponseBody responseBody = new CoralIRResponseBody();
    responseBody.setRelNodeText(relNodeText);

    return ResponseEntity.status(HttpStatus.OK).body(responseBody);
  }

  private RelNode getRelNode(String query, String sourceLanguage) {
    RelNode relNode = null;
    if (sourceLanguage.equalsIgnoreCase("trino")) {
      relNode = new TrinoToRelConverter(hiveMetastoreClient).convertSql(query);
    } else if (sourceLanguage.equalsIgnoreCase("hive") || sourceLanguage.equalsIgnoreCase("spark")) {
      relNode = new HiveToRelConverter(hiveMetastoreClient).convertSql(query);
    }
    return relNode;
  }
}
