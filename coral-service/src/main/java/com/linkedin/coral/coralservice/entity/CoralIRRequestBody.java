/**
 * Copyright 2022-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.entity;


public class CoralIRRequestBody {
  private String sourceLanguage;
  private String query;

  public String getSourceLanguage() {
    return sourceLanguage;
  }

  public String getQuery() {
    return query;
  }
}
