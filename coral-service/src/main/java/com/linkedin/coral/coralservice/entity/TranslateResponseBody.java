/**
 * Copyright 2022-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.entity;


public class TranslateResponseBody {
  private String originalQuery;
  private String translatedQuery;
  private String sourceLanguage;
  private String targetLanguage;

  public TranslateResponseBody() {
  }

  public String getOriginalQuery() {
    return originalQuery;
  }

  public void setOriginalQuery(String originalQuery) {
    this.originalQuery = originalQuery;
  }

  public String getTranslatedQuery() {
    return translatedQuery;
  }

  public void setTranslatedQuery(String translatedQuery) {
    this.translatedQuery = translatedQuery;
  }

  public String getSourceLanguage() {
    return sourceLanguage;
  }

  public void setSourceLanguage(String sourceLanguage) {
    this.sourceLanguage = sourceLanguage;
  }

  public String getTargetLanguage() {
    return targetLanguage;
  }

  public void setTargetLanguage(String targetLanguage) {
    this.targetLanguage = targetLanguage;
  }
}
