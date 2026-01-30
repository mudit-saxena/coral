/**
 * Copyright 2022-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.coralservice.entity;


public class CoralIRResponseBody {
  private String relNodeText;
  private String postRewriteRelNodeText;

  public CoralIRResponseBody() {
  }

  public String getRelNodeText() {
    return relNodeText;
  }

  public void setRelNodeText(String relNodeText) {
    this.relNodeText = relNodeText;
  }

  public String getPostRewriteRelNodeText() {
    return postRewriteRelNodeText;
  }

  public void setPostRewriteRelNodeText(String postRewriteRelNodeText) {
    this.postRewriteRelNodeText = postRewriteRelNodeText;
  }
}
