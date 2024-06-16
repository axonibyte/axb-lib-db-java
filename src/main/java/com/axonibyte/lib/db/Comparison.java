/*
 * Copyright (c) 2021-2024 Axonibyte Innovations, LLC. All rights reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.axonibyte.lib.db;

/**
 * Models a SQL comparison.
 *
 * @author Caleb L. Power <cpower@axonibyte.com>
 */
public class Comparison {

  private boolean or = false;

  /**
   * The operation to use when filtering values.
   *
   * @author Caleb L. Power <cpower@axonibyte.com>
   */
  public static enum ComparisonOp {
    
    /**
     * Substring searches.
     */
    LIKE(" LIKE ? "),
    
    /**
     * Needle in a haystack.
     */
    EQUAL_TO(" = ? "),
    
    /**
     * Get values less than the one provided.
     */
    LESS_THAN(" < ? "),
    
    /**
     * Get values less than or equal to the one provided.
     */
    LESS_THAN_OR_EQUAL_TO(" <= ? "),
    
    /**
     * Get values greater than the one provided.
     */
    GREATER_THAN(" > ? "),
    
    /**
     * Get values greater than or equal to the one provided.
     */
    GREATER_THAN_OR_EQUAL_TO(" >= ? "),
    
    /**
     * Get values other than the needle.
     */
    NOT_EQUAL_TO(" <> ? "),

    /**
     * Needle is null.
     */
    IS_NULL(" IS NULL "),

    /**
     * Needle is not null.
     */
    IS_NOT_NULL(" IS NOT NULL "),

    /**
     * Needle is in a collection.
     */
    IN(" IN ? "),

    /**
     * Needle is not in a collection.
     */
    NOT_IN(" NOT IN ? ");
    
    private String op = null;
    
    private ComparisonOp(String op) {
      this.op = op;
    }
    
    /**
     * Retrieves the operation associated with this comparison.
     * 
     * @return the operation and parameter
     */
    String getOp() {
      return op;
    }
    
  }

  private ComparisonOp op = null;
  private String left = null;
  private String right = null;

  /**
   * Instantiates a {@link Comparison}.
   *
   * Note that {@code right} can be {@code null} if the op would otherwise make
   * {@code right} a NOP. If specified in these cases, {@code right} is still
   * ignored.
   *
   * @param left the left side of the comparison
   * @param right the right side of the comparison
   * @param op the {@link ComparisonOp}
   */
  public Comparison(Object left, Object right, ComparisonOp op) {
    this.op = op;
    this.left = left instanceof SQLBuilder
        ? String.format(
            "( %1$s )",
            left.toString())
        : left.toString();
    this.right = null == right
        ? null
        : right instanceof SQLBuilder
            ? String.format(
                "( %1$s )",
                right.toString())
        : right.toString();
  }

  /**
   * Retrieves the comparison operator.
   *
   * @return the {@link ComparisonOp}
   */
  public ComparisonOp op() {
    return op;
  }

  /**
   * Retrieves the left side of the comparison.
   *
   * @return the {@link String} representation of the left side of the comparison
   */
  public String left() {
    return left;
  }

  /**
   * Retrieves the right side of the comparison.
   *
   * @return the {@link String} representation of the right side of the comparison
   */
  public String right() {
    return right;
  }

  /**
   * Use an OR conjunction in the next {@link Comparison}. (Really only useful
   * for JOIN conditions.
   *
   * @return this {@link Comparison} instance
   */
  public Comparison or() {
    this.or = true;
    return this;
  }

  /**
   * Determines whether or not to use the OR conjunction in the following
   * {@link Comparison}. If {@code false}, use AND instead (default).
   *
   * @return {@code true} iff we should use OR next time
   */
  public boolean isOr() {
    return or;
  }
  
}
