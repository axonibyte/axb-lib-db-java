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

public class Comparison {

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
    NOT_IN(" NOT IN ?");
    
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
  
}
