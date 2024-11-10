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
 * A stop-gap measure to replace individual arguments with functions of variable
 * order just prior to building the statement string.
 *
 * @author Caleb L. Power <cpower@axonibyte.com>
 */
public class Wrapper {

  private int position; // 1-index'd position
  private int argCount; // the number of comma-delimited "?" symbols in the function
  private String fn; // the function, no parentheses

  /**
   * Instantiates a wrapper.
   *
   * @param position the current argument position to target (indexed by 1)
   * @param fn the name of the function (without parentheses) e.g. "UNHEX"
   */
  public Wrapper(int position, String fn) {
    this(position, fn, 1);
  }

  /**
   * Instantiates a wrapper.
   *
   * @param position the current argument position to target (indexed by 1)
   * @param fn the name of the function (without parentheses) e.g. "UNHEX"
   * @param the function order (e.g. 2 for "FOO(?, ?)")
   */
  public Wrapper(int position, String fn, int argCount) {
    this.position = position;
    this.fn = fn;
    this.argCount = argCount;
  }

  /**
   * Retrieves the position associated with this wrapper.
   *
   * @return the position
   */
  public int getPosition() {
    return position;
  }

  /**
   * {@inheritDoc}
   */
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < argCount; i++) {
      if(!sb.isEmpty())
        sb.append(", ");
      sb.append("?");
    }
    return String.format(
        "%1$s(%2$s)",
        fn.toUpperCase(),
        sb.toString());
  }
  
}
