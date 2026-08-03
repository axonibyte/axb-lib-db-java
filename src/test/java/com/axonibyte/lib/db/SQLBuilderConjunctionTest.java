/*
 * Copyright (c) 2026 Axonibyte Innovations, LLC. All rights reserved.
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

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests parenthesisation of WHERE-clause conjunctions.
 *
 * <p>Every generated statement must be balanced, because an unbalanced parenthesis is
 * a syntax error that only surfaces when the query is actually executed.</p>
 *
 * @author Caleb L. Power &lt;cpower@axonibyte.com&gt;
 */
public class SQLBuilderConjunctionTest {

  private static void assertBalanced(String sql) {
    int depth = 0;
    for(char c : sql.toCharArray()) {
      if('(' == c) depth++;
      else if(')' == c) depth--;
      Assert.assertTrue(depth >= 0, "closing paren before opening paren in: " + sql);
    }
    Assert.assertEquals(depth, 0, "unbalanced parentheses in: " + sql);
  }

  @Test public void or_beforeAnyWhere_producesBalancedSQL() {
    // Previously emitted "WHERE column_foo = ? OR column_bar = ?)" -- a stray closing
    // parenthesis, because the close was emitted whenever the last filter sat in an OR
    // run regardless of whether an opening parenthesis had ever been written.
    String sql = new SQLBuilder()
        .select("my_table", "id")
        .or()
        .where("column_foo")
        .where("column_bar")
        .toString();

    assertBalanced(sql);
    Assert.assertEquals(
        sql,
        "SELECT id FROM my_table WHERE column_foo = ? OR column_bar = ?");
  }

  @Test public void or_beforeAnyWhere_withSingleFilter_producesBalancedSQL() {
    String sql = new SQLBuilder()
        .select("my_table", "id")
        .or()
        .where("column_foo")
        .toString();

    assertBalanced(sql);
    Assert.assertEquals(sql, "SELECT id FROM my_table WHERE column_foo = ?");
  }

  @Test public void or_afterOneWhere_groupsThatFilterIntoTheOrRun() {
    // Documented, deliberate behavior: or() is retroactive by exactly one filter. This
    // pins it so the surprise cannot silently change.
    String sql = new SQLBuilder()
        .select("my_table", "id")
        .where("account")
        .or()
        .where("type")
        .toString();

    assertBalanced(sql);
    Assert.assertEquals(
        sql,
        "SELECT id FROM my_table WHERE (account = ? OR type = ?)");
  }

  @Test public void or_afterTwoWheres_leavesTheFirstFilterOutsideTheGroup() {
    // The correct idiom for "scoped AND (alternatives)": scoping filters first, with
    // or() immediately preceding the last alternative.
    String sql = new SQLBuilder()
        .select("my_table", "id")
        .where("account")
        .where("type")
        .or()
        .where("type")
        .toString();

    assertBalanced(sql);
    Assert.assertEquals(
        sql,
        "SELECT id FROM my_table WHERE account = ? AND (type = ? OR type = ?)");
  }

  @Test public void orThenAnd_closesTheGroupBeforeResumingAnd() {
    String sql = new SQLBuilder()
        .select("my_table", "id")
        .where("a")
        .or()
        .where("b")
        .and()
        .where("c")
        .toString();

    assertBalanced(sql);
  }

  @Test public void allFilterCombinationsUpToFourTermsAreBalanced() {
    // Exhaustive over conjunction placement, since the parenthesisation logic is
    // index-driven and easy to get subtly wrong at the boundaries.
    String[] cols = { "a", "b", "c", "d" };
    for(int n = 1; n <= cols.length; n++) {
      for(int mask = 0; mask < (1 << (n + 1)); mask++) {
        var builder = new SQLBuilder().select("t", "id");
        for(int i = 0; i < n; i++) {
          if(0 != (mask & (1 << i))) builder.or();
          builder.where(cols[i]);
        }
        if(0 != (mask & (1 << n))) builder.or();
        assertBalanced(builder.toString());
      }
    }
  }

}
