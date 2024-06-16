/*
 * Copyright (c) 2022-2024 Axonibyte Innovations, LLC. All rights reserved.
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

import java.util.UUID;

import com.axonibyte.lib.db.SQLBuilder.Comparison;
import com.axonibyte.lib.db.SQLBuilder.Order;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * A set of tests for {@link SQLBuilder}.
 *
 * @author Caleb L. Power <cpower@axonibyte.com>
 */
public class SQLBuilderTest {
  
  /**
   * Tests {@link SQLBuilder#select(String, Object...)} to ensure
   * that a SELECT statement referencing a single column can be
   * properly generated.
   */
  @Test public void testSelect_singleColumn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo");
    Assert.assertEquals(sqlBuilder.toString(), "SELECT column_foo FROM my_table");
  }

  /**
   * Tests {@link SQLBuilder#select(String, Object...)} to ensure
   * that a SELECT statement referencing multiple columns can be
   * properly generated.
   */
  @Test public void testSelect_multipleColumns() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo", "column_bar", "column_baz");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo, column_bar, column_baz FROM my_table");
  }

  /**
   * Tests {@link SQLBuilder#select(String)} and {@link SQLBuilder#column(Object)}
   * to ensure that columns can be added to a SELECT statement after the fact.
   */
  @Test public void testSelect_additionalColumn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table")
        .column("column_foo")
        .column("column_bar");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo, column_bar FROM my_table");
  }

  /**
   * Primarily tests {@link SQLBuilder#select(String)} and
   * {@link SQLBuilder#where(String)} to ensure that a WHERE clause
   * can be added to a SELECT statement.
   */
  @Test public void testSelect_singleWhereExact() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar = ?");
  }

  /**
   * Primarily tests {@link SQLBuilder#select(String, Object...)}
   * and {@link SQLBuilder#where(Object...)} to ensure that boolean
   * AND can be used within a WHERE clause.
   */
  @Test public void testSelect_multipleWhereExact() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar", "column_baz", "column_qux");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar = ? AND column_baz = ? AND column_qux = ?");
  }

  /**
   * Tests {@link SQLBuilder#where(String, Comparison)} to ensure that
   * a LIKE modifier can be used in a WHERE clause.
   */
  @Test public void testSelect_singleWhereLike() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar", Comparison.LIKE);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar LIKE ?");
  }

  /**
   * Tests {@link SQLBuilder#where(String, Comparison)} several times in
   * quick successtion to ensure that the AND conjunction can be used
   * more than once after the fact.
   */
  @Test public void testSelect_multipleWhereLike() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar", Comparison.LIKE)
        .where("column_baz", Comparison.LIKE)
        .where("column_qux", Comparison.LIKE);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar LIKE ? AND column_baz LIKE ? AND column_qux LIKE ?");
  }

  /**
   * Tests {@link SQLBuilder#limit(int)} to ensure that a LIMIT
   * modifier can be added to a SELECT statement.
   */
  @Test public void testSelect_limit() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar")
        .limit(69);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar = ? LIMIT 69");
  }

  /**
   * Tests {@link SQLBuilder#limit(int, int)} to ensure that both
   * LIMIT and OFFSET modifiers can be added to a SELECT statement.
   */
  @Test public void testSelect_limitOffset() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar")
        .limit(69, 420);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar = ? LIMIT 69 OFFSET 420");
  }

  /**
   * Tests {@link SQLBuilder#where(String, Object)} to ensure that
   * it is possible to mix WHERE clauses using LIKE and WHERE clauses
   * using an equality symbol.
   */
  @Test public void testSelect_whereMixed() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_bar", Comparison.EQUAL_TO)
        .where("column_baz", Comparison.LIKE)
        .where("column_qux", Comparison.EQUAL_TO);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_bar = ? AND column_baz LIKE ? AND column_qux = ?");
  }

  /**
   * Tests {@link SQLBuilder#and()} and {@link SQLBuilder#or()} to
   * ensure that WHERE clauses can use a variety of conjunctions.
   */
  @Test public void testWhere_andOrMixed() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_foo")
        .or()
        .where("column_bar", "column_baz")
        .and()
        .where("column_qax");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE (column_foo = ? OR column_bar = ? OR column_baz = ?) AND column_qax = ?");
  }

  /**
   * Tests {@link SQLBuilder#or()} to ensure that parantheses are
   * terminated at the end of a statement.
   */
  @Test public void testWhere_orTerminating() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .where("column_foo", "column_bar")
        .or()
        .where("column_baz", "column_qaz");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table WHERE column_foo = ? AND (column_bar = ? OR column_baz = ? OR column_qaz = ?)");
  }

  /**
   * Tests {@link SQLBuilder#fn(String, String, Object...)} to ensure that custom
   * functions can be utilized.
   */
  @Test public void testFn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table")
        .fn("FOO", "foo_alias", "bar", "baz");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT FOO(bar, baz) AS foo_alias FROM my_table");
  }
  
  /**
   * Tests {@link SQLBuilder#count(Object)} to ensure that
   * entries can be counted via SELECT statement.
   */
  @Test public void testCount() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table")
        .count("*", "count_foo");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT COUNT(*) AS count_foo FROM my_table");
  }

  /**
   * Tests {@link SQLBuilder#group(Object...)} to ensure that results can be
   * grouped by column.
   */
  @Test public void testGroup() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .group("column_foo", "column_bar");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table GROUP BY column_foo, column_bar");
  }

  /**
   * Tests {@link SQLBuilder#order(Object, Order)} to ensure that
   * a query can be built that arranges results in ascending order.
   */
  @Test public void testOrder_asc() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .order("column_bar", SQLBuilder.Order.ASC);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table ORDER BY column_bar ASC");
  }

  /**
   * Tests {@link SQLBuilder#order(Object, Order)} to ensure that
   * a query can be built that arranges results in descending order.
   */
  @Test public void testOrder_desc() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .order("column_bar", SQLBuilder.Order.DESC);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table ORDER BY column_bar DESC");
  }

  /**
   * Tests both {@link SQLBuilder#group(Object...)} and
   * {@link SQLBuilder#order(String, Order)} to ensure that the transition
   * between the two clauses is smooth.
   */
  @Test public void testGroupAndOrder() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "column_foo")
        .group("column_foo", "column_bar")
        .order("column_baz", SQLBuilder.Order.ASC)
        .order("column_bar", SQLBuilder.Order.DESC);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT column_foo FROM my_table GROUP BY column_foo, column_bar ORDER BY column_baz ASC, column_bar DESC");
  }

  /**
   * Tests {@link SQLBuilder#update(String, Object...)} to ensure
   * that an UPDATE statement with a single column can be generated.
   */
  @Test public void testUpdate_singleColumn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .update("my_table", "column_foo");
    Assert.assertEquals(sqlBuilder.toString(), "UPDATE my_table SET column_foo = ?");
  }

  /**
   * Tests {@link SQLBuilder#update(String, Object...)} to ensure
   * that an UPDATE statement with multiple columns can be generated.
   */
  @Test public void testUpdate_multipleColumns() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .update("my_table", "column_foo", "column_bar", "column_baz");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "UPDATE my_table SET column_foo = ?, column_bar = ?, column_baz = ?");
  }

  /**
   * Tests {@link SQLBuilder#insert(String, Object...)} to ensure
   * that an INSERT statement with a single column can be generated.
   */
  @Test public void testInsert_singleColumn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .insert("my_table", "column_foo");
    Assert.assertEquals(sqlBuilder.toString(), "INSERT INTO my_table (column_foo) VALUES (?)");
  }

  /**
   * Tests {@link SQLBuilder#insert(String, Object...)} to ensure
   * that an INSERT statement with multiple columns can be generated.
   */
  @Test public void testInsert_multipleColumns() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .insert("my_table", "column_foo", "column_bar", "column_baz");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "INSERT INTO my_table (column_foo, column_bar, column_baz) VALUES (?, ?, ?)");
  }

  /**
   * Tests {@link SQLBuilder#delete(String)} to ensure that
   * a DELETE statement can be generated.
   */
  @Test public void testDelete_allRows() {
    SQLBuilder sqlBuilder = new SQLBuilder().delete("my_table");
    Assert.assertEquals(sqlBuilder.toString(), "DELETE FROM my_table");
  }

  /**
   * Tests {@link SQLBuilder#delete(String)} and {@link SQLBuilder#where(String, Comparison)}
   * to ensure that a statement for deleting a set of records from the database
   * can be generated.
   */
  @Test public void testDelete_whereClause() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .delete("my_table")
        .where("column_foo", Comparison.EQUAL_TO);
    Assert.assertEquals(sqlBuilder.toString(), "DELETE FROM my_table WHERE column_foo = ?");
  }

  /**
   * Tests {@link SQLBuilder#replace(String, Object...)} to ensure that
   * a REPLACE statement can be generated with a single column.
   */
  @Test public void testReplace_singleColumn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .replace("my_table", "column_foo");
    Assert.assertEquals(sqlBuilder.toString(), "REPLACE INTO my_table SET column_foo = ?");
  }

  /**
   * Tests {@link SQLBuilder#replace(String, Object...)} to ensure that
   * a REPLACE statement can be generated with multiple columns.
   */
  @Test public void testReplace_multipleColumns() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .replace("my_table", "column_foo", "column_bar", "column_baz");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "REPLACE INTO my_table SET column_foo = ?, column_bar = ?, column_baz = ?");
  }

  /**
   * Tests {@link SQLBuilder#join(Join, String, String, String, String, Comparison)}
   * to ensure that joins can be constructed properly.
   */
  @Test public void testJoin() {
    SQLBuilder sqlBuilder = new SQLBuilder()
      .select(
          "my_table",
          "a.foo",
          "a.bar",
          "b.baz")
      .tableAlias("a")
      .join(
          SQLBuilder.Join.INNER,
          "my_other_table",
          "b",
          "a.xyzzy",
          "b.yeet",
          Comparison.EQUAL_TO);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT a.foo, a.bar, b.baz FROM my_table a INNER JOIN my_other_table b ON a.xyzzy = b.yeet");
  }

  /**
   * Tests {@link SQLBuilder#select(SQLBuilder, String)} to ensure that selections
   * with subqueried tables can be constructed properly.
   */
  @Test public void testSelectSubquery() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select(
            new SQLBuilder()
                .select("table_inner", "foo", "bar"),
            "a",
            "baz",
            "bux");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT baz, bux FROM ( SELECT foo, bar FROM table_inner ) a");
  }

  /**
   * Tests {@link SQLBuilder#column(SQLBuilder, String)} to ensure that columns
   * with subqueries can be incorporated into the statement.
   */
  @Test public void testColumnSubquery() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("foo", "bar", "baz")
        .column(
            new SQLBuilder()
                .select("table_inner", "xyzzy", "yeet"),
            "a");
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT bar, baz, ( SELECT xyzzy, yeet FROM table_inner ) AS a FROM foo");
  }

  /**
   * Tests {@link SQLBuilder#join(Join, SQLBuilder, String, Object, Object, Comparison)}
   * to ensure that complicated joins with subqueries can be built.
   */
  @Test public void testJoinSubquery() {
    SQLBuilder sqlBuilder = new SQLBuilder()
      .select(
          "my_table",
          "a.foo",
          "a.bar",
          "b.baz")
      .tableAlias("a")
      .join(
          SQLBuilder.Join.INNER,
          new SQLBuilder().select("inner", "i_alpha", "i_beta"),
          "b",
          new SQLBuilder().select("left", "l_alpha", "l_beta"),
          new SQLBuilder().select("right", "r_alpha", "r_beta"),
          Comparison.EQUAL_TO);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT a.foo, a.bar, b.baz FROM my_table a INNER JOIN ( SELECT i_alpha, i_beta FROM inner ) b ON ( SELECT l_alpha, l_beta FROM left ) = ( SELECT r_alpha, r_beta FROM right )");
  }

  /**
   * Tests {@link SQLBuilder#whereIn(Object, int)} to make sure that WHERE IN
   * clauses can be built.
   */
  @Test public void testWhereIn() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "foo", "bar")
        .whereIn("baz", false, 3);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT foo, bar FROM my_table WHERE baz IN (?, ?, ?)");
  }

  /**
   * Tests {@link SQLBuilder#whereIn(Object, int)} to make sure that WHERE IN
   * clauses can be built.
   */
  @Test public void testWhereIn_not() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "foo", "bar")
        .whereIn("baz", true, 3);
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT foo, bar FROM my_table WHERE baz NOT IN (?, ?, ?)");
  }

  /**
   * Tests {@link SQLBuilder#where(Object, Comparison, SQLBuilder)} to ensure
   * that subqueries can be added to WHERE clauses
   */
  @Test public void testWhereSubquery() {
    SQLBuilder sqlBuilder = new SQLBuilder()
        .select("my_table", "foo", "bar")
        .where(
            "baz",
            Comparison.GREATER_THAN,
            new SQLBuilder().select("inner").max("yeet"));
    Assert.assertEquals(
        sqlBuilder.toString(),
        "SELECT foo, bar FROM my_table WHERE baz > ( SELECT MAX(yeet) FROM inner )");
  }

  /**
   * Tests {@link SQLBuilder#testUUIDToBytes(UUID)} to ensure that,
   * given a valid UUID, a byte array will be returned with an underlying
   * datum matching that of the UUID.
   */
  @Test public void testUUIDToBytes() {
    final UUID testUUID = UUID.fromString("35f49d10-0813-4ad5-8f58-006aea46c3b0");
    final byte[] testBytes = new byte[] {
      (byte)0x35, (byte)0xf4, (byte)0x9d, (byte)0x10,
      (byte)0x08, (byte)0x13, (byte)0x4a, (byte)0xd5,
      (byte)0x8f, (byte)0x58, (byte)0x00, (byte)0x6a,
      (byte)0xea, (byte)0x46, (byte)0xc3, (byte)0xb0
    };
    byte[] resultBytes = SQLBuilder.uuidToBytes(testUUID);
    Assert.assertEquals(resultBytes.length, 16);
    for(int i = 0; i < 16; i++)
      Assert.assertEquals(
          resultBytes[i],
          testBytes[i],
          String.format("byte mismatch idx=%1$d", i));
  }

  /**
   * Tests {@link SQLBuilder#testBytesToUUID(byte[])} to ensure that,
   * given a valid byte array, a UUID will be returned with an underlying
   * datum matching that of the byte array.
   */
  @Test public void testBytesToUUID() {
    final UUID testUUID = UUID.fromString("35f49d10-0813-4ad5-8f58-006aea46c3b0");
    final byte[] testBytes = new byte[] {
      (byte)0x35, (byte)0xf4, (byte)0x9d, (byte)0x10,
      (byte)0x08, (byte)0x13, (byte)0x4a, (byte)0xd5,
      (byte)0x8f, (byte)0x58, (byte)0x00, (byte)0x6a,
      (byte)0xea, (byte)0x46, (byte)0xc3, (byte)0xb0
    };
    UUID resultUUID = SQLBuilder.bytesToUUID(testBytes);
    Assert.assertEquals(testUUID.compareTo(resultUUID), 0);
  }
  
}
