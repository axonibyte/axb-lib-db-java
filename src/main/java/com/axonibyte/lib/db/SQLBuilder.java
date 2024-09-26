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

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import com.axonibyte.lib.db.Comparison.ComparisonOp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a rudimentary SQL query. Note that variables still have to be added.
 * It's recommended that {@link java.sql.PreparedStatement} be utilized.
 * 
 * @author Caleb L. Power <cpower@axonibyte.com>
 */
public class SQLBuilder {
  
  private static enum Statement {
    INSERT, // create
    INSERT_IGNORE, // create (but ignore errors)
    SELECT, // read
    UPDATE, // update
    DELETE, // delete
    REPLACE, // replace
  }
  
  /**
   * The order in which records should be retrieved.
   * 
   * @author Caleb L. Power <cpower@axonibyte.com>
   */
  public static enum Order {
    
    /**
     * Ascending order.
     */
    ASC,
    
    /**
     * Descending order.
     */
    DESC;
  }
  
  /**
   * Denotes some kind of table join.
   */
  public static enum Join {
    
    /**
     * Denotes an inner join.
     */
    INNER,
    
    /**
     * Denotes an outer join.
     */
    OUTER,
    
    /**
     * Denotes a left join.
     */
    LEFT,
    
    /**
     * Denotes a right join.
     */
    RIGHT;
    
  }
  
  private int limit = 0;
  private int offset = 0;
  private Entry<String, String> table = null;
  private List<String> columns = new ArrayList<>(); // columns to retrieve/modify
  private List<Entry<String, String>> filters = new ArrayList<>(); // filters for the WHERE clause
  private List<String> groupBy = new ArrayList<>(); // columns to group by
  // joins = [ { JOIN, { { TABLE, ALIAS }, { LEFT, { RIGHT, COMPARISON } } } }, ... ]
  //private List<Entry<Join, Entry<Entry<String, String>, Entry<String, Entry<String, ComparisonOp>>>>> joins = new ArrayList<>();
  // joins = [ { JOIN, { { TABLE, ALIAS }, [ COMPARISON... ] } } ]
  private List<Entry<Join, Entry<Entry<String, String>, List<Comparison>>>> joins = new ArrayList<>();
  private Map<Integer, Boolean> conjunctions = new HashMap<>();
  private Map<String, Order> orderBy = new LinkedHashMap<>();
  private Statement statement = null;

  private final Logger logger = LoggerFactory.getLogger(SQLBuilder.class);
  
  /**
   * Begins SELECT statement with multiple columns.
   * 
   * @param table the name of the table from which data is retrieved
   * @param columns the columns to be selected
   * @return this SQLBuilder object
   */
  public SQLBuilder select(String table, Object... columns) {
    select(table);
    for(var column : columns) column(column);
    return this;
  }

  /**
   * Begins SELECT statement with multiple columns and a subquery instead of a table.
   *
   * @param subquery a subquery, the results of which will be queried
   * @param alias the returned subquery result alias
   * @param columns the columns to be selected
   * @return this SQLBuilder object
   */
  public SQLBuilder select(SQLBuilder subquery, String alias, Object... columns) {
    select(
        String.format(
            "( %1$s )",
            subquery.toString()),
        columns);
    tableAlias(alias);
    return this;
  }
  
  /**
   * Begins SELECT statement.
   * 
   * @param table the name of the table from which data is retrieved
   * @return this SQLBuilder object
   */
  public SQLBuilder select(String table) {
    this.statement = Statement.SELECT;
    this.table = new SimpleEntry<>(table, null);
    return this;
  }

  /**
   * Begins SELECT statement with a subquery instead of a table.
   *
   * @param subquery a subquery, the results of which will be queried
   * @param columns the columns to be selected
   * @return this SQLBuilder object
   */
  public SQLBuilder select(SQLBuilder subquery, String alias) {
    select(
        String.format(
            "( %1$s )",
            subquery.toString()));
    tableAlias(alias);
    return this;
  }
  
  /**
   * Begins UPDATE statement with multiple columns.
   * 
   * @param table the name of the table in which data will be altered
   * @param columns the columns that are to be altered
   * @return this SQLBuilder object
   */
  public SQLBuilder update(String table, Object... columns) {
    update(table);
    for(var column : columns) column(column);
    return this;
  }
  
  /**
   * Begins UPDATE statement.
   * 
   * @param table the name of the table in which data will be altered
   * @return this SQLBuilder object
   */
  public SQLBuilder update(String table) {
    this.statement = Statement.UPDATE;
    this.table = new SimpleEntry<>(table, null);
    return this;
  }
  
  /**
   * Begins INSERT statement with multiple columns.
   * 
   * @param table the table in which rows will be inserted
   * @param columns the columns to be inserted
   * @return this SQLBuilder object
   */
  public SQLBuilder insert(String table, Object... columns) {
    insert(table);
    for(var column : columns) column(column);
    return this;
  }
  
  /**
   * Begins INSERT statement.
   * 
   * @param table the table in which rows will be inserted
   * @return this SQLBuilder object
   */
  public SQLBuilder insert(String table) {
    this.statement = Statement.INSERT;
    this.table = new SimpleEntry<>(table, null);
    return this;
  }
  
  /**
   * Begins INSERT IGNORE statement with multiple columns.
   * 
   * @param table the table in which rows will be inserted
   * @param columns the columns to be inserted
   * @return this SQLBuilder object
   */
  public SQLBuilder insertIgnore(String table, Object... columns) {
    insertIgnore(table);
    for(var column : columns) column(column);
    return this;
  }
  
  /**
   * Begins INSERT IGNORE statement.
   * 
   * @param table the table in which rows will be inserted
   * @return this SQLBuilder object
   */
  public SQLBuilder insertIgnore(String table) {
    this.statement = Statement.INSERT_IGNORE;
    this.table = new SimpleEntry<>(table, null);
    return this;
  }
  
  /**
   * Begins DELETE statement.
   * 
   * @param table the table from which records are to be deleted
   * @return this SQLBuilder object
   */
  public SQLBuilder delete(String table) {
    this.statement = Statement.DELETE;
    this.table = new SimpleEntry<>(table, null);
    return this;
  }
  
  /**
   * Begins REPLACE statement with multiple columns.
   *
   * @param table the table in which records are to be replaced
   * @param columns the columns that are to be altered
   * @return this SQLBuilder object
   */
  public SQLBuilder replace(String table, Object... columns) {
    replace(table);
    for(var column : columns) column(column);
    return this;
  }
  
  /**
   * Begins REPLACE statement.
   *
   * @param table the table in which records are to be replaced
   * @return this SQLBuilder object
   */
  public SQLBuilder replace(String table) {
    this.statement = Statement.REPLACE;
    this.table = new SimpleEntry<>(table, null);
    return this;
  }
  
  /**
   * Sets the alias of the primary table for use in the query.
   *
   * @param alias the table alias
   * @return this SQLBuilder object
   */
  public SQLBuilder tableAlias(String alias) {
    this.table.setValue(alias);
    return this;
  }
  
  /**
   * Specifies an additional column for some statement.
   * 
   * @param column the name of the column in question
   * @return this SQLBuilder object
   */
  public SQLBuilder column(Object column) {
    columns.add(column.toString());
    return this;
  }

  /**
   * Specifies an additional column with an alias for some statement.
   *
   * @param column the name of the column in question
   * @param alias the alias of the returned column
   * @return this SQLBuilder object
   */
  public SQLBuilder column(Object column, String alias) {
    return column(
        String.format(
            "%1$s AS %2$s",
            column.toString(),
            alias));
  }

  /**
   * Specifies a subquery to be returned as a column with an alias.
   *
   * @param subquery the subquery for which results will be returned
   * @param alias the alias of the returned column
   * @return this SQLBuilder object
   */
  public SQLBuilder column(SQLBuilder subquery, String alias) {
    return column(
        String.format(
            "( %1$s )",
            subquery.toString()),
        alias);
  }

  /**
   * Specifies that some function should be used in a returned column.
   *
   * @param fn the name of the function (without parantheses)
   * @param args the function arguments
   * @return this SQLBuilder object
   */
  public SQLBuilder fn(String fn, Object... args) {
    StringBuilder sb = new StringBuilder();
    for(var arg : args) {
      if(!sb.isEmpty())
        sb.append(", ");
      sb.append(arg.toString());
    }
    
    return column(
        String.format(
            "%1$s(%2$s)",
            fn,
            sb.toString()));
  }

  /**
   * Specifies that some function should be used in a returned (aliased) column.
   *
   * @param fn the name of the function (without parantheses)
   * @param alias the alias of the returned column
   * @param args the function arguments
   * @return this SQLBuilder object
   */
  public SQLBuilder fn(String fn, String alias, Object... args) {
    StringBuilder sb = new StringBuilder();
    for(var arg : args) {
      if(!sb.isEmpty())
        sb.append(", ");
      sb.append(arg.toString());
    }
    
    return column(
        String.format(
            "%1$s(%2$s)",
            fn,
            sb.toString()),
        alias);
  }

  /**
   * Specifies that the AVG function should invoked on a column.
   *
   * @param column the name of the column or wildcard
   * @return this SQLBuilder object
   */
  public SQLBuilder avg(Object column) {
    return fn("AVG", column);
  }

  /**
   * Specifies that the AVG function should be voked on an aliased column.
   *
   * @param column the name of the column or wildcard
   * @param alias the column alias
   * @return this SQLBuilder object
   */
  public SQLBuilder avg(Object column, String alias) {
    return fn("AVG", alias, column);
  }
  
  /**
   * Specifies that the COUNT function should invoked on a column.
   *
   * @param column the name of the column or wildcard
   * @return this SQLBuilder object
   */
  public SQLBuilder count(Object column) {
    return fn("COUNT", column);
  }

  /**
   * Specifies that the COUNT function should be voked on an aliased column.
   *
   * @param column the name of the column or wildcard
   * @param alias the column alias
   * @return this SQLBuilder object
   */
  public SQLBuilder count(Object column, String alias) {
    return fn("COUNT", alias, column);
  }

  /**
   * Specifies that the DISTINCT function should invoked on a column.
   *
   * @param column the name of the column or wildcard
   * @return this SQLBuilder object
   */
  public SQLBuilder distinct(Object column) {
    return fn("DISTINCT", column);
  }

  /**
   * Specifies that the DISTINCT function should be voked on an aliased column.
   *
   * @param column the name of the column or wildcard
   * @param alias the column alias
   * @return this SQLBuilder object
   */
  public SQLBuilder distinct(Object column, String alias) {
    return fn("DISTINCT", alias, column);
  }

  /**
   * Specifies that the MAX function should invoked on a column.
   *
   * @param column the name of the column or wildcard
   * @return this SQLBuilder object
   */
  public SQLBuilder max(Object column) {
    return fn("MAX", column);
  }

  /**
   * Specifies that the MAX function should be voked on an aliased column.
   *
   * @param column the name of the column or wildcard
   * @param alias the column alias
   * @return this SQLBuilder object
   */
  public SQLBuilder max(Object column, String alias) {
    return fn("MAX", alias, column);
  }

  /**
   * Specifies that the MIN function should invoked on a column.
   *
   * @param column the name of the column or wildcard
   * @return this SQLBuilder object
   */
  public SQLBuilder min(Object column) {
    return fn("MIN", column);
  }

  /**
   * Specifies that the MIN function should be voked on an aliased column.
   *
   * @param column the name of the column or wildcard
   * @param alias the column alias
   * @return this SQLBuilder object
   */
  public SQLBuilder min(Object column, String alias) {
    return fn("MIN", alias, column);
  }
  
  /**
   * Specifies that the SUM function should invoked on a column.
   *
   * @param column the name of the column or wildcard
   * @return this SQLBuilder object
   */
  public SQLBuilder sum(Object column) {
    return fn("SUM", column);
  }

  /**
   * Specifies that the SUM function should be voked on an aliased column.
   *
   * @param column the name of the column or wildcard
   * @param alias the column alias
   * @return this SQLBuilder object
   */
  public SQLBuilder sum(Object column, String alias) {
    return fn("SUM", alias, column);
  }
  
  /**
   * Applies a join operation to the query.
   *
   * @param join the type of {@link Join} to be used
   * @param table the name of the table to be joined
   * @param alias the alias for the joining table
   * @param comparisons the comparisons used for this join
   * @return this SQLBuilder object
   */
  public SQLBuilder join(Join join, String table, String alias, Comparison... comparisons) {
    joins.add(
        new SimpleEntry<>(
            join,
            new SimpleEntry<>(
                new SimpleEntry<>(table, alias),
                Arrays.asList(comparisons))));
    return this;
  }

  /**
   * Applies a join operation to the query.
   *
   * @param join the type of {@link Join} to be used
   * @param subquery a subquery to be executed for the new side of the join operation
   * @param alias the alias for the joining table
   * @param comparison the comparisons used for this join
   * @return this SQLBuilder object
   */
  public SQLBuilder join(Join join, SQLBuilder subquery, String alias, Comparison... comparisons) {
    return join(
        join,
        String.format(
            "( %1$s )",
            subquery.toString()),
        alias,
        comparisons);
  }
  
  /**
   * Specifies that AND should be used from this point in the
   * WHERE clause.
   *
   * @return this SQLBuilder object
   */
  public SQLBuilder and() {
    conjunctions.put(filters.size(), false);
    return this;
  }
  
  /**
   * Specifies the beginning of a parenthized piece where
   * OR should be used in the WHERE clause.
   *
   * @return this SQLBuilder object
   */
  public SQLBuilder or() {
    conjunctions.put(filters.size(), true);
    return this;
  }
  
  /**
   * Adds WHERE clause with specified columns to statement.
   *
   * @param columns the columns that shall act as filters
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object... columns) {
    return where(ComparisonOp.EQUAL_TO, columns);
  }
  
  /**
   * Adds WHERE clause with specified columns to statement.
   *
   * @param comparison the {@link Comparison} to use
   * @param columns the columns that shall act as filters
   * @return this SQLBuilder object
   */
  public SQLBuilder where(ComparisonOp op, Object... columns) {
    for(var column : columns) where(column, op);
    return this;
  }
  
  /**
   * Adds a specific column to WHERE clause.
   *
   * @param column the column
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object column) {
    return where(column, ComparisonOp.EQUAL_TO);
  }
  
  /**
   * Adds a specific column to WHERE clause.
   *
   * Note that {@link ComparisonOp#IN} and {@link ComparisonOp#NOT_IN} are invalid
   * here--use {@link SQLBuilder#whereIn(Object, int)} instead.
   * 
   * @param column the column
   * @param comparison the comparison operation
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object column, ComparisonOp comparison) {
    if(ComparisonOp.IN == comparison || ComparisonOp.NOT_IN == comparison)
      throw new IllegalArgumentException(
          String.format(
              "%1$s is not valid for this method.",
              comparison.name()));
    filters.add(new SimpleEntry<>(column.toString(), comparison.getOp()));
    return this;
  }

  /**
   * Adds a WHERE clause that compares two columns.
   *
   * Note that {@link ComparisonOp#IN} and {@link ComparisonOp#NOT_IN} are not
   * valid here.
   *
   * @param leftColumn the first column to compare
   * @param rightColumn the second column to compare
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object leftColumn, ComparisonOp comparison, Object rightColumn) {
    if(ComparisonOp.IN == comparison || ComparisonOp.NOT_IN == comparison)
      throw new IllegalArgumentException(
          String.format(
              "%1$s is not valid for this method.",
              comparison.name()));
    filters.add(
        new SimpleEntry<>(
            leftColumn.toString(),
            comparison.getOp().replace("?", rightColumn.toString())));
    return this;
  }

  /**
   * Adds an IN (or NOT IN) comparison to a WHERE clause.
   *
   * @param colum the name of the column
   * @param not {@code true} for NOT IN, {@code false} for IN
   * @param count the number of items in the collection
   * @return this SQLBuilder object
   */
  public SQLBuilder whereIn(Object column, boolean not, int count) {
    StringBuilder sb = new StringBuilder("(");
    for(int i = 0; i < count; i++) {
      if(0 < i) sb.append(", ");
      sb.append("?");
    }
    sb.append(")");
    filters.add(
        new SimpleEntry<>(
            column.toString(),
            (not ? ComparisonOp.NOT_IN : ComparisonOp.IN)
                .getOp().replace("?", sb.toString())));
    return this;
  }

  /**
   * Adds a subquery to WHERE clause.
   *
   * @param column the column
   * @param op the comparison operation
   * @param subquery the subquery for comparison
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object column, ComparisonOp op, SQLBuilder subquery) {
    filters.add(
        new SimpleEntry<>(
            column.toString(),
            op.getOp().replace(
                "?",
                String.format(
                    "( %1$s )",
                    subquery.toString()))));
    return this;
  }

  /**
   * Adds a column or set of columns to a GROUP BY clause.
   *
   * @param columns a varargs array of column names
   */
  public SQLBuilder group(Object... columns) {
    for(var column : columns)
      groupBy.add(column.toString());
    return this;
  }
  
  /**
   * Adds an ORDER BY clause.
   * 
   * @param column the column to order by
   * @param order the order
   * @return this SQLBuilder object
   */
  public SQLBuilder order(Object column, Order order) {
    orderBy.put(column.toString(), order);
    return this;
  }
  
  /**
   * Adds a LIMIT clause.
   * 
   * @param limit the number of records that the result should be limited to
   * @return this SQLBuilder object
   */
  public SQLBuilder limit(int limit) {
    this.limit = limit;
    return this;
  }
  
  /**
   * Adds a LIMIT clause and a follow-up OFFSET clause.
   *
   * @param limit the number of records that the result should be limited to
   * @param offset the number of records that should be skipped
   * @return this SQLBuilder object
   */
  public SQLBuilder limit(int limit, int offset) {
    this.limit = limit;
    this.offset = offset;
    return this;
  }
  
  /**
   * Builds the query.
   * 
   * @return a String with the built query
   */
  private String build() {
    StringBuilder stringBuilder = new StringBuilder();
    
    switch(statement) {
    case INSERT_IGNORE: // if this is an INSERT IGNORE statement
      stringBuilder.append("IGNORE ");
      // fall through, execute the rest of the build case for INSERT
      
    case INSERT: // if this is an INSERT statement
      
      // add table, columns; put INSERT at begining just in case IGNORE is specified
      stringBuilder.insert(0, "INSERT ").append("INTO ").append(table.getKey()).append(" (");
      for(int i = 0; i < columns.size(); i++) {
        stringBuilder.append(columns.get(i));
        if(i < columns.size() - 1) stringBuilder.append(", ");
      }
      
      // add values
      stringBuilder.append(") VALUES (");
      for(int i = 0; i < columns.size(); i++) {
        stringBuilder.append("?");
        if(i < columns.size() - 1) stringBuilder.append(", ");
      }
      stringBuilder.append(") ");
      
      break;
    
    case SELECT: // if this is a SELECT statement
      
      // add columns
      stringBuilder.append("SELECT ");
      for(int i = 0; i < columns.size(); i++) {
        stringBuilder.append(columns.get(i));
        if(i < columns.size() - 1) stringBuilder.append(", ");
      }
      
      // add table
      stringBuilder.append(" FROM ").append(table.getKey()).append(" ");

      if(null != table.getValue())
        stringBuilder.append(table.getValue()).append(" ");
      
      break;
    
    case UPDATE: // if this is an UPDATE statement
      
      // add table
      stringBuilder.append("UPDATE ").append(table.getKey()).append(" SET ");
      
      // add columns
      for(int i = 0; i < columns.size(); i++) {
        stringBuilder.append(columns.get(i)).append(" = ?");
        if(i < columns.size() - 1) stringBuilder.append(",");
        stringBuilder.append(" ");
      }
      
      break;
    
    case DELETE: // if this is a DELETE statement
      
      /*
       * Danger Will Robinson! Using DELETE statement without a WHERE clause WILL
       * result in all rows from a table being dropped.
       */
      
      // add table
      stringBuilder.append("DELETE FROM ").append(table.getKey()).append(" ");
      
      break;
    
    case REPLACE: // if this is a REPLACE statement
      
      // add table
      stringBuilder.append("REPLACE INTO ").append(table.getKey()).append(" SET ");
      
      // add columns
      for(int i = 0; i < columns.size(); i++) {
        stringBuilder.append(columns.get(i)).append(" = ?");
        if(i < columns.size() - 1) stringBuilder.append(",");
        stringBuilder.append(" ");
      }
      
      break;
    
    default:
      return null;
    }
    
    // add joins
    for(var join : joins) {
      stringBuilder.append(join.getKey().name())
          .append(" JOIN ")
          .append(join.getValue().getKey().getKey())
          .append(" ")
          .append(join.getValue().getKey().getValue())
          .append(" ON ");

      StringBuilder compSB = new StringBuilder();
      var comps = join.getValue().getValue();
      if(1 < comps.size())
        compSB.append("( ");
      for(int i = 0; i < comps.size(); i++) {
        if(0 < i)
          compSB.append(
              comps.get(i - 1).isOr()
                  ? "OR "
                  : "AND ");
        var comp = comps.get(i);
        compSB
            .append(comp.left())
            .append(
                null == comp.right()
                    ? comp.op().getOp()
                    : comp.op().getOp().replace(
                        "?",
                        comp.right()));        
      }
      if(1 < comps.size())
        compSB.append(") ");

      stringBuilder.append(compSB);
    }
    
    // append WHERE clause if filters exist
    if(filters.size() > 0) {
      stringBuilder.append("WHERE ");
      boolean useOr = false;
      for(int i = 0; i < filters.size(); i++) {
        if(conjunctions.containsKey(i)) useOr = conjunctions.get(i);
        if(i > 0) stringBuilder.append(useOr ? "OR " : "AND ");
        if(conjunctions.containsKey(i + 1) && conjunctions.get(i + 1))
          stringBuilder.append("(");
        stringBuilder.append(filters.get(i).getKey());
        stringBuilder.append(filters.get(i).getValue());
        if(conjunctions.containsKey(i + 1) && !conjunctions.get(i + 1)
            || useOr && filters.size() - 1 == i)
          stringBuilder.insert(stringBuilder.length() - 1, ")");
      }
    }

    // append GROUP BY clause if groups are specified
    int groupCount = 0;
    for(var groupEntry : groupBy) {
      if(groupCount++ == 0)
        stringBuilder.append("GROUP BY ");
      else stringBuilder.insert(stringBuilder.length() - 1, ',');
      stringBuilder
          .append(groupEntry)
          .append(' ');
    }
    
    // append ORDER BY clause if order is specified
    int orderCount = 0;
    for(var orderEntry : orderBy.entrySet()) {
      if(orderCount++ == 0)
        stringBuilder.append("ORDER BY ");
      else stringBuilder.insert(stringBuilder.length() - 1, ',');
      stringBuilder
          .append(orderEntry.getKey())
          .append(' ')
          .append(orderEntry.getValue().toString())
          .append(' ');
    }
    
    // append LIMIT clause if limit is specified
    if(limit > 0) {
      stringBuilder.append("LIMIT ").append(limit);
      if(offset > 0)
        stringBuilder.append(" OFFSET ").append(offset);
    }
    
    // trim whitespace just in case
    return stringBuilder.toString().trim();
  }
  
  /**
   * Clears out query specifications for object reuse
   */
  public void clear() {
    columns.clear();
    filters.clear();
    statement = null;
    table = null;
  }
  
  /**
   * Builds query and utilizes log appropriately.
   */
  @Override public String toString() {
    String query = build();
    if(null == query)
      throw new IllegalStateException("Failed to generate statement.");
    logger.debug("Generated query: {}", query);
    return query;
  }
  
  /**
   * Converts a UUID to an array of bytes.
   *
   * @param uuid the UUID that needs to be converted
   * @return a byte array of length 16, unless the uuid
   *         argument is {@code null}, in which case
   *         return {@code null}
   */
  public static byte[] uuidToBytes(UUID uuid) {
    if(null == uuid) return null;
    ByteBuffer byteBuf = ByteBuffer.wrap(new byte[16]);
    byteBuf.putLong(uuid.getMostSignificantBits());
    byteBuf.putLong(uuid.getLeastSignificantBits());
    return byteBuf.array();
  }
  
  /**
   * Converts an array of bytes to a UUID.
   *
   * @param bytes the byte array that needs to be converted
   * @return a UUID representing the converted bytes, unless
   *         the byte array argument is {@code null}, in
   *         which case return {@code null}
   */
  public static UUID bytesToUUID(byte[] bytes) {
    if(null == bytes) return null;
    ByteBuffer byteBuf = ByteBuffer.wrap(bytes);
    return new UUID(byteBuf.getLong(), byteBuf.getLong());
  }
  
}
