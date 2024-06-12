/*
 * Copyright (c) 2021-2023 Axonibyte Innovations, LLC. All rights reserved.
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

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
   * The operation to use when filtering values.
   *
   * @author Caleb L. Power <cpower@axonibyte.com>
   */
  public static enum Comparison {
    
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
    NOT_EQUAL_TO(" <> ? ");
    
    private String op = null;
    
    private Comparison(String op) {
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
  private List<Entry<String, Comparison>> filters = new ArrayList<>(); // filters for the WHERE clause
  private List<String> groupBy = new ArrayList<>(); // columns to group by
  // joins = [ { JOIN, { { TABLE, ALIAS }, { LEFT, { RIGHT, COMPARISON } } } }, ... ]
  private List<Entry<Join, Entry<Entry<String, String>, Entry<String, Entry<String, Comparison>>>>> joins = new ArrayList<>();
  private Map<Integer, Boolean> conjunctions = new HashMap<>();
  private Map<String, Order> orderBy = new LinkedHashMap<>();
  private Statement statement = null;

  private final Logger logger = LoggerFactory.getLogger(SQLBuilder.class);
  
  /**
   * Begins SELECT statement with multiple columns.
   * 
   * @param table the name of the table from which data is retrieved
   * @param columns that will be selected
   * @return this SQLBuilder object
   */
  public SQLBuilder select(String table, Object... columns) {
    select(table);
    for(var column : columns) column(column);
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
   * Specifies that one of the columns should be a result count.
   *
   * @param column the name of the returned column
   * @return this SQLBuilder object
   */
  public SQLBuilder count(Object column) {
    columns.add(
        String.format(
            "COUNT(*) AS %1$s",
            column.toString()));
    return this;
  }
  
  /**
   * Specifies that one of the columns should be summed.
   *
   * @param column the name of the column to be counted and returned
   * @return this SQLBuilder object
   */
  public SQLBuilder sum(Object column) {
    columns.add(
        String.format(
            "SUM(%1$s) AS %1$s",
            column));
    return this;
  }
  
  /**
   * Applies a join operation to the query.
   *
   * @param join the type of {@link Join} to be used
   * @param table the name of the table to be joined
   * @param alias the alias for the joining table
   * @param left the first column that is to be matched
   * @param right the right column that is to be matched
   * @param comparison the operation of comparison between left and right columns
   * @return this SQLBuilder object
   */
  public SQLBuilder join(Join join, String table, String alias, String left, String right, Comparison comparison) {
    joins.add(
        new SimpleEntry<>(
            join,
            new SimpleEntry<>(
                new SimpleEntry<>(table, alias),
                new SimpleEntry<>(
                    left,
                    new SimpleEntry<>(right, comparison)))));
    return this;
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
    return where(Comparison.EQUAL_TO, columns);
  }
  
  /**
   * Adds WHERE clause with specified columns to statement.
   *
   * @param comparison the {@link Comparison} to use
   * @param columns the columns that shall act as filters
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Comparison comparison, Object... columns) {
    for(var column : columns) where(column, comparison);
    return this;
  }
  
  /**
   * Adds a specific column to WHERE clause.
   *
   * @param column the column
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object column) {
    return where(column, Comparison.EQUAL_TO);
  }
  
  /**
   * Adds a specific column to WHERE clause.
   * 
   * @param column the column
   * @param comparison the comparison operation
   * @return this SQLBuilder object
   */
  public SQLBuilder where(Object column, Comparison comparison) {
    filters.add(new SimpleEntry<>(column.toString(), comparison));
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
    case INSERT: // if this is an INSERT statement
      
      // add table, columns
      stringBuilder.append("INSERT INTO ").append(table.getKey()).append(" (");
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
          .append(" ON ")
          .append(join.getValue().getValue().getKey())
          .append(join.getValue().getValue().getValue().getValue().getOp()
              .replace("?", join.getValue().getValue().getValue().getKey()));
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
        stringBuilder.append(filters.get(i).getValue().getOp());
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
