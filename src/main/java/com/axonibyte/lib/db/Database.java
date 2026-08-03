/*
 * Copyright (c) 2021-2026 Axonibyte Innovations, LLC. All rights reserved.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.CodeSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles interactions with the database.
 *
 * @author Caleb L. Power <cpower@axonibyte.com>
 */
public class Database {

  private static final Logger logger = LoggerFactory.getLogger(Database.class);
  
  private HikariConfig hikariConfig = null;
  private HikariDataSource hikariDataSource = null;
  private String dbName = null;
  private String dbPrefix = null;
  
  /**
   * Instantiates the database handler.
   *
   * @param location the address and port to which the database is bound
   * @param prefix the string to prepend to all tables
   * @param username the username portion of the database credentials
   * @param password the password portion of the database credentials
   * @param ssl {@code true} iff the database connection should be secured
   * @throws SQLException if the database connection malfunctions
   */
  public Database(String location, String prefix, String username, String password, boolean ssl) throws SQLException {
    this(location, prefix, username, password, ssl, new HashMap<>() {{
      put("cachePrepStmts", "true");
      put("prepStmtCacheSize", "250");
      put("prepStmtCacheSqlLimit", "2048");
      put("useServerPrepStmts", "true");
      put("useLocalSessionState", "true");
      put("rewriteBatchedStatements", "true");
      put("cacheResultSetMetadata", "true");
      put("cacheServerConfiguration", "true");
      put("elideSetAutoCommits", "true");
      put("maintainTimeState", "false");
      put("connectionTimeout", "30000");
      put("maxLifetime", "180000");
      put("idleTimeout", "30000");
      put("leakDetectionThreshold", "5000");
    }});
  }
  
  /**
   * Instantiates the database handler with custom Hikari properties.
   *
   * @param location the address and port to which the database is bound
   * @param prefix the string to prepend to all tables
   * @param username the username portion of the database credentials
   * @param password the password portion of the database credentials
   * @param ssl {@code true} iff the database connection should be secured
   * @param properties Hikari properties
   * @throws SQLException if the database connection malfunctions
   */
  public Database(String location, String prefix, String username, String password, boolean ssl,
      Map<String, String> properties) throws SQLException {
    String[] locationArgs = location.split("/");
    if(2 != locationArgs.length)
      throw new SQLException(
          "Database location must include name of database e.g. port/database)");
    
    this.dbName = locationArgs[1];
    this.dbPrefix = prefix;
    
    this.hikariConfig = new HikariConfig();
    this.hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
    this.hikariConfig.setJdbcUrl(
        String.format(
            "jdbc:mariadb://%1$s?autoReconnect=true&serverTimezone=UTC&useSSL=%2$b",
            location,
            ssl));
    this.hikariConfig.setUsername(username);
    this.hikariConfig.setPassword(password);
    for(var property : properties.entrySet()) {
      logger.info(
          "Adding Hikiri data source property {}={}",
          property.getKey(),
          property.getValue());
      this.hikariConfig.addDataSourceProperty(property.getKey(), property.getValue());
    }
    this.hikariDataSource = new HikariDataSource(hikariConfig);
  }
  
  /**
   * Retrieves a {@link Connection} to the database.
   *
   * @return a {@link Connection} object
   * @throws SQLException if a database error occurs
   */
  public Connection connect() throws SQLException {
    logger.debug("Obtaining database connection.");
    return hikariDataSource.getConnection();
  }
  
  /**
   * Rerieves the name of the database.
   *
   * @return a string representing the name of the database
   */
  public String getName() {
    return dbName;
  }
  
  /**
   * Retrieves the global table prefix.
   *
   * @return the string to be prepended to all table names
   */
  public String getPrefix() {
    return dbPrefix;
  }
  
  /**
   * Thoroughly closes a database connection, prepared statement, and result set.
   *
   * @param con a {@link Connection} to close, or {@code null} to skip connection closure
   * @param stmt a {@link PreparedStatement to close}, or {@code null} to skip statement closure
   * @param res a {@link ResultSet} to close, or {@code null} to skip resultset closure
   */
  public void close(Connection con, PreparedStatement stmt, ResultSet res) {
    // Failures here are logged rather than thrown, because close() is called from
    // finally blocks where throwing would mask the original exception. They were
    // previously swallowed entirely, which hid connection-return and commit failures
    // completely -- including, with autocommit disabled, silently discarded work.
    try {
      if(null != res) {
        logger.debug("Closing result set.");
        res.close();
      }
    } catch(SQLException e) {
      logger.warn("Failed to close result set: {}", describe(e));
    }

    try {
      if(null != stmt) {
        logger.debug("Closing statement.");
        stmt.close();
      }
    } catch(SQLException e) {
      logger.warn("Failed to close statement: {}", describe(e));
    }

    try {
      if(null != con) {
        logger.debug("Closing connection.");
        con.close();
      }
    } catch(SQLException e) {
      logger.warn("Failed to return connection to the pool: {}", describe(e));
    }
  }

  private static String describe(Exception e) {
    return null == e.getMessage() ? e.getClass().getSimpleName() : e.getMessage();
  }

  /**
   * Runs a unit of work inside a single database transaction, committing on success
   * and rolling back on any failure.
   *
   * <p>Connections come from the pool with autocommit enabled, so a sequence of
   * statements issued through {@link #connect()} is not atomic: a failure partway
   * through leaves the earlier statements committed. Anything that writes more than
   * one row across more than one statement -- an invoice and its line items, an
   * organization and its first member, a message and its recipients -- should run
   * through here instead.</p>
   *
   * <p>Autocommit is restored before the connection is returned to the pool.</p>
   *
   * @param <T> the type produced by the unit of work
   * @param work the work to perform, receiving a {@link Connection} in a transaction
   * @return whatever the unit of work returned
   * @throws SQLException if the work failed, if the commit failed, or if a connection
   *         could not be obtained; the transaction is rolled back in every such case
   */
  public <T> T transaction(TransactionalWork<T> work) throws SQLException {
    Connection con = null;
    boolean priorAutoCommit = true;

    try {
      con = connect();
      priorAutoCommit = con.getAutoCommit();
      con.setAutoCommit(false);

      T result = work.execute(con);

      con.commit();
      return result;

    } catch(SQLException | RuntimeException e) {
      if(null != con) {
        try {
          logger.warn("Rolling back transaction: {}", describe(e));
          con.rollback();
        } catch(SQLException rollbackFailure) {
          logger.error("Rollback itself failed: {}", describe(rollbackFailure));
          e.addSuppressed(rollbackFailure);
        }
      }
      if(e instanceof SQLException) throw (SQLException)e;
      throw (RuntimeException)e;

    } finally {
      if(null != con) {
        try {
          con.setAutoCommit(priorAutoCommit);
        } catch(SQLException e) {
          logger.warn("Failed to restore autocommit: {}", describe(e));
        }
        close(con, null, null);
      }
    }
  }

  /**
   * A unit of work to be executed inside a transaction.
   *
   * @param <T> the type produced by the unit of work
   */
  @FunctionalInterface public interface TransactionalWork<T> {

    /**
     * Performs the work. The connection is in a transaction; do not commit, roll
     * back, or close it.
     *
     * @param con the transactional {@link Connection}
     * @return the result of the work
     * @throws SQLException if the work fails, causing a rollback
     */
    public T execute(Connection con) throws SQLException;

  }
  

  /**
   * Splits a SQL script into the individual statements it contains.
   *
   * <p>Semicolons only terminate a statement when they are actually code:
   * those inside string literals, quoted identifiers, or comments are part of
   * the text around them. A naive {@code split(";")} corrupts any script
   * containing so much as a semicolon in a default value.
   *
   * <p>Comments are preserved rather than stripped. They are legal SQL, the
   * server ignores them, and removing them would mean re-solving the same
   * quoting problem a second time for no benefit.
   *
   * @param script the full text of a bootstrap script
   * @return the statements it contains, in order, with blank ones omitted
   */
  static List<String> splitStatements(String script) {
    List<String> statements = new ArrayList<>();
    if(null == script) return statements;

    StringBuilder current = new StringBuilder();
    int len = script.length();

    for(int i = 0; i < len; i++) {
      char c = script.charAt(i);
      char next = i + 1 < len ? script.charAt(i + 1) : '\0';

      // Line comments run to the end of the line -- which is exactly why the
      // reader must preserve newlines. Joining lines with a space, as this
      // once did, let a `--` header comment silently swallow the entire rest
      // of the file: the statement then executed as a no-op with no error.
      if(('-' == c && '-' == next) || '#' == c) {
        while(i < len && '\n' != script.charAt(i)) current.append(script.charAt(i++));
        if(i < len) current.append('\n');
        continue;
      }

      if('/' == c && '*' == next) {
        current.append(c).append(next);
        i += 2;
        while(i < len && !('*' == script.charAt(i) && i + 1 < len && '/' == script.charAt(i + 1)))
          current.append(script.charAt(i++));
        if(i + 1 < len) {
          current.append("*/");
          i++;
        }
        continue;
      }

      if('\'' == c || '"' == c || '`' == c) {
        current.append(c);
        i++;
        while(i < len) {
          char q = script.charAt(i);
          // A backslash escape, or a doubled quote -- both mean the literal
          // continues past what looks like its terminator.
          if('\\' == q && i + 1 < len) {
            current.append(q).append(script.charAt(i + 1));
            i += 2;
            continue;
          }
          if(c == q && i + 1 < len && c == script.charAt(i + 1)) {
            current.append(q).append(q);
            i += 2;
            continue;
          }
          current.append(q);
          if(c == q) break;
          i++;
        }
        continue;
      }

      if(';' == c) {
        add(statements, current);
        current.setLength(0);
        continue;
      }

      current.append(c);
    }

    add(statements, current);
    return statements;
  }

  /** Adds a statement if it holds anything the server would act on. */
  private static void add(List<String> statements, StringBuilder candidate) {
    String trimmed = candidate.toString().trim();
    if(trimmed.isEmpty()) return;

    // A fragment that is only comments and whitespace is not a statement, and
    // preparing it is a syntax error. This is what makes a trailing semicolon,
    // or a file that is entirely a licence header, harmless.
    if(stripComments(trimmed).isBlank()) return;
    statements.add(trimmed);
  }

  /** The statement with its comments removed, for emptiness checks only. */
  private static String stripComments(String statement) {
    return statement
        .replaceAll("(?s)/\\*.*?\\*/", " ")
        .replaceAll("(?m)(--|#).*$", " ");
  }

  /**
   * Sets up the database, adding in tables and otherwise running through
   * predetermined scripts.
   *
   * @param clazz the class associated with resources to retrieve
   * @throws SQLException if there's a database malfunction
   */
  public void setup(Class<?> clazz, String parent) throws SQLException {
    Connection con = connect();
    PreparedStatement stmt = null;
    Set<String> fileList = new TreeSet<>();
    
    CodeSource src = clazz.getProtectionDomain().getCodeSource();
    if(null != src) {
      URL jar = src.getLocation();
      try(ZipInputStream zip = new ZipInputStream(jar.openStream())) {
        ZipEntry entry = null;
        while(null != (entry = zip.getNextEntry())) {
          var file = entry.getName();
          logger.debug("Checking if entry {} is a SQL script.", file);
          if(file.matches(parent + "/.*\\.sql")) {
            logger.debug("Entry {} is a SQL script, queueing.", file);
            fileList.add(file);
          }
        }
      } catch(IOException e) {
        logger.error("Failed to read jar.");
        e.printStackTrace();
      }
    } else logger.error("Could not retrieve class protection domain!");

    try {
      for(var file : fileList) {
        String resource = null;
        logger.info("Reading database bootstrap script {}", file);
        
        InputStream in = getClass().getClassLoader().getResourceAsStream(file);
        if(null == in) {
          // Listed in the archive but not resolvable by the classloader. Better
          // to say so than to skip it silently -- a bootstrap script that never
          // ran is invisible until the schema it creates is missing.
          logger.error("Bootstrap script {} could not be opened; skipping.", file);
          continue;
        }

        try(BufferedReader reader = new BufferedReader(
            new InputStreamReader(in, StandardCharsets.UTF_8))) {
          StringBuilder resBuilder = new StringBuilder();
          for(String line; null != (line = reader.readLine());)
            resBuilder.append(line).append('\n');
          resource = resBuilder.toString();
        }

        List<String> statements = splitStatements(
            resource.replace("${database}", dbName).replace("${prefix}", dbPrefix));
        if(1 < statements.size())
          logger.debug("Script {} contains {} statements.", file, statements.size());

        for(int i = 0; i < statements.size(); i++) {
          stmt = null;
          try {
            stmt = con.prepareStatement(statements.get(i));
            stmt.execute();
          } catch(SQLException e) {
            // Name the script and the statement within it. Without this a
            // failure part-way through a multi-statement file reports only the
            // syntax error, leaving the reader to work out which file -- and
            // which part of it -- the server was talking about.
            throw new SQLException(
                String.format(
                    "Bootstrap script %1$s failed at statement %2$d of %3$d: %4$s",
                    file,
                    i + 1,
                    statements.size(),
                    e.getMessage()),
                e.getSQLState(),
                e.getErrorCode(),
                e);
          } finally {
            close(null, stmt, null);
          }
        }
      }
    } catch(IOException e) {
      throw new SQLException(
          "Database bootstrap scripts could not be read.");
    } catch(SQLException e) {
      throw e;
    } finally {
      close(con, null, null);
    }
  }
  
}
