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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles interactions with the database.
 *
 * <p>Owns a connection pool, and is therefore itself a resource. Call
 * {@link #close()} when the application is shutting down, or hold it in a
 * try-with-resources block:
 *
 * <pre>
 *   try(Database db = new Database(location, prefix, user, pass, ssl)) {
 *     db.setup(App.class, "db");
 *     ...
 *   }
 * </pre>
 *
 * <p>Note that {@link #close(Connection, PreparedStatement, ResultSet)} is a
 * different operation entirely: it returns one connection to the pool, and is
 * what a {@code finally} block calls after a query. {@link #close()} shuts the
 * pool itself down.
 *
 * @author Caleb L. Power <cpower@axonibyte.com>
 */
public class Database implements AutoCloseable {

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
      // Raised from 5000. Until now none of the four settings above reached the
      // pool at all -- they were passed as driver properties, which silently
      // ignore them -- so this value has never actually been in force and there
      // is no established behaviour to preserve. Five seconds is too eager for
      // a library whose own setup() legitimately holds one connection for the
      // length of a migration run: it would report a leak, with a stack trace,
      // on every boot of a project with more than a handful of scripts.
      put("leakDetectionThreshold", "60000");
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
      var setting = POOL_SETTINGS.get(property.getKey());

      // Pool settings and driver properties are different things that arrive
      // through the same map, and telling them apart is the whole point of this
      // branch. Everything used to go to addDataSourceProperty, which hands the
      // value to the JDBC driver -- so a pool setting sent that way was accepted
      // without complaint and then silently ignored. The shipped defaults below
      // named four of them, so this class has spent its whole life claiming a
      // connection timeout, a maximum lifetime, an idle timeout and a leak
      // detection threshold while running on Hikari's defaults for all four.
      if(null != setting) {
        logger.info(
            "Setting Hikari pool property {}={}",
            property.getKey(),
            property.getValue());
        try {
          setting.accept(this.hikariConfig, property.getValue());
        } catch(IllegalArgumentException e) {
          // Refused rather than ignored. A misconfigured pool setting that is
          // quietly dropped is exactly the failure this change exists to end.
          throw new SQLException(
              String.format(
                  "Pool property %1$s cannot take the value \"%2$s\"",
                  property.getKey(),
                  property.getValue()),
              e);
        }
        continue;
      }

      logger.info(
          "Adding Hikari data source property {}={}",
          property.getKey(),
          property.getValue());
      this.hikariConfig.addDataSourceProperty(property.getKey(), property.getValue());
    }
    this.hikariDataSource = new HikariDataSource(hikariConfig);
  }

  /**
   * The properties that configure the pool rather than the driver.
   *
   * <p>Anything not named here is passed through to the JDBC driver as a data
   * source property, which is what every entry used to be. Hikari's own naming
   * is used verbatim, so a caller can look a setting up in Hikari's
   * documentation and expect it to work.
   */
  private static final Map<String, BiConsumer<HikariConfig, String>> POOL_SETTINGS =
      Map.ofEntries(
          Map.entry("connectionTimeout", (c, v) -> c.setConnectionTimeout(Long.parseLong(v))),
          Map.entry("idleTimeout", (c, v) -> c.setIdleTimeout(Long.parseLong(v))),
          Map.entry("maxLifetime", (c, v) -> c.setMaxLifetime(Long.parseLong(v))),
          Map.entry("keepaliveTime", (c, v) -> c.setKeepaliveTime(Long.parseLong(v))),
          Map.entry("validationTimeout", (c, v) -> c.setValidationTimeout(Long.parseLong(v))),
          Map.entry(
              "leakDetectionThreshold",
              (c, v) -> c.setLeakDetectionThreshold(Long.parseLong(v))),
          Map.entry(
              "initializationFailTimeout",
              (c, v) -> c.setInitializationFailTimeout(Long.parseLong(v))),
          Map.entry("maximumPoolSize", (c, v) -> c.setMaximumPoolSize(Integer.parseInt(v))),
          Map.entry("minimumIdle", (c, v) -> c.setMinimumIdle(Integer.parseInt(v))),
          Map.entry("poolName", (c, v) -> c.setPoolName(v)),
          Map.entry("connectionTestQuery", (c, v) -> c.setConnectionTestQuery(v)),
          Map.entry("connectionInitSql", (c, v) -> c.setConnectionInitSql(v)),
          Map.entry("autoCommit", (c, v) -> c.setAutoCommit(Boolean.parseBoolean(v))),
          Map.entry("readOnly", (c, v) -> c.setReadOnly(Boolean.parseBoolean(v))));
  
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
   * Shuts the connection pool down, closing every connection it holds.
   *
   * <p>There was previously no way to do this from outside the class: the
   * {@link HikariDataSource} is private and the only public {@code close} took
   * a connection, a statement and a result set. An application could return
   * connections to the pool but never dispose of the pool itself, so its
   * housekeeping thread and idle connections outlived any attempt at an orderly
   * shutdown. Harmless when the process is exiting anyway; not harmless for a
   * test that builds several, an application that reconfigures at runtime, or
   * anything embedding this in a longer-lived host.
   *
   * <p>Idempotent, and safe to call from a shutdown hook. Once closed,
   * {@link #connect()} fails: the pool is gone, and pretending otherwise would
   * hand out connections that cannot work.
   */
  @Override public void close() {
    if(null == hikariDataSource || hikariDataSource.isClosed()) return;
    logger.info("Closing connection pool.");
    hikariDataSource.close();
  }

  /**
   * Determines whether the connection pool has been shut down.
   *
   * @return {@code true} if {@link #close()} has already run
   */
  public boolean isClosed() {
    return null == hikariDataSource || hikariDataSource.isClosed();
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
   * <p>Nothing is tracked between runs: every script is executed on every call.
   * Scripts must therefore be idempotent -- {@code IF NOT EXISTS} throughout,
   * and {@code DROP} only in its {@code IF EXISTS} form.
   *
   * @param clazz the class whose protection domain holds the scripts
   * @param parent the directory within that archive to read scripts from
   * @throws SQLException if there's a database malfunction, or if a script
   *         could not be read
   */
  public void setup(Class<?> clazz, String parent) throws SQLException {
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
        // To slf4j, with the throwable, rather than to stderr. printStackTrace
        // bypassed the log entirely, so the one record of why no migration ran
        // went somewhere nobody was reading.
        logger.error("Failed to enumerate bootstrap scripts: {}", describe(e), e);
      }
    } else logger.error("Could not retrieve class protection domain!");

    // Opened once there is something to run, rather than before the archive is
    // scanned. It used to be taken first and held for the whole scan, and any
    // unchecked failure in there leaked it outright -- the finally that returns
    // it guards only the loop below.
    Connection con = connect();

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
        } catch(IOException e) {
          // Named, and with the cause attached. This was caught by a handler
          // wrapping the whole loop that threw away both -- "Database bootstrap
          // scripts could not be read", with no indication of which one or why.
          throw new SQLException(
              String.format(
                  "Bootstrap script %1$s could not be read: %2$s",
                  file,
                  describe(e)),
              e);
        }

        List<String> statements = splitStatements(
            resource.replace("${database}", dbName).replace("${prefix}", dbPrefix));
        if(1 < statements.size())
          logger.debug("Script {} contains {} statements.", file, statements.size());

        runStatements(con, file, statements);
      }
    } finally {
      close(con, null, null);
    }
  }

  /**
   * Executes the statements of one bootstrap script, in order, on one connection.
   *
   * <p>Through {@link Statement} rather than {@link PreparedStatement}, which is
   * not a stylistic preference. The server's prepared-statement protocol accepts
   * only a whitelist of commands, and {@code PREPARE}, {@code EXECUTE} and
   * {@code DEALLOCATE PREPARE} are not on it -- preparing one is error 1295,
   * "This command is not supported in the prepared statement protocol yet".
   * Those three are how a script does anything conditionally: MariaDB has no
   * {@code ALTER TABLE ... IF NOT EXISTS} for most changes, so an idempotent
   * migration inspects {@code information_schema}, builds the statement it needs
   * into a session variable, and prepares that. Every such script was therefore
   * asking the server for something it refuses.
   *
   * <p>It did not fail outright only because MariaDB Connector/J catches that
   * particular error and quietly re-runs the statement as text -- so the scripts
   * did work, at the price of an error packet logged at WARN for every guarded
   * statement on every boot, and of a schema whose correctness rested on an
   * undocumented fallback in a driver that is free to drop it.
   *
   * <p>Nothing is lost by the change. A bootstrap script has no parameters to
   * bind -- {@code ${database}} and {@code ${prefix}} are substituted into the
   * text long before it is sent -- so preparing bought a round trip and a plan
   * for a statement executed exactly once.
   *
   * <p>One {@link Statement} serves the whole script, and the caller's single
   * connection serves the whole run. That matters for the same guarded scripts:
   * a session variable belongs to the connection that set it, so a script that
   * sets one and then prepares from it in the next statement is only correct
   * while both land on the same connection.
   *
   * @param con the connection to execute on; held by the caller for the whole run
   * @param file the name of the script, for error messages
   * @param statements the statements it contains, in order
   * @throws SQLException if any statement fails, naming the script and the
   *         statement within it
   */
  static void runStatements(Connection con, String file, List<String> statements)
      throws SQLException {
    // A file that is entirely a licence header splits to nothing. Asking the
    // connection for a statement to run none of them is pointless work.
    if(statements.isEmpty()) return;

    try(Statement stmt = con.createStatement()) {
      for(int i = 0; i < statements.size(); i++) {
        try {
          stmt.execute(statements.get(i));
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
        }
      }
    }
  }

}
