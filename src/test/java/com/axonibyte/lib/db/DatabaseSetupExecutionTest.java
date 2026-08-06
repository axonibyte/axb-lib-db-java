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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

/**
 * Covers <em>how</em> {@code Database.setup} hands a bootstrap script to the
 * server, as opposed to how it splits one up.
 *
 * <p>The distinction matters because of a failure that a splitting test cannot
 * see. Every statement used to go through {@code prepareStatement}, and the
 * server's prepared-statement protocol accepts only a whitelist of commands.
 * {@code PREPARE}, {@code EXECUTE} and {@code DEALLOCATE PREPARE} are not on it,
 * and those three are precisely how a migration does anything conditionally:
 * MariaDB has no {@code ALTER TABLE ... IF NOT EXISTS} for most changes, so an
 * idempotent script reads {@code information_schema}, builds the statement it
 * needs into a session variable, and prepares that. Preparing a {@code PREPARE}
 * is error 1295.
 *
 * <p>The reason it went unnoticed for so long is the reason it needs a test
 * rather than a bug report: MariaDB Connector/J catches 1295 and quietly re-runs
 * the statement as text. The schema came out right, the boot log filled with
 * error packets at WARN, and the correctness of every guarded migration rested
 * on an undocumented fallback that the driver is free to withdraw.
 *
 * <p>These use a recording fake rather than a database. What is being pinned is
 * which JDBC call is made, which is not observable from the schema afterwards --
 * on a driver with the fallback, both spellings produce the same tables.
 *
 * @author Axonibyte Innovations, LLC
 */
public class DatabaseSetupExecutionTest {

  /**
   * A guarded migration, in the shape the real ones take.
   *
   * <p>Four statements: read the catalogue into a session variable, prepare it,
   * run it, discard it. Every one of the last three is unpreparable.
   */
  private static final String GUARDED_MIGRATION = String.join(
      "\n",
      "/*",
      " * Pin every text column to utf8mb4.",
      " */",
      "SET @conv = IF(",
      "  (SELECT COUNT(*) FROM information_schema.COLUMNS",
      "    WHERE TABLE_SCHEMA = 'yasss' AND TABLE_NAME = 'user'",
      "      AND CHARACTER_SET_NAME <> 'utf8mb4') = 0,",
      "  'DO 0',",
      "  'ALTER TABLE `yasss`.`user` CONVERT TO CHARACTER SET utf8mb4');",
      "PREPARE conv FROM @conv;",
      "EXECUTE conv;",
      "DEALLOCATE PREPARE conv;");

  @Test public void everyStatementIsExecutedInOrder() throws SQLException {
    Journal journal = new Journal();
    List<String> script = List.of("CREATE TABLE a (id INT)", "CREATE INDEX i ON a (id)");

    run(journal, "001_table_a.sql", script);

    assertEquals(journal.executed, script);
  }

  @Test public void nothingIsEverPrepared() throws SQLException {
    // The regression, stated directly. Before the fix this list held all four
    // statements of the script below, three of which the server refuses.
    Journal journal = new Journal();

    run(journal, "017_charset_utf8mb4.sql", Database.splitStatements(GUARDED_MIGRATION));

    assertEquals(
        journal.prepared,
        List.of(),
        "bootstrap statements must not go through the prepared-statement protocol");
  }

  @Test public void theUnpreparableStatementsReachTheServerVerbatim() throws SQLException {
    // Splitting and executing, together: the guard idiom has to survive both.
    // A script mangled on the way through is the other way this silently
    // produces the wrong schema.
    Journal journal = new Journal();

    run(journal, "017_charset_utf8mb4.sql", Database.splitStatements(GUARDED_MIGRATION));

    assertEquals(journal.executed.size(), 4, journal.executed.toString());
    assertTrue(journal.executed.get(0).contains("SET @conv = IF("), journal.executed.get(0));
    assertEquals(journal.executed.get(1), "PREPARE conv FROM @conv");
    assertEquals(journal.executed.get(2), "EXECUTE conv");
    assertEquals(journal.executed.get(3), "DEALLOCATE PREPARE conv");
  }

  @Test public void oneStatementServesTheWholeScript() throws SQLException {
    // Not merely tidy. A session variable belongs to the connection that set
    // it, so `SET @conv` and the `PREPARE conv FROM @conv` two statements later
    // are only correct while both land on the same connection -- which the
    // caller guarantees by holding one for the entire run.
    Journal journal = new Journal();

    run(journal, "017_charset_utf8mb4.sql", Database.splitStatements(GUARDED_MIGRATION));

    assertEquals(journal.statementsCreated, 1);
    assertEquals(journal.statementsClosed, 1, "the statement should be closed afterwards");
  }

  @Test public void aScriptWithNoStatementsNeverAsksForOne() throws SQLException {
    // A file that is entirely a licence header splits to nothing, and there is
    // no point asking for a statement to run none of them.
    Journal journal = new Journal();

    run(journal, "LICENCE_ONLY.sql", Database.splitStatements("-- nothing to see here\n"));

    assertEquals(journal.statementsCreated, 0);
    assertEquals(journal.executed, List.of());
  }

  // --- failures --------------------------------------------------------------

  @Test public void aFailureNamesTheScriptAndTheStatementWithinIt() {
    // Without this the reader gets a bare syntax error and no indication of
    // which of two dozen files -- or which part of one -- the server meant.
    Journal journal = new Journal();
    journal.failOn = "EXECUTE conv";

    SQLException thrown = expectThrows(
        SQLException.class,
        () -> run(journal, "017_charset_utf8mb4.sql", Database.splitStatements(GUARDED_MIGRATION)));

    assertTrue(thrown.getMessage().contains("017_charset_utf8mb4.sql"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("statement 3 of 4"), thrown.getMessage());
  }

  @Test public void aFailureKeepsTheServersOwnDiagnosis() {
    // The SQL state and vendor code are how a caller tells "this table already
    // exists" from "the server has gone away". Wrapping must not discard them.
    Journal journal = new Journal();
    journal.failOn = "EXECUTE conv";

    SQLException thrown = expectThrows(
        SQLException.class,
        () -> run(journal, "017.sql", Database.splitStatements(GUARDED_MIGRATION)));

    assertEquals(thrown.getSQLState(), "42000");
    assertEquals(thrown.getErrorCode(), 1064);
    assertTrue(thrown.getCause() instanceof SQLException, "the original should be the cause");
  }

  @Test public void aFailureStopsTheScript() {
    // Later statements in a migration routinely depend on earlier ones. Running
    // on past a failure would compound the damage rather than report it.
    Journal journal = new Journal();
    journal.failOn = "PREPARE conv";

    assertThrows(
        SQLException.class,
        () -> run(journal, "017.sql", Database.splitStatements(GUARDED_MIGRATION)));

    assertEquals(journal.executed.size(), 2, journal.executed.toString());
  }

  @Test public void theStatementIsClosedEvenWhenOneFails() {
    Journal journal = new Journal();
    journal.failOn = "PREPARE conv";

    assertThrows(
        SQLException.class,
        () -> run(journal, "017.sql", Database.splitStatements(GUARDED_MIGRATION)));

    assertEquals(journal.statementsClosed, 1);
  }

  // --- the fake --------------------------------------------------------------

  private static void run(Journal journal, String file, List<String> statements)
      throws SQLException {
    Database.runStatements(connection(journal), file, statements);
  }

  /** What the fake connection was asked to do. */
  private static final class Journal {
    private final List<String> prepared = new ArrayList<>();
    private final List<String> executed = new ArrayList<>();
    private int statementsCreated = 0;
    private int statementsClosed = 0;

    /** SQL containing this fragment fails, as the server would reject it. */
    private String failOn = null;
  }

  /**
   * A {@link Connection} that records rather than connects.
   *
   * <p>A proxy rather than a mock because the property under test is a negative
   * one -- that {@code prepareStatement} is never reached -- and a recorded list
   * asserts that more legibly than an expectation that was never set.
   *
   * <p>It serves the old spelling faithfully rather than refusing it: a
   * {@link PreparedStatement} that remembers the SQL it was built from, so that
   * {@code execute()} with no argument records the same thing {@code
   * execute(sql)} does. A reintroduced {@code prepareStatement} therefore fails
   * on the assertion that names the problem, rather than on a cast or a null
   * several frames away from it.
   */
  private static Connection connection(Journal journal) {
    return (Connection)Proxy.newProxyInstance(
        DatabaseSetupExecutionTest.class.getClassLoader(),
        new Class<?>[] { Connection.class },
        (proxy, method, args) -> {
          switch(method.getName()) {
            case "createStatement":
              journal.statementsCreated++;
              return statement(journal, null);
            case "prepareStatement":
              journal.prepared.add((String)args[0]);
              return statement(journal, (String)args[0]);
            default:
              return fallback(proxy, method, args);
          }
        });
  }

  /**
   * @param journal where to record what happens
   * @param preparedSql the SQL this statement was prepared from, or {@code null}
   *        if it came from {@code createStatement} and takes its SQL per call
   */
  private static Statement statement(Journal journal, String preparedSql) {
    return (Statement)Proxy.newProxyInstance(
        DatabaseSetupExecutionTest.class.getClassLoader(),
        new Class<?>[] { PreparedStatement.class },
        (proxy, method, args) -> {
          switch(method.getName()) {
            case "execute":
              String sql = null == args || 0 == args.length ? preparedSql : (String)args[0];
              journal.executed.add(sql);
              if(null != journal.failOn && sql.contains(journal.failOn))
                throw new SQLException("You have an error in your SQL syntax", "42000", 1064);
              return false;
            case "close":
              journal.statementsClosed++;
              return null;
            default:
              return fallback(proxy, method, args);
          }
        });
  }

  /** Identity semantics for the three Object methods, nothing for the rest. */
  private static Object fallback(Object proxy, java.lang.reflect.Method method, Object[] args) {
    switch(method.getName()) {
      case "toString": return "fake " + method.getDeclaringClass().getSimpleName();
      case "hashCode": return System.identityHashCode(proxy);
      case "equals": return proxy == args[0];
      default: return boolean.class == method.getReturnType() ? Boolean.FALSE : null;
    }
  }
}
