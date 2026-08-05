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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Map;

import org.testng.annotations.Test;

/**
 * Covers the pool's own lifecycle.
 *
 * <p>{@link Database} owns a Hikari connection pool and had no way to dispose
 * of it: the field is private, and the only public {@code close} took a
 * connection, a statement and a result set. An application could return
 * connections to the pool and never shut the pool down, so its housekeeping
 * thread and idle connections outlived any orderly shutdown.
 *
 * <p>These run without a database, which needs one accommodation. Hikari fails
 * fast — it dials out while the pool is built, and throws if nothing answers —
 * so the fixture passes {@code initializationFailTimeout} negative, Hikari's
 * own switch for "do not connect at construction". That this works at all is
 * itself a property of the change these accompany: pool settings only reach the
 * pool now. What is under test here is the lifecycle, not the dialling.
 *
 * @author Axonibyte Innovations, LLC
 */
public class DatabaseLifecycleTest {

  /**
   * A pool that has been configured but has never connected to anything.
   *
   * <p>The address is deliberately one nothing is listening on: any test that
   * reached the network would be testing Hikari rather than this class.
   */
  static Database detached() throws SQLException {
    return new Database(
        "127.0.0.1:1/nodb", "t_", "nobody", "nothing", false,
        Map.of(
            // Negative disables the fail-fast probe, so building the pool
            // performs no I/O at all.
            "initializationFailTimeout", "-1",
            // One connection, and do not go looking for it in the background.
            "maximumPoolSize", "1",
            "minimumIdle", "0"));
  }

  @Test public void isAutoCloseable() {
    // The property that lets a caller write try-with-resources at all. Without
    // the interface the compiler refuses, however many close methods the class
    // happens to have.
    assertTrue(AutoCloseable.class.isAssignableFrom(Database.class));
  }

  @Test public void closesAndSaysSo() throws SQLException {
    Database db = detached();
    assertFalse(db.isClosed(), "a fresh pool should be open");

    db.close();
    assertTrue(db.isClosed(), "the pool should report itself closed afterwards");
  }

  @Test public void closingTwiceIsHarmless() throws SQLException {
    // Called from shutdown hooks, and sometimes from a try-with-resources that
    // a hook has already beaten to it.
    Database db = detached();
    db.close();
    db.close();
    assertTrue(db.isClosed());
  }

  @Test public void tryWithResourcesClosesIt() throws SQLException {
    Database escaped;
    try(Database db = detached()) {
      assertFalse(db.isClosed());
      escaped = db;
    }
    assertTrue(escaped.isClosed(), "leaving the block should have closed the pool");
  }

  @Test public void tryWithResourcesClosesItOnFailureToo() throws SQLException {
    Database escaped = null;
    try(Database db = detached()) {
      escaped = db;
      throw new IllegalStateException("something went wrong mid-block");
    } catch(IllegalStateException expected) {
      // The point: the pool is disposed of on the way out regardless.
    }
    assertNotNull(escaped);
    assertTrue(escaped.isClosed());
  }

  @Test public void closeDeclaresNoCheckedException() throws Exception {
    // AutoCloseable.close() throws Exception, which would force every
    // try-with-resources caller into a catch they have nothing to do with.
    // Narrowing it to nothing is what keeps the idiom usable.
    assertTrue(
        Database.class.getMethod("close").getExceptionTypes().length == 0,
        "close() should not oblige callers to handle a checked exception");
  }

  @Test public void connectingAfterCloseFails() throws SQLException {
    // Rather than handing back a connection from a pool that no longer exists.
    Database db = detached();
    db.close();
    assertThrows(SQLException.class, () -> db.connect());
  }

  @Test public void theOtherCloseStillTakesNulls() throws SQLException {
    // `close()` and `close(Connection, PreparedStatement, ResultSet)` are
    // different operations that now share a name. This pins that adding the
    // no-argument one did not disturb the three-argument one, whose null
    // tolerance every `finally` block in every dependent project relies on.
    try(Database db = detached()) {
      db.close(null, null, null);
    }
  }

  @Test public void metadataSurvivesClosing() throws SQLException {
    // The prefix and name are read all over a dependent application, including
    // from shutdown paths that may run after the pool has gone.
    Database db = detached();
    db.close();
    assertTrue("nodb".equals(db.getName()));
    assertTrue("t_".equals(db.getPrefix()));
  }
}
