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

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

/**
 * Covers the separation of pool settings from driver properties.
 *
 * <p>Both arrive through the same map, and everything in it used to be handed
 * to {@code addDataSourceProperty} — which passes the value to the JDBC driver.
 * A pool setting sent that way is accepted without complaint and then ignored,
 * so the shipped defaults named a connection timeout, a maximum lifetime, an
 * idle timeout and a leak detection threshold while the pool quietly ran on
 * Hikari's own values for all four. Nothing failed; the configuration simply
 * did not exist.
 *
 * <p>The assertions below lean on {@code initializationFailTimeout}, which is
 * both a pool setting and observable without a database: negative means "do not
 * connect while building the pool". If pool settings are being dropped, the
 * constructor dials out and throws, so a construction that *succeeds* against
 * an address nothing is listening on is proof the setting arrived.
 *
 * @author Axonibyte Innovations, LLC
 */
public class DatabasePoolConfigTest {

  /** An address nothing will answer on. */
  private static final String NOWHERE = "127.0.0.1:1/nodb";

  private static Database build(Map<String, String> properties) throws SQLException {
    return new Database(NOWHERE, "t_", "nobody", "nothing", false, properties);
  }

  /** The fixture's settings, plus whatever a test wants to add. */
  private static Map<String, String> detachedWith(String key, String value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("initializationFailTimeout", "-1");
    properties.put("maximumPoolSize", "1");
    properties.put("minimumIdle", "0");
    if(null != key) properties.put(key, value);
    return properties;
  }

  @Test public void poolSettingsReachThePool() throws SQLException {
    // The whole point, stated as behaviour rather than as a getter: this
    // construction can only succeed if initializationFailTimeout was applied.
    try(Database db = build(detachedWith(null, null))) {
      assertTrue(!db.isClosed());
    }
  }

  @Test public void withoutThemHikariStillFailsFast() {
    // The control. Left to its default, Hikari dials out while building and
    // throws when nothing answers -- which is the behaviour every caller has
    // always had, and which the change above must not remove.
    assertThrows(Exception.class, () -> build(Map.of("maximumPoolSize", "1")));
  }

  @Test public void driverPropertiesAreStillPassedThrough() throws SQLException {
    // Anything not recognised as a pool setting keeps going to the driver, as
    // it always did. These are genuine MariaDB properties and must not be
    // parsed as pool settings, or a perfectly good configuration starts
    // throwing.
    Map<String, String> properties = detachedWith(null, null);
    properties.put("cachePrepStmts", "true");
    properties.put("prepStmtCacheSize", "250");
    properties.put("useServerPrepStmts", "true");

    try(Database db = build(properties)) {
      assertTrue(!db.isClosed());
    }
  }

  @Test public void aNonNumericPoolSettingIsRefused() {
    // Refused rather than ignored. Silently dropping a misconfigured pool
    // setting is precisely the failure this change exists to end, so it would
    // be perverse to replace one silent drop with another.
    //
    // Caught rather than asserted with assertThrows, which returns void in
    // TestNG -- the message matters as much as the type, because a caller
    // reading it needs to know which property they got wrong.
    try {
      build(detachedWith("connectionTimeout", "half a minute"));
      fail("a pool setting that cannot be parsed should not be accepted");
    } catch(SQLException e) {
      assertTrue(
          e.getMessage().contains("connectionTimeout"),
          "the message should name the property: " + e.getMessage());
    }
  }

  @Test public void anOutOfRangePoolSettingIsRefused() {
    // Hikari validates in the setter for this one; the point is that its
    // complaint reaches the caller instead of vanishing.
    assertThrows(
        SQLException.class,
        () -> build(detachedWith("maximumPoolSize", "0")));
  }

  @Test public void aNonNumericDriverPropertyIsFine() {
    // The counterpart to the two above: only recognised pool settings are
    // parsed. A driver property is an opaque string and stays one.
    Map<String, String> properties = detachedWith(null, null);
    properties.put("connectionCollation", "utf8mb4_general_ci");

    try(Database db = build(properties)) {
      assertTrue(!db.isClosed());
    } catch(SQLException e) {
      throw new AssertionError("a driver property should not be parsed", e);
    }
  }

}
