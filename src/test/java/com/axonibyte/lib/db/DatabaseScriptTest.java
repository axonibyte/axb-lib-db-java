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
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.Test;

/**
 * Covers the splitting of bootstrap scripts into individual statements.
 *
 * <p>This is the part of {@code Database.setup} that can go wrong silently. A
 * script that is mis-split does not throw -- it executes something other than
 * what was written, and the only symptom is a schema that is subtly not what
 * the file says. The regression these guard against did exactly that: script
 * lines were joined with a space rather than a newline, so a {@code --} header
 * comment commented out the entire rest of the file and the statement ran as a
 * no-op with no error at all.
 *
 * @author Axonibyte Innovations, LLC
 */
public class DatabaseScriptTest {

  @Test public void splitStatements_singleStatement() {
    List<String> out = Database.splitStatements("CREATE TABLE a (id INT)");
    assertEquals(out.size(), 1);
    assertEquals(out.get(0), "CREATE TABLE a (id INT)");
  }

  @Test public void splitStatements_trailingSemicolonIsNotASecondStatement() {
    assertEquals(Database.splitStatements("CREATE TABLE a (id INT);").size(), 1);
  }

  @Test public void splitStatements_severalStatements() {
    List<String> out = Database.splitStatements(
        "CREATE TABLE a (id INT);\nCREATE INDEX i ON a (id);\n");
    assertEquals(out.size(), 2);
    assertTrue(out.get(0).startsWith("CREATE TABLE"));
    assertTrue(out.get(1).startsWith("CREATE INDEX"));
  }

  @Test public void splitStatements_null() {
    assertEquals(Database.splitStatements(null).size(), 0);
  }

  @Test public void splitStatements_emptyScript() {
    // An empty file used to be a StringIndexOutOfBoundsException: the reader
    // unconditionally deleted a trailing character that was never appended.
    assertEquals(Database.splitStatements("").size(), 0);
    assertEquals(Database.splitStatements("   \n\n  ").size(), 0);
  }

  // --- comments --------------------------------------------------------------

  @Test public void splitStatements_lineCommentEndsAtTheNewline() {
    // The regression. With lines joined by a space this yielded one statement
    // consisting entirely of comment, which executes as nothing.
    List<String> out = Database.splitStatements(
        "-- Widen the column for IPv6.\n-- Another line.\nALTER TABLE a ADD COLUMN b INT");
    assertEquals(out.size(), 1);
    assertTrue(out.get(0).contains("ALTER TABLE a ADD COLUMN b INT"), out.get(0));
  }

  @Test public void splitStatements_hashCommentEndsAtTheNewline() {
    List<String> out = Database.splitStatements("# a MySQL-style comment\nSELECT 1");
    assertEquals(out.size(), 1);
    assertTrue(out.get(0).contains("SELECT 1"));
  }

  @Test public void splitStatements_blockComment() {
    List<String> out = Database.splitStatements("/* a header\n   spanning lines */\nSELECT 1");
    assertEquals(out.size(), 1);
    assertTrue(out.get(0).contains("SELECT 1"));
  }

  @Test public void splitStatements_aScriptOfOnlyCommentsYieldsNothing() {
    // Preparing a comment is a syntax error, so a licence-header-only file must
    // not reach the server.
    assertEquals(Database.splitStatements("-- nothing to see here\n").size(), 0);
    assertEquals(Database.splitStatements("/* nothing */\n").size(), 0);
  }

  @Test public void splitStatements_semicolonInsideALineCommentDoesNotSplit() {
    List<String> out = Database.splitStatements("-- see foo; bar\nSELECT 1");
    assertEquals(out.size(), 1);
  }

  @Test public void splitStatements_semicolonInsideABlockCommentDoesNotSplit() {
    List<String> out = Database.splitStatements("/* foo; bar */ SELECT 1");
    assertEquals(out.size(), 1);
  }

  // --- quoting ---------------------------------------------------------------

  @Test public void splitStatements_semicolonInsideAStringDoesNotSplit() {
    // The reason this cannot be a split(";"): a semicolon in a default value is
    // data, and splitting on it produces two invalid fragments.
    List<String> out = Database.splitStatements(
        "INSERT INTO a (t) VALUES ('one; two')");
    assertEquals(out.size(), 1);
    assertTrue(out.get(0).contains("'one; two'"));
  }

  @Test public void splitStatements_semicolonInsideAQuotedIdentifierDoesNotSplit() {
    for(String script : new String[] {
        "CREATE TABLE `odd;name` (id INT)",
        "CREATE TABLE \"odd;name\" (id INT)" }) {
      assertEquals(Database.splitStatements(script).size(), 1, script);
    }
  }

  @Test public void splitStatements_escapedQuoteDoesNotEndTheString() {
    List<String> out = Database.splitStatements(
        "INSERT INTO a (t) VALUES ('it\\'s here; really')");
    assertEquals(out.size(), 1, out.toString());
  }

  @Test public void splitStatements_doubledQuoteDoesNotEndTheString() {
    List<String> out = Database.splitStatements(
        "INSERT INTO a (t) VALUES ('it''s here; really')");
    assertEquals(out.size(), 1, out.toString());
  }

  @Test public void splitStatements_commentMarkerInsideAStringIsNotAComment() {
    // '--' inside a literal is data. Treating it as a comment would swallow the
    // rest of the line, including the closing quote.
    List<String> out = Database.splitStatements(
        "INSERT INTO a (t) VALUES ('a -- b');\nSELECT 1");
    assertEquals(out.size(), 2, out.toString());
  }

  // --- shape of real bootstrap scripts ---------------------------------------

  @Test public void splitStatements_aCommentedMigration() {
    // The exact shape that broke: a block-comment header, then one statement.
    String script = String.join(
        "\n",
        "/*",
        " * Widen volunteer IP storage to accommodate IPv6.",
        " */",
        "ALTER TABLE ${prefix}volunteer",
        "  ADD COLUMN IF NOT EXISTS",
        "  ip_addr_bin VARBINARY(16)");

    List<String> out = Database.splitStatements(script);
    assertEquals(out.size(), 1);
    assertTrue(out.get(0).contains("VARBINARY(16)"), out.get(0));
    // Newlines survive, which is what keeps a line comment from running on.
    assertTrue(out.get(0).contains("\n"));
  }

  @Test public void splitStatements_aTableAndItsIndexInOneFile() {
    // Two statements in one file: previously impossible, since the whole file
    // was handed to a single prepareStatement.
    String script = String.join(
        "\n",
        "CREATE TABLE IF NOT EXISTS a (",
        "  id BINARY(16) NOT NULL,",
        "  PRIMARY KEY (id)",
        ") Engine=InnoDB;",
        "",
        "CREATE INDEX IF NOT EXISTS idx_a ON a (id);");

    List<String> out = Database.splitStatements(script);
    assertEquals(out.size(), 2, out.toString());
    assertTrue(out.get(0).startsWith("CREATE TABLE"));
    assertTrue(out.get(1).startsWith("CREATE INDEX"));
  }
}
