# Axonibyte Java Library Suite - Database

Copyright (c) 2021-2026 Axonibyte Innovations, LLC. All rights reserved.

This Java library handles database interactions, SQL parsing, and table mappings
for Java software. While there are a variety of libraries available to interact
with SQL databases, none of them are very intuitive when it comes to working
with the queries themselves. Libraries that assist in SQL building are often not
free. This library takes care of both use cases.

## Lifecycle

`Database` owns a connection pool, so it is itself a resource. Close it when the
application shuts down, or hold it in a try-with-resources block:

```java
try(Database db = new Database(location, prefix, user, pass, ssl)) {
  db.setup(App.class, "db");
  ...
}
```

Two different operations share the name `close`, and the distinction matters:

- `close(Connection, PreparedStatement, ResultSet)` returns **one connection** to
  the pool. This is what a `finally` block calls after a query, and it tolerates
  nulls.
- `close()` shuts **the pool itself** down. It is idempotent, throws no checked
  exception, and afterwards `connect()` fails rather than handing back a
  connection that cannot work. `isClosed()` reports which state it is in.

Note that the constructors fail fast: they open a connection while building, so
an unreachable database is reported at construction rather than at first query.

## Bootstrap scripts

`Database.setup(Class<?>, String)` runs every `*.sql` resource under the given
folder, in filename order, on every call. Two properties of that are worth
stating plainly, because both used to be folklore:

- **Nothing is tracked.** Every script runs on every boot, so each one must be
  idempotent — `IF NOT EXISTS` throughout, and `DROP` only in its `IF EXISTS`
  form. There is no migration ledger and no notion of a script having already
  been applied.
- **A script may contain several statements**, separated by semicolons.
  Semicolons inside string literals, quoted identifiers, and comments are left
  alone. A file that is only comments contributes nothing.

Both `--`/`#` line comments and `/* */` block comments are supported and are
passed through to the server untouched.

Scripts are interpolated before execution: `${database}` becomes the configured
database name and `${prefix}` the configured table prefix.

If a statement fails, the resulting `SQLException` names the script and which
statement within it failed.

## Documentation

Documentation is in progress, but has not been completed at this time. A wiki
may be provided when documentation has been completed.

## License

This library has been released under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0.html).
