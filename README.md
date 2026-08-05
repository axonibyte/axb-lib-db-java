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
Pass `initializationFailTimeout` as a negative value to turn that off.

## Pool settings

The properties map given to the six-argument constructor carries two different
kinds of thing, and they are now told apart:

- **Pool settings** are applied to the pool itself. Recognised keys, using
  Hikari's own names: `connectionTimeout`, `idleTimeout`, `maxLifetime`,
  `keepaliveTime`, `validationTimeout`, `leakDetectionThreshold`,
  `initializationFailTimeout`, `maximumPoolSize`, `minimumIdle`, `poolName`,
  `connectionTestQuery`, `connectionInitSql`, `autoCommit`, `readOnly`. A value
  that cannot be parsed, or that the setting rejects, fails construction rather
  than being dropped.
- **Everything else** is passed to the JDBC driver as a data source property,
  which is what all of them used to be.

> **Behaviour change in 0.5.0.** Every entry previously went to the driver, so
> any pool setting in that map was accepted and then silently ignored — including
> four in this library's own defaults. Deployments relying on those defaults have
> been running on Hikari's values, and will now get the configured ones: a
> 3-minute `maxLifetime` and a 30-second `idleTimeout`, both shorter than
> Hikari's, so expect more connection turnover. `leakDetectionThreshold` was
> raised from 5s to 60s at the same time — it had never been in force, and 5s is
> short enough that `setup()`'s own connection would trip it on any project with
> more than a handful of migration scripts.

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
