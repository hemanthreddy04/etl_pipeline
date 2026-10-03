# Bundled library

`sql-asm.js` is the asm.js build of [sql.js](https://github.com/sql-js/sql.js) 1.10.3 (SQLite 3.45.2 compiled to JavaScript),
taken unchanged from the project's GitHub release `v1.10.3`. It is MIT licensed: see `LICENSE-sql.js` and `AUTHORS-sql.js`.
SQLite itself is in the public domain.

The asm.js build is used instead of the WebAssembly one because it is a single script with no second file to fetch,
so it also runs on pages with a strict content policy.
