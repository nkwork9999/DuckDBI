# Regression tests

Run `bash scripts/test-unit.sh` for C++ encoding/server helper tests and frontend
syntax and logic checks. Run `npm ci --prefix test`, then
`npx --prefix test playwright install chromium` and `node test/browser.test.cjs`
for browser regressions. Set `UI_BROWSER_CHANNEL=chrome` to use installed Chrome.

The browser tests cover SQL identifier quoting, safe result rendering, NULL and
zero values, table filtering/retry, keyboard execution, duplicate request
prevention, server errors, and mobile layout. DuckGL also checks spatial schema
selection and map bounds; DuckDBI checks chart data and CSV downloads.
HTTP responses and remote visualization libraries are mocked for reproducibility;
these tests do not replace native DuckDB extension or real map-tile integration.

A native HTTP integration test runs the production server against an actual
DuckDB database (without extension-loader metadata). With matching DuckDB headers
and shared library installed, compile `test/native_http_test.cpp` with C++17,
`-I src/include`, the DuckDB include/library paths, and `-lduckdb`, then run it.
It checks HTML routing, real table discovery, JSON values and errors, and restart.
It uses loopback ports 19881 (DuckGL) and 19882 (DuckDBI).

DuckDBI also tests language switching, reload/route persistence, automatic Chinese
locale selection with unavailable localStorage, and Markdown previews on both
routes. Markdown regressions cover headings/lists/code, multiple documents,
NULL/non-text columns, query replacement, missing parser errors, and HTML/link
sanitization. SQL fences in stored Markdown must not issue further queries.
The existing Marked v9 parser is served from a pinned dev dependency in browser
tests, rather than mocked or downloaded from the CDN.
