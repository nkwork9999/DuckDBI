# DuckDBI

Browser-based BI dashboard for DuckDB. Run SQL queries and build interactive dashboards directly from the DuckDB CLI.

## Features

- **Embedded Web UI** - Single-page application served from within DuckDB
- **SQL Query Editor** - Execute queries against your DuckDB database from the browser
- **Interactive Charts** - Bar, Line, Scatter, Area, 3D Scatter (Plotly.js)
- **Dashboard Builder** - Drag-and-drop layout with Gridstack.js
- **PDF Export** - Export dashboards via jsPDF + html2canvas
- **Markdown Support** - Preview Markdown stored in query result columns, or build reports with embedded SQL
- **Interface Languages** - English, Simplified Chinese, and Japanese; browser-language detection and saved selection

## Installation

```sql
INSTALL duckdbi FROM community;
LOAD duckdbi;
```

## Usage

```sql
-- Start the BI server (opens browser)
SELECT duckdbi_start('localhost', 8080);

-- Stop the server
SELECT duckdbi_stop();
```

Then open `http://localhost:8080` in your browser.

## Interface language

Use the **Language** selector in the page header (English / 简体中文 / 日本語).
The initial language follows your browser, with English as the fallback. Your
choice is saved locally and shared between the main page and `/advanced`.
Database table names, column names, SQL, and report contents are not translated.

## Preview Markdown from a query

Query the column that contains your blog or document text, for example:

```sql
SELECT title, body FROM posts ORDER BY title LIMIT 20;
```

In **Markdown preview**, select `body` under **Markdown column**, then click
**Preview Markdown**. Each nonempty text value appears as a separate document.
This works on the main page and in the **Query** tab at `/advanced`.
Headings, lists, emphasis, code blocks, links, and tables are supported. Preview
shows up to 200 result rows; NULL and non-text values are skipped. Running a new
successful query clears the previous preview.

Stored Markdown is display-only: SQL code blocks are shown as code and never
executed. HTML is sanitized; executable elements, event handlers, embedded media,
and unsafe link protocols are removed. Images are omitted. Marked.js must load
from the existing CDN; a failed load displays an error. The separate report
editor at `/advanced` retains its existing embedded-SQL workflow.

## Build from Source

```bash
git clone --recurse-submodules https://github.com/nkwork9999/duckdbi.git
cd duckdbi
make release
```

The built extension will be at `build/release/extension/duckdbi/duckdbi.duckdb_extension`.

## Architecture

- **C++ Extension** - DuckDB loadable extension using cpp-httplib for the HTTP server
- **Embedded SPA** - HTML/CSS/JS compiled into the extension binary as a string literal
- **REST API** - `/api/query` endpoint proxies SQL to DuckDB and returns JSON results
- **CDN Dependencies** - Plotly.js, Gridstack.js, jsPDF, html2canvas, Marked.js (loaded at runtime)

## Requirements

- DuckDB v1.4.2+
- Internet connection (CDN libraries loaded on first access; cached by browser afterward)
- Modern web browser

## License

MIT
