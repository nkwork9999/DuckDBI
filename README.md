# DuckDBI

Browser-based BI dashboard for DuckDB. Run SQL queries and build interactive dashboards directly from the DuckDB CLI.

## Features

- **Embedded Web UI** - Single-page application served from within DuckDB
- **SQL Query Editor** - Execute queries against your DuckDB database from the browser
- **Interactive Charts** - Bar, Line, Scatter, Area, 3D Scatter (Plotly.js)
- **Dashboard Builder** - Drag-and-drop layout with Gridstack.js
- **PDF Export** - Export dashboards via jsPDF + html2canvas
- **Markdown Support** - Add text panels with Markdown rendering

## Installation

```sql
INSTALL duckdbi FROM community;
LOAD duckdbi;
```

## Usage

```sql
-- Start the BI server (opens browser)
SELECT duckdbi_start('mydata.db', 8080);

-- Stop the server
SELECT duckdbi_stop();
```

Then open `http://localhost:8080` in your browser.

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
