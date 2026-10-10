#pragma once
#include <string>
static const std::string DUCKDBI_SIMPLE_HTML =
    std::string(
        R"DUCKUI(<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>DuckDBI</title><script src="/ui.js"></script><script src="https://unpkg.com/marked@9/marked.min.js"></script><script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script><style>
:root{color-scheme:light;--ink:#1e293b;--muted:#64748b;--line:#e2e8f0;--accent:#176b52;--surface:#fff;font-family:system-ui,-apple-system,sans-serif}
*{box-sizing:border-box}body{margin:0;background:#f6f8fa;color:var(--ink);font-size:14px}button,input,select,textarea{font:inherit}button,a,input,select,textarea{outline-offset:3px}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible,summary:focus-visible{outline:3px solid #69b99e}button{cursor:pointer;border:1px solid var(--line);border-radius:6px;background:white;padding:8px 12px;color:var(--ink)}button:hover{background:#edf5f1}button:disabled{cursor:wait;opacity:.55}.primary{background:var(--accent);color:white;border-color:var(--accent)}.primary:hover{background:#11533f}header{height:68px;padding:0 24px;display:flex;align-items:center;justify-content:space-between;background:white;border-bottom:1px solid var(--line);gap:12px}h1{font-size:20px;margin:0;letter-spacing:-.5px}h2{font-size:15px;margin:0}p{line-height:1.5}.muted,.hint{color:var(--muted)}.hint{font-size:12px;margin:8px 0 0}.brand{display:flex;align-items:baseline;gap:12px}.workspace{display:grid;grid-template-columns:240px minmax(0,1fr);min-height:calc(100vh - 68px)}aside{background:white;border-right:1px solid var(--line);padding:20px;min-width:0}.toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}.search{width:100%;padding:9px 10px;border:1px solid var(--line);border-radius:6px;margin:16px 0 10px}.table-list{display:grid;gap:4px;max-height:65vh;overflow:auto}.table-button{width:100%;text-align:left;border-color:transparent;overflow-wrap:anywhere}.table-button[aria-current=true]{background:#e8f4ee;color:var(--accent);border-color:#b8dfcd}.table-schema{display:block;color:var(--muted);font-size:11px;margin-top:3px}.main{padding:24px;min-width:0}.card{background:white;border:1px solid var(--line);border-radius:9px;padding:20px;margin-bottom:20px}textarea{width:100%;min-height:135px;resize:vertical;padding:14px;border:1px solid var(--line);border-radius:6px;background:#fafbfc;color:var(--ink);font-family:ui-monospace,SFMono-Regular,monospace;font-size:13px;line-height:1.6;margin:14px 0}#status{font-size:13px;color:var(--muted);overflow-wrap:anywhere}#status[data-error=true]{color:#a62626}.result-scroll{overflow:auto;max-height:48vh;margin-top:16px}.results{border-collapse:collapse;min-width:100%;font-size:13px}.results th,.results td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line);white-space:pre-wrap;max-width:340px;overflow-wrap:anywhere;vertical-align:top}.results th{background:#f7faf8;position:sticky;top:0;font-weight:600}.null{color:#94a3b8;font-style:italic}.empty{padding:20px 0;color:var(--muted)}details>summary{cursor:pointer;font-weight:600;min-height:24px}select{border:1px solid var(--line);border-radius:5px;padding:7px;background:white}label{font-weight:500}.chart-controls{display:flex;gap:16px;align-items:end;flex-wrap:wrap;margin-top:18px}.chart-controls label{display:grid;gap:6px}a{color:var(--accent)}[hidden]{display:none!important}@media(max-width:720px){header{height:auto;min-height:68px;padding:16px}.brand .muted{display:none}.workspace{grid-template-columns:1fr}aside{border-right:0;border-bottom:1px solid var(--line);padding:16px}.table-list{max-height:150px}.main{padding:16px}.card{padding:16px}.results th,.results td{padding:8px}.header-link{font-size:12px}}
.markdown-preview{overflow-wrap:anywhere}.markdown-document{padding:16px 0;border-bottom:1px solid var(--line,#334155)}.markdown-document pre{overflow:auto;padding:12px;background:#80808018}.markdown-document table{border-collapse:collapse}.markdown-document th,.markdown-document td{border:1px solid #94a3b8;padding:6px}.markdown-document img{max-width:100%} header{height:auto;min-height:68px;flex-wrap:wrap}.language-controls{display:flex;align-items:center;gap:8px}@media(max-width:720px){header{align-items:flex-start}}
</style></head><body><header><div class="brand"><h1>DuckDBI</h1><span class="muted" data-i18n="Your data, one query at a time">Your data, one query at a time</span></div><div class="language-controls"><label data-i18n="Language" for="ui-language">Language</label> <select id="ui-language"><option value="en">English</option><option value="zh-CN">简体中文</option><option value="ja">日本語</option></select></div><a class="header-link" href="/advanced" data-i18n="Dashboards &amp; reports">Dashboards &amp; reports</a></header><div class="workspace"><aside aria-label="Database tables" data-i18n-aria-label="Database tables"><div class="toolbar"><h2 data-i18n="Tables">Tables</h2><button id="refresh" type="button" aria-label="Refresh tables" data-i18n-aria-label="Refresh tables" data-i18n="Refresh">Refresh</button></div><label for="table-search" class="hint" data-i18n="Find a table">Find a table</label><input id="table-search" class="search" type="search" placeholder="Filter tables" data-i18n-placeholder="Filter tables" autocomplete="off"><div id="tables" class="table-list">Loading tables…</div></aside><main class="main"><p id="status" role="status" aria-live="polite" data-i18n="Ready">Ready</p><section class="card" aria-labelledby="query-heading"><div class="toolbar"><h2 id="query-heading" data-i18n="SQL query">SQL query</h2><button class="primary" id="run" type="button" data-i18n="Run query">Run query</button></div><label for="sql-editor" class="hint" data-i18n="Select a table or write a query">Select a table or write a query</label><textarea id="sql-editor" spellcheck="false">SELECT 1 AS id, 'Hello DuckDB' AS message;</textarea><p class="hint" data-i18n="Ctrl / ⌘ + Enter to run">Ctrl / ⌘ + Enter to run</p></section><section class="card" aria-labelledby="results-heading"><div class="toolbar"><h2 id="results-heading" data-i18n="Results">Results</h2><span id="result-meta" class="muted"></span></div><div id="result-content" class="result-scroll" aria-busy="false"><p class="empty" data-i18n="Run a query to see your data.">Run a query to see your data.</p></div></section><details class="card"><summary data-i18n="Markdown preview">Markdown preview</summary><div class="chart-controls"><label><span data-i18n="Markdown column">Markdown column</span><select id="markdown-column"></select></label><button id="preview-markdown" type="button" disabled data-i18n="Preview Markdown">Preview Markdown</button></div><p class="hint" data-i18n="Select a result column containing Markdown text.">Select a result column containing Markdown text.</p><div id="markdown-preview" class="markdown-preview"></div></details><details class="card"><summary data-i18n="Chart results">Chart results</summary><div class="chart-controls"><label><span data-i18n="Type">Type</span><select id="chart-type"><option value="bar" data-i18n="Bar">Bar</option><option value="line" data-i18n="Line">Line</option><option value="scatter" data-i18n="Scatter">Scatter</option><option value="area" data-i18n="Area">Area</option><option value="scatter3d" data-i18n="3D scatter">3D scatter</option></select></label><label><span data-i18n="X column">X column</span><select id="chart-x"></select></label><label><span data-i18n="Y column">Y column</span><select id="chart-y"></select></label><label id="chart-z-label" hidden><span data-i18n="Z column">Z column</span><select id="chart-z"></select></label><button id="draw-chart" type="button" data-i18n="Draw chart">Draw chart</button></div><p id="chart-hint" class="hint">Run a query to choose chart columns.</p><div id="chart" style="min-height:320px"></div></details><button id="download" type="button" disabled data-i18n="Download results as CSV">Download results as CSV</button></main></div><script>
const t = DuckDBIUI.t;
)DUCKUI") +
    std::string(R"DUCKUI2(const $ = id => document.getElementById(id);
let tables = [], selectedTable = null, lastRows = [], queryGeneration = 0;
function quoteIdentifier(value) { return '"' + String(value).replace(/"/g, '""') + '"'; }
function qualifiedTable(table) { return quoteIdentifier(table.table_schema || 'main') + '.' + quoteIdentifier(table.table_name); }
function setStatus(message, error = false) { $('status').textContent = t(message); $('status').dataset.error = String(error); }
async function api(url, options) {
    const response = await fetch(url, options);
    let value;
    try { value = await response.json(); } catch (_) { throw new Error(t("The server returned an invalid response.")); }
    if (!response.ok || value?.error) throw new Error(value?.error || 'Request failed (' + response.status + ').');
    return value;
}
function renderTable(target, rows, limit = 200) {
    target.replaceChildren();
    if (!Array.isArray(rows) || !rows.length) { const empty = document.createElement('p'); empty.className = 'empty'; empty.textContent = t("No rows returned."); target.appendChild(empty); return; }
    const columns = Object.keys(rows[0]);
    const table = document.createElement('table'); table.className = 'results';
    const head = table.createTHead().insertRow();
    for (const column of columns) { const cell = document.createElement('th'); cell.scope = 'col'; cell.textContent = column; head.appendChild(cell); }
    const body = table.createTBody();
    for (const row of rows.slice(0, limit)) {
        const tr = body.insertRow();
        for (const column of columns) { const cell = tr.insertCell(); const value = row[column]; cell.textContent = value === null ? 'NULL' : typeof value === 'object' ? JSON.stringify(value) : String(value); if (value === null) cell.className = 'null'; }
    }
    target.appendChild(table);
    if (rows.length > limit) { const note = document.createElement('p'); note.className = 'hint'; note.textContent = 'Showing ' + limit + ' of ' + rows.length.toLocaleString() + ' rows.'; target.appendChild(note); }
}
function renderTables() {
    const list = $('tables'); list.replaceChildren();
    const filter = $('table-search').value.trim().toLocaleLowerCase();
    const matches = tables.filter(table => (table.table_schema + '.' + table.table_name).toLocaleLowerCase().includes(filter));
    if (!matches.length) { list.textContent = tables.length ? t("No matching tables.") : t("No tables found."); return; }
    for (const table of matches) {
        const button = document.createElement('button'); button.type = 'button'; button.className = 'table-button';
        button.setAttribute('aria-current', String(selectedTable === table));
        const name = document.createElement('span'); name.textContent = table.table_name;
        const schema = document.createElement('span'); schema.className = 'table-schema'; schema.textContent = table.table_schema || 'main';
        button.append(name, schema); button.addEventListener('click', () => selectTable(table)); list.appendChild(button);
    }
}
async function refreshTables() {
    const button = $('refresh'); button.disabled = true;
    try { const result = await api('/api/tables'); if (!Array.isArray(result)) throw new Error(t("Invalid table list.")); tables = result; renderTables(); setStatus(t("Tables updated.")); }
    catch (error) { $('tables').textContent = t("Could not load tables. Use Refresh to retry."); setStatus(error.message, true); }
    finally { button.disabled = false; }
}
async function executeQuery() {
    const sql = $('sql-editor').value;
    if (!sql.trim()) { setStatus(t("Enter a SQL query first."), true); $('sql-editor').focus(); return; }
    if ($('run').disabled) return;
    const generation = ++queryGeneration, start = performance.now();
    $('run').disabled = true; $('run').textContent = t("Running…"); $('result-content').setAttribute('aria-busy', 'true'); setStatus(t("Running query…"));
    try {
        const rows = await api('/api/query', {method:'POST', headers:{'Content-Type':'text/plain'}, body:sql});
        if (generation !== queryGeneration) return;
        if (!Array.isArray(rows)) throw new Error(t("Invalid query results."));
        lastRows = rows; renderTable($('result-content'), rows); $('result-meta').textContent = rows.length.toLocaleString() + ' rows · ' + ((performance.now()-start)/1000).toFixed(2) + ' s';
        queryCompleted(); setStatus(t("Query complete."));
    } catch (error) { if (generation === queryGeneration) { setStatus(error.message, true); } }
    finally { if (generation === queryGeneration) { $('run').disabled = false; $('run').textContent = t("Run query"); $('result-content').setAttribute('aria-busy', 'false'); } }
}
$('run').addEventListener('click', executeQuery);
$('refresh').addEventListener('click', refreshTables);
$('table-search').addEventListener('input', renderTables);
$('sql-editor').addEventListener('keydown', event => { if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') { event.preventDefault(); executeQuery(); } });

function selectTable(table) { selectedTable = table; renderTables(); $('sql-editor').value = 'SELECT * FROM ' + qualifiedTable(table) + ' LIMIT 200'; $('sql-editor').focus(); executeQuery(); }
function queryCompleted() {
    const columns = Object.keys(lastRows[0] || {}).filter(column => lastRows.some(row => typeof row[column] === 'string'));
    const select = $('markdown-column'), selected = select.value; select.replaceChildren();
    for (const column of columns) { const option = document.createElement('option'); option.value = column; option.textContent = column; select.appendChild(option); }
    if (columns.includes(selected)) select.value = selected;
    $('preview-markdown').disabled = !columns.length; $('markdown-preview').replaceChildren();
    $('download').disabled = !lastRows.length;
    for (const id of ['chart-x','chart-y','chart-z']) { const select = $(id); select.replaceChildren(); for (const column of Object.keys(lastRows[0] || {})) { const option = document.createElement('option'); option.value = column; option.textContent = column; select.appendChild(option); } }
    $('chart-y').selectedIndex = $('chart-y').options.length > 1 ? 1 : 0; $('chart-z').selectedIndex = $('chart-z').options.length > 2 ? 2 : 0;
    $('chart').replaceChildren(); $('chart-hint').textContent = t("Choose columns from the latest query results.");
}
function csvCell(value) { const text = value == null ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value); return '"' + text.replace(/"/g, '""') + '"'; }
function downloadCSV() {
    if (!lastRows.length) return;
    const columns = Object.keys(lastRows[0]); const csv = [columns.map(csvCell).join(','), ...lastRows.map(row => columns.map(column => csvCell(row[column])).join(','))].join('\r\n');
    const url = URL.createObjectURL(new Blob([csv], {type:'text/csv;charset=utf-8'})); const link = document.createElement('a'); link.href = url; link.download = 'query-results.csv'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}
async function drawChart() {
    if (!lastRows.length) { setStatus(t("Run a query before drawing a chart."), true); return; }
    if (!window.Plotly) { setStatus(t("Chart library could not load. Check your connection and reload."), true); return; }
    const type = $('chart-type').value, x = $('chart-x').value, y = $('chart-y').value, z = $('chart-z').value;
    const numeric = value => value === null || value === '' ? null : Number.isFinite(Number(value)) ? Number(value) : null;
    const trace = {x:lastRows.map(row => row[x]), y:lastRows.map(row => numeric(row[y])), type:type === 'bar' ? 'bar' : type === 'scatter3d' ? 'scatter3d' : 'scatter', marker:{color:'#176b52'}, mode:type === 'scatter' || type === 'scatter3d' ? 'markers' : 'lines+markers'};
    if (type === 'area') trace.fill = 'tozeroy'; if (type === 'scatter3d') trace.z = lastRows.map(row => numeric(row[z]));
    try { await Plotly.newPlot('chart', [trace], {margin:{t:20,r:20,b:50,l:55}, xaxis:{title:x}, yaxis:{title:y}, paper_bgcolor:'#fff', plot_bgcolor:'#fff', scene:{xaxis:{title:x},yaxis:{title:y},zaxis:{title:z}}}, {responsive:true,displaylogo:false}); $('chart-hint').textContent = t("Chart uses the latest query results."); }
    catch (error) { setStatus('Could not draw chart: ' + error.message, true); }
}
$('download').addEventListener('click', downloadCSV); $('draw-chart').addEventListener('click', drawChart);
$('chart-type').addEventListener('change', () => { $('chart-z-label').hidden = $('chart-type').value !== 'scatter3d'; });
$('preview-markdown').addEventListener('click', () => { try { DuckDBIUI.preview($('markdown-preview'),lastRows,$('markdown-column').value); } catch (error) { setStatus(error.message,true); } });
$('markdown-column').addEventListener('change', () => $('markdown-preview').replaceChildren());
DuckDBIUI.init();
document.addEventListener('duckdbi-language-change', () => { setStatus(t(t("Ready"))); });
refreshTables();
</script></body></html>)DUCKUI2");
