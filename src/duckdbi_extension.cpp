#define DUCKDB_EXTENSION_MAIN

#include "duckdbi_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "httplib_wrapper.hpp"

#include <thread>
#include <atomic>
#include <memory>
#include <sstream>
#include <map>
#include <vector>
#include <mutex>
#include <cstdlib>

namespace duckdb {

// ============================================================================
// HTML/JavaScript UI - Embedded Single Page Application
// ============================================================================
static const char* DUCKDBI_HTML = R"HTML(
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DuckDBI - DuckDB BI Tool</title>
    
    <!-- Plotly.js for charts -->
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <!-- Gridstack.js for dashboard layout -->
    <script src="https://unpkg.com/gridstack@9/dist/gridstack-all.js"></script>
    <link href="https://unpkg.com/gridstack@9/dist/gridstack.min.css" rel="stylesheet"/>
    <!-- jsPDF & html2canvas for PDF export -->
    <script src="https://unpkg.com/jspdf@2.5.1/dist/jspdf.umd.min.js"></script>
    <script src="https://html2canvas.hertzen.com/dist/html2canvas.min.js"></script>
    <!-- Marked.js for Markdown -->
    <script src="https://unpkg.com/marked@9/marked.min.js"></script>
    
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a; 
            color: #e2e8f0; 
            min-height: 100vh;
        }
        
        /* Navigation */
        .navbar {
            background: #1e293b;
            padding: 0 20px;
            display: flex;
            align-items: center;
            height: 56px;
            border-bottom: 1px solid #334155;
        }
        .logo {
            font-size: 22px;
            font-weight: bold;
            color: #38bdf8;
            margin-right: 40px;
        }
        .logo span { color: #f472b6; }
        .nav-tabs { display: flex; gap: 5px; }
        .nav-tab {
            padding: 10px 20px;
            background: transparent;
            border: none;
            color: #94a3b8;
            cursor: pointer;
            border-radius: 8px 8px 0 0;
            font-size: 14px;
            transition: all 0.2s;
        }
        .nav-tab:hover { background: #334155; color: #e2e8f0; }
        .nav-tab.active { background: #0f172a; color: #38bdf8; }
        .nav-right { margin-left: auto; display: flex; gap: 10px; }
        
        /* Layout */
        .main-container { display: flex; height: calc(100vh - 56px); }
        .sidebar {
            width: 260px;
            background: #1e293b;
            border-right: 1px solid #334155;
            display: flex;
            flex-direction: column;
        }
        .sidebar-header {
            padding: 12px 15px;
            border-bottom: 1px solid #334155;
            font-weight: 600;
            color: #38bdf8;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .sidebar-content { flex: 1; overflow-y: auto; padding: 10px; }
        .sidebar-footer { padding: 10px; border-top: 1px solid #334155; }
        .content-area { flex: 1; overflow: auto; padding: 20px; }
        
        /* Tab Content */
        .tab-content { display: none; height: 100%; }
        .tab-content.active { display: flex; }
        
        /* Buttons */
        .btn {
            padding: 8px 14px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 13px;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }
        .btn-sm { padding: 4px 10px; font-size: 12px; }
        .btn-primary { background: #3b82f6; color: white; }
        .btn-primary:hover { background: #2563eb; }
        .btn-success { background: #10b981; color: white; }
        .btn-success:hover { background: #059669; }
        .btn-danger { background: #ef4444; color: white; }
        .btn-secondary { background: #475569; color: white; }
        .btn-secondary:hover { background: #334155; }
        .btn-block { width: 100%; justify-content: center; }
        
        /* Table Items */
        .table-item {
            padding: 10px 12px;
            margin: 4px 0;
            background: #334155;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            border-left: 3px solid transparent;
        }
        .table-item:hover { background: #475569; border-left-color: #38bdf8; }
        .table-item.active { background: #475569; border-left-color: #38bdf8; }
        .table-name { font-weight: 600; color: #e2e8f0; font-size: 13px; }
        .table-info { font-size: 11px; color: #94a3b8; margin-top: 2px; }
        
        /* Cards */
        .card {
            background: #1e293b;
            border-radius: 10px;
            border: 1px solid #334155;
        }
        .card-header {
            padding: 12px 16px;
            border-bottom: 1px solid #334155;
            font-weight: 600;
            font-size: 14px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .card-body { padding: 16px; }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-bottom: 16px;
        }
        .stat-card {
            background: #334155;
            padding: 16px;
            border-radius: 8px;
            text-align: center;
        }
        .stat-value { font-size: 26px; font-weight: bold; color: #38bdf8; }
        .stat-label { font-size: 11px; color: #94a3b8; margin-top: 4px; text-transform: uppercase; }
        
        /* Tables */
        .data-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        .data-table th, .data-table td {
            padding: 10px 12px;
            text-align: left;
            border-bottom: 1px solid #334155;
        }
        .data-table th {
            background: #334155;
            color: #94a3b8;
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
        }
        .data-table tr:hover { background: rgba(56,189,248,0.05); }
        .data-table .clickable { cursor: pointer; color: #38bdf8; }
        .data-table .clickable:hover { text-decoration: underline; }
        
        /* SQL Editor */
        .sql-editor {
            width: 100%;
            min-height: 100px;
            background: #0f172a;
            color: #e2e8f0;
            border: 1px solid #334155;
            border-radius: 6px;
            padding: 12px;
            font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
            font-size: 13px;
            resize: vertical;
            line-height: 1.5;
        }
        .sql-editor:focus { outline: none; border-color: #38bdf8; }
        
        /* Form */
        .form-group { margin-bottom: 12px; }
        .form-label { 
            display: block; 
            margin-bottom: 4px; 
            color: #94a3b8; 
            font-size: 11px;
            text-transform: uppercase;
            font-weight: 600;
        }
        .form-select, .form-input {
            width: 100%;
            padding: 8px 10px;
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 6px;
            color: #e2e8f0;
            font-size: 13px;
        }
        .form-select:focus, .form-input:focus { outline: none; border-color: #38bdf8; }
        
        /* Dashboard Grid */
        .grid-stack { background: transparent; min-height: 400px; }
        .grid-stack-item-content {
            background: #1e293b;
            border-radius: 10px;
            border: 1px solid #334155;
            overflow: hidden;
        }
        .gs-item-header {
            padding: 8px 12px;
            background: #334155;
            font-weight: 600;
            font-size: 13px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .gs-item-body { padding: 8px; height: calc(100% - 40px); }
        
        /* Report */
        .report-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
            height: 100%;
        }
        .report-pane { display: flex; flex-direction: column; height: 100%; }
        .report-editor {
            flex: 1;
            width: 100%;
            background: #0f172a;
            color: #e2e8f0;
            border: 1px solid #334155;
            border-radius: 6px;
            padding: 12px;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 13px;
            resize: none;
            line-height: 1.6;
        }
        .report-preview {
            flex: 1;
            background: #1e293b;
            border-radius: 6px;
            padding: 16px;
            overflow-y: auto;
        }
        .report-preview h1 { color: #38bdf8; font-size: 24px; margin-bottom: 12px; }
        .report-preview h2 { color: #e2e8f0; font-size: 18px; margin: 16px 0 8px; }
        .report-preview p { color: #94a3b8; line-height: 1.6; margin-bottom: 8px; }
        .report-preview pre { 
            background: #0f172a; 
            padding: 12px; 
            border-radius: 6px; 
            overflow-x: auto;
            margin: 8px 0;
        }
        .report-preview code { color: #38bdf8; font-size: 12px; }
        
        /* Modal */
        .modal-overlay {
            display: none;
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.7);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }
        .modal-overlay.show { display: flex; }
        .modal {
            background: #1e293b;
            border-radius: 10px;
            width: 550px;
            max-width: 90vw;
            max-height: 90vh;
            overflow: auto;
        }
        .modal-header {
            padding: 16px;
            border-bottom: 1px solid #334155;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-weight: 600;
        }
        .modal-body { padding: 16px; }
        .modal-footer {
            padding: 12px 16px;
            border-top: 1px solid #334155;
            display: flex;
            justify-content: flex-end;
            gap: 8px;
        }
        
        /* Chart Container */
        .chart-container {
            background: #1e293b;
            border-radius: 8px;
            padding: 12px;
            margin: 8px 0;
            min-height: 250px;
        }
        
        /* Chart Type Selector */
        .chart-types {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
            margin-bottom: 12px;
        }
        .chart-type-btn {
            padding: 12px 8px;
            background: #334155;
            border: 2px solid transparent;
            border-radius: 6px;
            cursor: pointer;
            text-align: center;
            transition: all 0.2s;
        }
        .chart-type-btn:hover { background: #475569; }
        .chart-type-btn.active { border-color: #38bdf8; background: #475569; }
        .chart-type-btn .icon { font-size: 20px; margin-bottom: 4px; }
        .chart-type-btn .label { font-size: 10px; color: #94a3b8; }
        
        /* Status Bar */
        .status-bar {
            position: fixed;
            bottom: 0; left: 0; right: 0;
            height: 28px;
            background: #1e293b;
            border-top: 1px solid #334155;
            padding: 0 16px;
            display: flex;
            align-items: center;
            font-size: 11px;
            color: #94a3b8;
        }
        .status-dot {
            width: 8px; height: 8px;
            border-radius: 50%;
            background: #10b981;
            margin-right: 8px;
        }
        
        /* Spinner */
        .spinner {
            width: 18px; height: 18px;
            border: 2px solid #334155;
            border-top-color: #38bdf8;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        
        /* Empty State */
        .empty-state {
            text-align: center;
            padding: 40px 20px;
            color: #64748b;
        }
        .empty-state .icon { font-size: 48px; margin-bottom: 12px; opacity: 0.5; }
    </style>
</head>
<body>
    <!-- Navigation -->
    <nav class="navbar">
        <div class="logo">Duck<span>DBI</span></div>
        <div class="nav-tabs">
            <button class="nav-tab active" onclick="showTab('explore')">📊 Explore</button>
            <button class="nav-tab" onclick="showTab('dashboard')">📈 Dashboard</button>
            <button class="nav-tab" onclick="showTab('report')">📝 Report</button>
            <button class="nav-tab" onclick="showTab('query')">💻 Query</button>
        </div>
        <div class="nav-right">
            <button class="btn btn-secondary" onclick="refreshTables()">🔄 Refresh</button>
        </div>
    </nav>

    <!-- Main Container -->
    <div class="main-container">
        
        <!-- ============ EXPLORE TAB ============ -->
        <div id="explore-tab" class="tab-content active">
            <div class="sidebar">
                <div class="sidebar-header">📁 Tables</div>
                <div class="sidebar-content" id="explore-tables"></div>
            </div>
            <div class="content-area" id="explore-content">
                <div class="empty-state">
                    <div class="icon">📊</div>
                    <p>Select a table from the sidebar to explore</p>
                </div>
            </div>
        </div>

        <!-- ============ DASHBOARD TAB ============ -->
        <div id="dashboard-tab" class="tab-content">
            <div class="sidebar">
                <div class="sidebar-header">
                    <span>📊 Charts</span>
                    <button class="btn btn-sm btn-primary" onclick="openChartBuilder()">+ Add</button>
                </div>
                <div class="sidebar-content" id="chart-list"></div>
                <div class="sidebar-footer">
                    <button class="btn btn-success btn-block" onclick="exportDashboardPDF()">📥 Export PDF</button>
                </div>
            </div>
            <div class="content-area">
                <div class="grid-stack" id="dashboard-grid"></div>
            </div>
        </div>

        <!-- ============ REPORT TAB ============ -->
        <div id="report-tab" class="tab-content">
            <div class="sidebar">
                <div class="sidebar-header">
                    <span>📄 Reports</span>
                    <button class="btn btn-sm btn-primary" onclick="newReport()">+ New</button>
                </div>
                <div class="sidebar-content" id="report-list"></div>
                <div class="sidebar-footer">
                    <button class="btn btn-success btn-block" onclick="exportReportPDF()">📥 Export PDF</button>
                </div>
            </div>
            <div class="content-area">
                <div class="report-container">
                    <div class="report-pane">
                        <div style="margin-bottom: 8px; display: flex; gap: 8px;">
                            <button class="btn btn-primary" onclick="runReport()">▶ Run</button>
                            <button class="btn btn-secondary" onclick="saveReport()">💾 Save</button>
                        </div>
                        <textarea id="report-editor" class="report-editor" placeholder="# Report Title

Write Markdown with embedded SQL:

```sql {chart: 'bar'}
SELECT category, SUM(amount) 
FROM sales GROUP BY category
```
"></textarea>
                    </div>
                    <div class="report-pane">
                        <div style="margin-bottom: 8px; font-weight: 600; font-size: 13px;">Preview</div>
                        <div id="report-preview" class="report-preview">
                            <div class="empty-state">
                                <div class="icon">📝</div>
                                <p>Click "Run" to preview your report</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- ============ QUERY TAB ============ -->
        <div id="query-tab" class="tab-content">
            <div class="sidebar">
                <div class="sidebar-header">📁 Tables</div>
                <div class="sidebar-content" id="query-tables"></div>
            </div>
            <div class="content-area">
                <div class="card" style="margin-bottom: 16px;">
                    <div class="card-header">SQL Query</div>
                    <div class="card-body">
                        <textarea id="sql-editor" class="sql-editor" placeholder="SELECT * FROM your_table LIMIT 100;">SELECT 1 as id, 'Hello DuckDBI!' as message;</textarea>
                        <div style="margin-top: 10px; display: flex; gap: 8px;">
                            <button class="btn btn-primary" onclick="executeQuery()">▶ Execute</button>
                            <button class="btn btn-secondary" onclick="formatSQL()">Format</button>
                        </div>
                    </div>
                </div>
                <div class="card">
                    <div class="card-header">
                        <span>Results</span>
                        <span id="query-stats" style="font-weight:normal;font-size:11px;color:#94a3b8;"></span>
                    </div>
                    <div class="card-body" style="overflow-x:auto;">
                        <div id="query-results">
                            <div class="empty-state">
                                <div class="icon">💻</div>
                                <p>Execute a query to see results</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Chart Builder Modal -->
    <div id="chart-modal" class="modal-overlay">
        <div class="modal">
            <div class="modal-header">
                <span>Add Chart</span>
                <button class="btn btn-sm btn-secondary" onclick="closeChartModal()">✕</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label class="form-label">Chart Type</label>
                    <div class="chart-types">
                        <div class="chart-type-btn active" data-type="bar" onclick="selectChartType('bar')">
                            <div class="icon">📊</div><div class="label">Bar</div>
                        </div>
                        <div class="chart-type-btn" data-type="line" onclick="selectChartType('line')">
                            <div class="icon">📈</div><div class="label">Line</div>
                        </div>
                        <div class="chart-type-btn" data-type="pie" onclick="selectChartType('pie')">
                            <div class="icon">🥧</div><div class="label">Pie</div>
                        </div>
                        <div class="chart-type-btn" data-type="scatter" onclick="selectChartType('scatter')">
                            <div class="icon">⚬</div><div class="label">Scatter</div>
                        </div>
                        <div class="chart-type-btn" data-type="area" onclick="selectChartType('area')">
                            <div class="icon">▨</div><div class="label">Area</div>
                        </div>
                        <div class="chart-type-btn" data-type="histogram" onclick="selectChartType('histogram')">
                            <div class="icon">▤</div><div class="label">Histogram</div>
                        </div>
                        <div class="chart-type-btn" data-type="box" onclick="selectChartType('box')">
                            <div class="icon">☐</div><div class="label">Box</div>
                        </div>
                        <div class="chart-type-btn" data-type="heatmap" onclick="selectChartType('heatmap')">
                            <div class="icon">🔥</div><div class="label">Heatmap</div>
                        </div>
                    </div>
                </div>
                <div class="form-group">
                    <label class="form-label">Title</label>
                    <input type="text" id="chart-title" class="form-input" placeholder="My Chart">
                </div>
                <div class="form-group">
                    <label class="form-label">SQL Query</label>
                    <textarea id="chart-sql" class="sql-editor" style="min-height:80px;" placeholder="SELECT category, SUM(amount) FROM sales GROUP BY category"></textarea>
                </div>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
                    <div class="form-group">
                        <label class="form-label">X Axis Column</label>
                        <input type="text" id="chart-x" class="form-input" placeholder="category">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Y Axis Column</label>
                        <input type="text" id="chart-y" class="form-input" placeholder="sum">
                    </div>
                </div>
                <div id="chart-preview-area"></div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closeChartModal()">Cancel</button>
                <button class="btn btn-primary" onclick="previewChart()">Preview</button>
                <button class="btn btn-success" onclick="addChart()">Add</button>
            </div>
        </div>
    </div>

    <!-- Column Detail Modal -->
    <div id="column-modal" class="modal-overlay">
        <div class="modal" style="width:700px;">
            <div class="modal-header">
                <span id="column-modal-title">Column Details</span>
                <button class="btn btn-sm btn-secondary" onclick="closeColumnModal()">✕</button>
            </div>
            <div class="modal-body" id="column-modal-body"></div>
        </div>
    </div>

    <!-- Status Bar -->
    <div class="status-bar">
        <div class="status-dot"></div>
        <span>Connected to DuckDB</span>
        <span style="margin-left:auto;" id="status-msg">Ready</span>
    </div>

    <script>
    // ============================================================================
    // State
    // ============================================================================
    let tables = [];
    let currentTable = null;
    let dashboardCharts = [];
    let gridStack = null;
    let chartCounter = 0;
    let selectedChartType = 'bar';

    // ============================================================================
    // API
    // ============================================================================
    async function api(url, opts = {}) {
        try {
            const res = await fetch(url, opts);
            return await res.json();
        } catch (e) {
            console.error('API Error:', e);
            setStatus('Error: ' + e.message, true);
            return { error: e.message };
        }
    }

    async function runSQL(sql) {
        return api('/api/query', {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain' },
            body: sql
        });
    }

    function setStatus(msg, isErr = false) {
        const el = document.getElementById('status-msg');
        el.textContent = msg;
        el.style.color = isErr ? '#ef4444' : '#94a3b8';
    }

    // ============================================================================
    // Navigation
    // ============================================================================
    function showTab(name) {
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
        document.getElementById(name + '-tab').classList.add('active');
        event.target.classList.add('active');
        
        if (name === 'dashboard' && !gridStack) initGrid();
    }

    // ============================================================================
    // Tables
    // ============================================================================
    async function refreshTables() {
        setStatus('Loading tables...');
        const data = await api('/api/tables');
        if (data.error) { setStatus('Failed to load', true); return; }
        tables = data;
        renderTables();
        setStatus('Loaded ' + tables.length + ' tables');
    }

    function renderTables() {
        const html = tables.length ? tables.map(t => `
            <div class="table-item" onclick="selectTable('${t.table_name}')">
                <div class="table-name">${t.table_name}</div>
                <div class="table-info">${t.table_schema || 'main'}</div>
            </div>
        `).join('') : '<div class="empty-state"><p>No tables found</p></div>';
        
        document.getElementById('explore-tables').innerHTML = html;
        document.getElementById('query-tables').innerHTML = html;
    }

    // ============================================================================
    // Explore
    // ============================================================================
    async function selectTable(name) {
        currentTable = name;
        document.querySelectorAll('#explore-tables .table-item').forEach(el => el.classList.remove('active'));
        event.target.closest('.table-item').classList.add('active');
        
        setStatus('Profiling ' + name + '...');
        const profile = await api('/api/explore/profile/' + encodeURIComponent(name));
        if (profile.error) { setStatus('Profile failed', true); return; }
        
        renderProfile(name, profile);
        setStatus('Ready');
    }

    function renderProfile(name, p) {
        const el = document.getElementById('explore-content');
        el.innerHTML = `
            <div class="stats-grid">
                <div class="stat-card"><div class="stat-value">${fmt(p.row_count)}</div><div class="stat-label">Rows</div></div>
                <div class="stat-card"><div class="stat-value">${p.column_count}</div><div class="stat-label">Columns</div></div>
                <div class="stat-card"><div class="stat-value">${p.size_estimate || 'N/A'}</div><div class="stat-label">Est. Size</div></div>
            </div>
            <div class="card">
                <div class="card-header">📋 ${name}</div>
                <div class="card-body" style="overflow-x:auto;">
                    <table class="data-table">
                        <thead><tr>
                            <th>Column</th><th>Type</th><th>Nulls</th><th>Unique</th><th>Min</th><th>Max</th>
                        </tr></thead>
                        <tbody>
                            ${p.columns.map(c => `<tr>
                                <td class="clickable" onclick="showColumn('${name}','${c.name}')">${c.name}</td>
                                <td><code>${c.type}</code></td>
                                <td>${c.null_percent !== undefined ? c.null_percent.toFixed(1)+'%' : '-'}</td>
                                <td>${fmt(c.unique_count)}</td>
                                <td>${trunc(String(c.min ?? ''), 15)}</td>
                                <td>${trunc(String(c.max ?? ''), 15)}</td>
                            </tr>`).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
            <div class="card" style="margin-top:16px;">
                <div class="card-header">📊 Distribution</div>
                <div class="card-body"><div id="dist-chart" style="height:280px;"></div></div>
            </div>
        `;
        
        // Render histogram for first numeric column
        const numCol = p.columns.find(c => /INT|FLOAT|DOUBLE|DECIMAL|REAL|NUMERIC/i.test(c.type));
        if (numCol) renderDistribution(name, numCol.name);
    }

    async function renderDistribution(table, col) {
        const data = await runSQL(`SELECT "${col}" as v FROM "${table}" WHERE "${col}" IS NOT NULL LIMIT 10000`);
        if (data.error || !data.length) return;
        
        const vals = data.map(r => parseFloat(r.v)).filter(v => !isNaN(v));
        Plotly.newPlot('dist-chart', [{
            x: vals, type: 'histogram', marker: { color: '#38bdf8' }, nbinsx: 30
        }], plotLayout(col, 'Frequency'), plotConfig());
    }

    async function showColumn(table, col) {
        setStatus('Loading column...');
        const d = await api(`/api/explore/column/${encodeURIComponent(table)}/${encodeURIComponent(col)}`);
        if (d.error) { setStatus('Failed', true); return; }
        
        document.getElementById('column-modal-title').textContent = `${col} (${d.type})`;
        
        let html = '';
        if (d.is_numeric) {
            html = `
                <div class="stats-grid">
                    <div class="stat-card"><div class="stat-value">${d.min}</div><div class="stat-label">Min</div></div>
                    <div class="stat-card"><div class="stat-value">${d.max}</div><div class="stat-label">Max</div></div>
                    <div class="stat-card"><div class="stat-value">${parseFloat(d.avg||0).toFixed(2)}</div><div class="stat-label">Mean</div></div>
                    <div class="stat-card"><div class="stat-value">${parseFloat(d.std||0).toFixed(2)}</div><div class="stat-label">Std</div></div>
                    <div class="stat-card"><div class="stat-value">${d.median||'-'}</div><div class="stat-label">Median</div></div>
                    <div class="stat-card"><div class="stat-value">${(d.null_percent||0).toFixed(1)}%</div><div class="stat-label">Nulls</div></div>
                </div>
                <div id="col-chart" style="height:280px;margin-top:12px;"></div>
            `;
        } else {
            html = `
                <div class="stats-grid">
                    <div class="stat-card"><div class="stat-value">${fmt(d.unique_count)}</div><div class="stat-label">Unique</div></div>
                    <div class="stat-card"><div class="stat-value">${(d.null_percent||0).toFixed(1)}%</div><div class="stat-label">Nulls</div></div>
                    <div class="stat-card"><div class="stat-value">${d.min_length||'-'}</div><div class="stat-label">Min Len</div></div>
                    <div class="stat-card"><div class="stat-value">${d.max_length||'-'}</div><div class="stat-label">Max Len</div></div>
                </div>
                <h4 style="margin:16px 0 8px;font-size:13px;">Top Values</h4>
                <div id="col-chart" style="height:280px;"></div>
            `;
        }
        
        document.getElementById('column-modal-body').innerHTML = html;
        document.getElementById('column-modal').classList.add('show');
        
        setTimeout(() => {
            if (d.is_numeric && d.histogram) {
                Plotly.newPlot('col-chart', [{
                    x: d.histogram.bins, y: d.histogram.counts, type: 'bar', marker: { color: '#38bdf8' }
                }], plotLayout(col, 'Frequency'), plotConfig());
            } else if (d.top_values) {
                Plotly.newPlot('col-chart', [{
                    x: d.top_values.map(v => v.count),
                    y: d.top_values.map(v => v.value),
                    type: 'bar', orientation: 'h', marker: { color: '#38bdf8' }
                }], { ...plotLayout('Count', ''), margin: { l: 120, r: 20, t: 20, b: 40 } }, plotConfig());
            }
        }, 50);
        
        setStatus('Ready');
    }

    function closeColumnModal() { document.getElementById('column-modal').classList.remove('show'); }

    // ============================================================================
    // Dashboard
    // ============================================================================
    function initGrid() {
        gridStack = GridStack.init({ column: 12, cellHeight: 70, margin: 8, float: true }, '#dashboard-grid');
    }

    function openChartBuilder() { document.getElementById('chart-modal').classList.add('show'); }
    function closeChartModal() { 
        document.getElementById('chart-modal').classList.remove('show');
        document.getElementById('chart-preview-area').innerHTML = '';
    }

    function selectChartType(type) {
        selectedChartType = type;
        document.querySelectorAll('.chart-type-btn').forEach(b => b.classList.toggle('active', b.dataset.type === type));
    }

    async function previewChart() {
        const sql = document.getElementById('chart-sql').value;
        if (!sql) { alert('Enter SQL query'); return; }
        
        const data = await runSQL(sql);
        if (data.error) { alert('Query error: ' + data.error); return; }
        
        let area = document.getElementById('chart-preview-area');
        area.innerHTML = '<div id="chart-preview" style="height:200px;margin-top:12px;background:#0f172a;border-radius:6px;"></div>';
        
        const x = document.getElementById('chart-x').value;
        const y = document.getElementById('chart-y').value;
        renderChart('chart-preview', selectedChartType, data, x, y);
    }

    async function addChart() {
        const sql = document.getElementById('chart-sql').value;
        const title = document.getElementById('chart-title').value || 'Chart ' + (chartCounter + 1);
        const x = document.getElementById('chart-x').value;
        const y = document.getElementById('chart-y').value;
        
        if (!sql) { alert('Enter SQL'); return; }
        
        const id = 'chart-' + (++chartCounter);
        dashboardCharts.push({ id, type: selectedChartType, title, sql, x, y });
        
        gridStack.addWidget({
            w: 6, h: 4,
            content: `
                <div class="grid-stack-item-content">
                    <div class="gs-item-header">
                        ${title}
                        <button class="btn btn-sm btn-secondary" onclick="removeChart('${id}')">✕</button>
                    </div>
                    <div class="gs-item-body" id="${id}"></div>
                </div>
            `
        });
        
        const data = await runSQL(sql);
        if (!data.error) setTimeout(() => renderChart(id, selectedChartType, data, x, y), 50);
        
        closeChartModal();
        updateChartList();
    }

    function removeChart(id) {
        const el = document.getElementById(id)?.closest('.grid-stack-item');
        if (el) gridStack.removeWidget(el);
        dashboardCharts = dashboardCharts.filter(c => c.id !== id);
        updateChartList();
    }

    function updateChartList() {
        document.getElementById('chart-list').innerHTML = dashboardCharts.length ? 
            dashboardCharts.map(c => `<div class="table-item"><div class="table-name">${c.title}</div><div class="table-info">${c.type}</div></div>`).join('') :
            '<div class="empty-state"><p>No charts yet</p></div>';
    }

    function renderChart(id, type, data, xCol, yCol) {
        const el = document.getElementById(id);
        if (!el || !data?.length) return;
        
        const cols = Object.keys(data[0]);
        const x = xCol || cols[0];
        const y = yCol || cols[1] || cols[0];
        const xData = data.map(r => r[x]);
        const yData = data.map(r => parseFloat(r[y]) || r[y]);
        
        let traces = [];
        const layout = plotLayout(x, y);
        
        switch (type) {
            case 'bar': traces = [{ x: xData, y: yData, type: 'bar', marker: { color: '#38bdf8' } }]; break;
            case 'line': traces = [{ x: xData, y: yData, type: 'scatter', mode: 'lines+markers', line: { color: '#38bdf8' } }]; break;
            case 'area': traces = [{ x: xData, y: yData, type: 'scatter', fill: 'tozeroy', line: { color: '#38bdf8' } }]; break;
            case 'scatter': traces = [{ x: xData, y: yData, type: 'scatter', mode: 'markers', marker: { color: '#38bdf8', size: 8 } }]; break;
            case 'pie': 
                traces = [{ labels: xData, values: yData, type: 'pie', marker: { colors: ['#38bdf8','#f472b6','#10b981','#f59e0b','#8b5cf6'] } }];
                delete layout.xaxis; delete layout.yaxis;
                break;
            case 'histogram': traces = [{ x: yData, type: 'histogram', marker: { color: '#38bdf8' } }]; break;
            case 'box': traces = [{ y: yData, type: 'box', marker: { color: '#38bdf8' } }]; break;
            case 'heatmap': traces = [{ z: [yData], type: 'heatmap', colorscale: 'YlOrRd' }]; break;
            default: traces = [{ x: xData, y: yData, type: 'bar', marker: { color: '#38bdf8' } }];
        }
        
        Plotly.newPlot(id, traces, layout, plotConfig());
    }

    async function exportDashboardPDF() {
        setStatus('Generating PDF...');
        try {
            const canvas = await html2canvas(document.getElementById('dashboard-grid'), { backgroundColor: '#0f172a', scale: 2 });
            const { jsPDF } = window.jspdf;
            const pdf = new jsPDF('landscape', 'mm', 'a4');
            const w = pdf.internal.pageSize.getWidth();
            const h = pdf.internal.pageSize.getHeight();
            const ratio = Math.min(w / canvas.width, h / canvas.height);
            pdf.addImage(canvas.toDataURL('image/png'), 'PNG', 5, 5, canvas.width * ratio - 10, canvas.height * ratio - 10);
            pdf.save('dashboard.pdf');
            setStatus('PDF exported');
        } catch (e) { setStatus('Export failed: ' + e.message, true); }
    }

    // ============================================================================
    // Report
    // ============================================================================
    function newReport() {
        document.getElementById('report-editor').value = `# Monthly Report

## Overview

\`\`\`sql {chart: "bar"}
SELECT 'Jan' as month, 100 as sales
UNION ALL SELECT 'Feb', 150
UNION ALL SELECT 'Mar', 120
\`\`\`

## Summary

Positive trend observed.
`;
        runReport();
    }

    async function runReport() {
        const md = document.getElementById('report-editor').value;
        const preview = document.getElementById('report-preview');
        
        const sqlRe = /```sql\s*(?:\{([^}]*)\})?\n([\s\S]*?)```/g;
        let html = md;
        let idx = 0;
        const charts = [];
        
        html = html.replace(sqlRe, (_, opts, sql) => {
            const id = 'rpt-chart-' + (idx++);
            let type = 'table';
            if (opts) {
                const m = opts.match(/chart:\s*["']?(\w+)["']?/);
                if (m) type = m[1];
            }
            charts.push({ id, sql: sql.trim(), type });
            return `<div id="${id}" class="chart-container"></div>`;
        });
        
        preview.innerHTML = marked.parse(html);
        
        for (const c of charts) {
            const data = await runSQL(c.sql);
            const el = document.getElementById(c.id);
            if (!el) continue;
            if (data.error) {
                el.innerHTML = `<div style="color:#ef4444;">Error: ${data.error}</div>`;
            } else if (c.type === 'table') {
                renderTable(c.id, data);
            } else {
                const cols = Object.keys(data[0] || {});
                renderChart(c.id, c.type, data, cols[0], cols[1]);
            }
        }
    }

    function renderTable(id, data) {
        if (!data?.length) { document.getElementById(id).innerHTML = '<p style="color:#94a3b8;">No results</p>'; return; }
        const cols = Object.keys(data[0]);
        document.getElementById(id).innerHTML = `
            <table class="data-table">
                <thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
                <tbody>${data.slice(0, 100).map(r => `<tr>${cols.map(c => `<td>${r[c] ?? 'NULL'}</td>`).join('')}</tr>`).join('')}</tbody>
            </table>
            ${data.length > 100 ? `<p style="color:#94a3b8;font-size:11px;margin-top:8px;">Showing 100/${data.length}</p>` : ''}
        `;
    }

    function saveReport() {
        localStorage.setItem('duckdbi-report', document.getElementById('report-editor').value);
        setStatus('Report saved');
    }

    async function exportReportPDF() {
        setStatus('Generating PDF...');
        try {
            const canvas = await html2canvas(document.getElementById('report-preview'), { backgroundColor: '#1e293b', scale: 2 });
            const { jsPDF } = window.jspdf;
            const pdf = new jsPDF('portrait', 'mm', 'a4');
            const w = pdf.internal.pageSize.getWidth();
            const h = (canvas.height * w) / canvas.width;
            pdf.addImage(canvas.toDataURL('image/png'), 'PNG', 0, 0, w, h);
            pdf.save('report.pdf');
            setStatus('PDF exported');
        } catch (e) { setStatus('Export failed: ' + e.message, true); }
    }

    // ============================================================================
    // Query
    // ============================================================================
    async function executeQuery() {
        const sql = document.getElementById('sql-editor').value;
        if (!sql.trim()) return;
        
        setStatus('Executing...');
        const t0 = performance.now();
        const data = await runSQL(sql);
        const ms = ((performance.now() - t0) / 1000).toFixed(2);
        
        if (data.error) {
            document.getElementById('query-results').innerHTML = `<div style="color:#ef4444;">${data.error}</div>`;
            document.getElementById('query-stats').textContent = '';
            setStatus('Query failed', true);
            return;
        }
        
        document.getElementById('query-stats').textContent = `${data.length} rows · ${ms}s`;
        renderTable('query-results', data);
        setStatus('Ready');
    }

    function formatSQL() {
        let sql = document.getElementById('sql-editor').value;
        ['SELECT','FROM','WHERE','GROUP BY','ORDER BY','HAVING','LIMIT','JOIN','LEFT JOIN','RIGHT JOIN','INNER JOIN','ON','AND','OR','UNION'].forEach(kw => {
            sql = sql.replace(new RegExp('\\b' + kw + '\\b', 'gi'), '\n' + kw);
        });
        document.getElementById('sql-editor').value = sql.trim();
    }

    // ============================================================================
    // Helpers
    // ============================================================================
    function fmt(n) {
        if (n == null) return '-';
        if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
        if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
        return String(n);
    }
    function trunc(s, n) { return s.length > n ? s.slice(0, n) + '…' : s; }
    
    function plotLayout(xTitle, yTitle) {
        return {
            paper_bgcolor: 'transparent', plot_bgcolor: 'transparent',
            font: { color: '#e2e8f0', size: 11 },
            margin: { t: 20, r: 20, b: 40, l: 50 },
            xaxis: { title: xTitle, gridcolor: '#334155' },
            yaxis: { title: yTitle, gridcolor: '#334155' }
        };
    }
    function plotConfig() { return { responsive: true, displayModeBar: false }; }

    // ============================================================================
    // Init
    // ============================================================================
    document.addEventListener('DOMContentLoaded', () => {
        refreshTables();
        updateChartList();
        const saved = localStorage.getItem('duckdbi-report');
        if (saved) document.getElementById('report-editor').value = saved;
    });
    </script>
</body>
</html>
)HTML";

// ============================================================================
// HTTP Server
// ============================================================================
class DuckDBIServer {
private:
    duckdb::unique_ptr<httplib::Server> server;
    std::thread server_thread;
    std::atomic<bool> running{false};
    DatabaseInstance* db;
    int port;
    std::mutex mtx;

    // Convert result to JSON
    static std::string ToJSON(duckdb::unique_ptr<QueryResult> result) {
        if (!result || result->HasError()) {
            std::string msg = result ? result->GetError() : "Unknown error";
            std::string esc;
            for (char c : msg) {
                if (c == '"') esc += "\\\"";
                else if (c == '\\') esc += "\\\\";
                else if (c == '\n') esc += "\\n";
                else esc += c;
            }
            return "{\"error\":\"" + esc + "\"}";
        }
        
        std::string json = "[";
        bool first_row = true;
        
        while (true) {
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) break;
            
            for (idx_t row = 0; row < chunk->size(); row++) {
                if (!first_row) json += ",";
                json += "{";
                
                bool first_col = true;
                for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
                    if (!first_col) json += ",";
                    
                    json += "\"" + result->names[col] + "\":";
                    auto val = chunk->GetValue(col, row);
                    
                    if (val.IsNull()) {
                        json += "null";
                    } else {
                        auto &type = result->types[col];
                        if (type.IsNumeric() && type.id() != LogicalTypeId::VARCHAR) {
                            json += val.ToString();
                        } else {
                            std::string s = val.ToString();
                            json += "\"";
                            for (char c : s) {
                                if (c == '"') json += "\\\"";
                                else if (c == '\\') json += "\\\\";
                                else if (c == '\n') json += "\\n";
                                else if (c == '\r') json += "\\r";
                                else if (c == '\t') json += "\\t";
                                else json += c;
                            }
                            json += "\"";
                        }
                    }
                    first_col = false;
                }
                json += "}";
                first_row = false;
            }
        }
        json += "]";
        return json;
    }

    // Generate table profile
    std::string Profile(Connection &conn, const std::string &table) {
        std::stringstream j;
        j << "{";
        
        // Row count
        auto rc = conn.Query("SELECT COUNT(*) FROM \"" + table + "\"");
        int64_t rows = 0;
        if (!rc->HasError()) {
            auto ch = rc->Fetch();
            if (ch && ch->size() > 0) rows = ch->GetValue(0, 0).GetValue<int64_t>();
        }
        j << "\"row_count\":" << rows << ",";
        
        // Columns
        auto cols = conn.Query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = '" + table + "' ORDER BY ordinal_position"
        );
        
        std::vector<std::pair<std::string, std::string>> columns;
        if (!cols->HasError()) {
            while (true) {
                auto ch = cols->Fetch();
                if (!ch || ch->size() == 0) break;
                for (idx_t i = 0; i < ch->size(); i++) {
                    columns.push_back({ch->GetValue(0, i).ToString(), ch->GetValue(1, i).ToString()});
                }
            }
        }
        
        j << "\"column_count\":" << columns.size() << ",";
        j << "\"size_estimate\":\"" << (rows * columns.size() * 8 / 1024 / 1024) << " MB\",";
        j << "\"columns\":[";
        
        bool first = true;
        for (size_t i = 0; i < columns.size(); i++) {
            const std::string &name = columns[i].first;
            const std::string &type = columns[i].second;
            if (!first) j << ",";
            first = false;
            
            j << "{\"name\":\"" << name << "\",\"type\":\"" << type << "\"";
            
            auto stats = conn.Query(
                "SELECT COUNT(*), COUNT(\"" + name + "\"), COUNT(DISTINCT \"" + name + "\"), "
                "MIN(\"" + name + "\"), MAX(\"" + name + "\") FROM \"" + table + "\""
            );
            
            if (!stats->HasError()) {
                auto ch = stats->Fetch();
                if (ch && ch->size() > 0) {
                    int64_t total = ch->GetValue(0, 0).GetValue<int64_t>();
                    int64_t notnull = ch->GetValue(1, 0).GetValue<int64_t>();
                    int64_t uniq = ch->GetValue(2, 0).GetValue<int64_t>();
                    
                    double null_pct = total > 0 ? ((total - notnull) * 100.0 / total) : 0;
                    j << ",\"null_percent\":" << null_pct;
                    j << ",\"unique_count\":" << uniq;
                    
                    auto minv = ch->GetValue(3, 0);
                    auto maxv = ch->GetValue(4, 0);
                    
                    auto escapeStr = [](const std::string &s) {
                        std::string r;
                        for (char c : s) {
                            if (c == '"') r += "\\\"";
                            else if (c == '\\') r += "\\\\";
                            else if (c == '\n') r += "\\n";
                            else r += c;
                        }
                        return r;
                    };
                    
                    if (!minv.IsNull()) j << ",\"min\":\"" << escapeStr(minv.ToString()) << "\"";
                    else j << ",\"min\":null";
                    
                    if (!maxv.IsNull()) j << ",\"max\":\"" << escapeStr(maxv.ToString()) << "\"";
                    else j << ",\"max\":null";
                }
            }
            j << "}";
        }
        
        j << "]}";
        return j.str();
    }

    // Generate column details
    std::string ColumnDetail(Connection &conn, const std::string &table, const std::string &col) {
        std::stringstream j;
        j << "{\"name\":\"" << col << "\"";
        
        auto typeq = conn.Query(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = '" + table + "' AND column_name = '" + col + "'"
        );
        
        std::string colType = "VARCHAR";
        if (!typeq->HasError()) {
            auto ch = typeq->Fetch();
            if (ch && ch->size() > 0) colType = ch->GetValue(0, 0).ToString();
        }
        j << ",\"type\":\"" << colType << "\"";
        
        bool isNum = (colType.find("INT") != std::string::npos || 
                      colType.find("FLOAT") != std::string::npos ||
                      colType.find("DOUBLE") != std::string::npos ||
                      colType.find("DECIMAL") != std::string::npos ||
                      colType.find("NUMERIC") != std::string::npos ||
                      colType.find("REAL") != std::string::npos);
        
        j << ",\"is_numeric\":" << (isNum ? "true" : "false");
        
        auto stats = conn.Query(
            "SELECT COUNT(*), COUNT(\"" + col + "\"), COUNT(DISTINCT \"" + col + "\") FROM \"" + table + "\""
        );
        
        if (!stats->HasError()) {
            auto ch = stats->Fetch();
            if (ch && ch->size() > 0) {
                int64_t total = ch->GetValue(0, 0).GetValue<int64_t>();
                int64_t notnull = ch->GetValue(1, 0).GetValue<int64_t>();
                int64_t uniq = ch->GetValue(2, 0).GetValue<int64_t>();
                
                double null_pct = total > 0 ? ((total - notnull) * 100.0 / total) : 0;
                j << ",\"null_percent\":" << null_pct;
                j << ",\"unique_count\":" << uniq;
            }
        }
        
        if (isNum) {
            auto numStats = conn.Query(
                "SELECT MIN(\"" + col + "\"), MAX(\"" + col + "\"), "
                "AVG(\"" + col + "\"), STDDEV(\"" + col + "\"), MEDIAN(\"" + col + "\") "
                "FROM \"" + table + "\""
            );
            
            if (!numStats->HasError()) {
                auto ch = numStats->Fetch();
                if (ch && ch->size() > 0) {
                    j << ",\"min\":" << ch->GetValue(0, 0).ToString();
                    j << ",\"max\":" << ch->GetValue(1, 0).ToString();
                    j << ",\"avg\":" << ch->GetValue(2, 0).ToString();
                    j << ",\"std\":" << (ch->GetValue(3, 0).IsNull() ? "0" : ch->GetValue(3, 0).ToString());
                    j << ",\"median\":" << ch->GetValue(4, 0).ToString();
                }
            }
            
            // Histogram
            auto hist = conn.Query(
                "WITH b AS (SELECT MIN(\"" + col + "\") as mn, MAX(\"" + col + "\") as mx FROM \"" + table + "\" WHERE \"" + col + "\" IS NOT NULL) "
                "SELECT width_bucket(\"" + col + "\"::DOUBLE, (SELECT mn FROM b), (SELECT mx FROM b)+0.0001, 20) as bkt, COUNT(*) as cnt "
                "FROM \"" + table + "\" WHERE \"" + col + "\" IS NOT NULL GROUP BY bkt ORDER BY bkt"
            );
            
            if (!hist->HasError()) {
                j << ",\"histogram\":{\"bins\":[";
                std::vector<std::pair<int, int64_t>> data;
                while (true) {
                    auto ch = hist->Fetch();
                    if (!ch || ch->size() == 0) break;
                    for (idx_t i = 0; i < ch->size(); i++) {
                        data.push_back({ch->GetValue(0, i).GetValue<int32_t>(), ch->GetValue(1, i).GetValue<int64_t>()});
                    }
                }
                for (size_t i = 0; i < data.size(); i++) {
                    if (i > 0) j << ",";
                    j << data[i].first;
                }
                j << "],\"counts\":[";
                for (size_t i = 0; i < data.size(); i++) {
                    if (i > 0) j << ",";
                    j << data[i].second;
                }
                j << "]}";
            }
        } else {
            // String stats
            auto lenStats = conn.Query(
                "SELECT MIN(LENGTH(\"" + col + "\")), MAX(LENGTH(\"" + col + "\")), AVG(LENGTH(\"" + col + "\")) "
                "FROM \"" + table + "\" WHERE \"" + col + "\" IS NOT NULL"
            );
            
            if (!lenStats->HasError()) {
                auto ch = lenStats->Fetch();
                if (ch && ch->size() > 0) {
                    j << ",\"min_length\":" << ch->GetValue(0, 0).ToString();
                    j << ",\"max_length\":" << ch->GetValue(1, 0).ToString();
                    j << ",\"avg_length\":" << ch->GetValue(2, 0).ToString();
                }
            }
            
            // Top values
            auto top = conn.Query(
                "SELECT \"" + col + "\" as v, COUNT(*) as c FROM \"" + table + "\" "
                "WHERE \"" + col + "\" IS NOT NULL GROUP BY \"" + col + "\" ORDER BY c DESC LIMIT 10"
            );
            
            if (!top->HasError()) {
                j << ",\"top_values\":[";
                bool first = true;
                while (true) {
                    auto ch = top->Fetch();
                    if (!ch || ch->size() == 0) break;
                    for (idx_t i = 0; i < ch->size(); i++) {
                        if (!first) j << ",";
                        first = false;
                        std::string v = ch->GetValue(0, i).ToString();
                        std::string esc;
                        for (char c : v) {
                            if (c == '"') esc += "\\\"";
                            else if (c == '\\') esc += "\\\\";
                            else if (c == '\n') esc += "\\n";
                            else esc += c;
                        }
                        j << "{\"value\":\"" << esc << "\",\"count\":" << ch->GetValue(1, i).GetValue<int64_t>() << "}";
                    }
                }
                j << "]";
            }
        }
        
        j << "}";
        return j.str();
    }

public:
    DuckDBIServer(DatabaseInstance* database, int p) : db(database), port(p) {}
    
    ~DuckDBIServer() { Stop(); }
    
    void Start(const std::string &host) {
        server = make_uniq<httplib::Server>();
        
        // UI
        server->Get("/", [](const httplib::Request&, httplib::Response& res) {
            res.set_content(DUCKDBI_HTML, "text/html; charset=utf-8");
        });
        
        // Execute SQL
        server->Post("/api/query", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mtx);
            try {
                Connection conn(*db);
                res.set_content(ToJSON(conn.Query(req.body)), "application/json");
            } catch (std::exception& e) {
                res.status = 500;
                res.set_content("{\"error\":\"" + std::string(e.what()) + "\"}", "application/json");
            }
        });
        
        // Tables list
        server->Get("/api/tables", [this](const httplib::Request&, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mtx);
            try {
                Connection conn(*db);
                auto result = conn.Query(
                    "SELECT table_name, table_schema FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY table_name"
                );
                res.set_content(ToJSON(std::move(result)), "application/json");
            } catch (std::exception& e) {
                res.status = 500;
                res.set_content("{\"error\":\"" + std::string(e.what()) + "\"}", "application/json");
            }
        });
        
        // Table profile
        server->Get(R"(/api/explore/profile/(.+))", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mtx);
            try {
                Connection conn(*db);
                res.set_content(Profile(conn, std::string(req.matches[1])), "application/json");
            } catch (std::exception& e) {
                res.status = 500;
                res.set_content("{\"error\":\"" + std::string(e.what()) + "\"}", "application/json");
            }
        });
        
        // Column details
        server->Get(R"(/api/explore/column/([^/]+)/(.+))", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lock(mtx);
            try {
                Connection conn(*db);
                res.set_content(ColumnDetail(conn, std::string(req.matches[1]), std::string(req.matches[2])), "application/json");
            } catch (std::exception& e) {
                res.status = 500;
                res.set_content("{\"error\":\"" + std::string(e.what()) + "\"}", "application/json");
            }
        });
        
        running = true;
        server_thread = std::thread([this, host]() { server->listen(host.c_str(), port); });
        
        // Open browser
#ifdef __APPLE__
        system(("open http://localhost:" + std::to_string(port)).c_str());
#elif __linux__
        system(("xdg-open http://localhost:" + std::to_string(port) + " 2>/dev/null &").c_str());
#elif _WIN32
        system(("start http://localhost:" + std::to_string(port)).c_str());
#endif
    }
    
    void Stop() {
        if (running && server) {
            server->stop();
            if (server_thread.joinable()) server_thread.join();
            running = false;
        }
    }
    
    bool IsRunning() const { return running; }
};

// Global server
static duckdb::unique_ptr<DuckDBIServer> g_server;

// ============================================================================
// DuckDB Functions
// ============================================================================
static void StartFunc(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &ctx = state.GetContext();
    auto host = args.data[0].GetValue(0).ToString();
    auto port = args.data[1].GetValue(0).GetValue<int32_t>();
    
    if (g_server && g_server->IsRunning()) g_server->Stop();
    
    auto &database = DatabaseInstance::GetDatabase(ctx);
    g_server = make_uniq<DuckDBIServer>(&database, port);
    g_server->Start(host);
    
    result.SetValue(0, Value("DuckDBI: http://" + host + ":" + std::to_string(port)));
}

static void StopFunc(DataChunk &args, ExpressionState &state, Vector &result) {
    if (g_server && g_server->IsRunning()) {
        g_server->Stop();
        result.SetValue(0, Value("DuckDBI stopped"));
    } else {
        result.SetValue(0, Value("No server running"));
    }
}

// ============================================================================
// Extension
// ============================================================================
void DuckdbiExtension::Load(ExtensionLoader &loader) {
    auto start = ScalarFunction("duckdbi_start", {LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR, StartFunc);
    loader.RegisterFunction(start);
    
    auto stop = ScalarFunction("duckdbi_stop", {}, LogicalType::VARCHAR, StopFunc);
    loader.RegisterFunction(stop);
}

std::string DuckdbiExtension::Name() { return "duckdbi"; }
std::string DuckdbiExtension::Version() const { return "0.1.0"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckdbi_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    duckdb::DuckdbiExtension extension;
    extension.Load(loader);
}

DUCKDB_EXTENSION_API const char *duckdbi_version() {
    return "0.1.0";
}

}