#pragma once
#include <string>
static const std::string DUCKDBI_UI_JS =
    std::string(R"DUCKJS(/* Shared UI helpers. Only annotated interface text is translated. */
window.DuckDBIUI = (() => {
'use strict';
const translations = {"Language": ["语言", "言語"], "Your data, one query at a time": ["每次查询，探索数据", "クエリでデータを探る"], "Dashboards & reports": ["仪表板与报告", "ダッシュボードとレポート"], "Database tables": ["数据库表", "データベースのテーブル"], "Tables": ["数据表", "テーブル"], "Refresh": ["刷新", "更新"], "Refresh tables": ["刷新数据表", "テーブルを更新"], "Find a table": ["查找数据表", "テーブルを検索"], "Filter tables": ["筛选数据表", "テーブルを絞り込む"], "Loading tables…": ["正在加载数据表…", "テーブルを読込中…"], "Ready": ["就绪", "準備完了"], "SQL query": ["SQL 查询", "SQL クエリ"], "SQL Query": ["SQL 查询", "SQL クエリ"], "Run query": ["运行查询", "クエリを実行"], "Select a table or write a query": ["选择数据表或编写查询", "テーブルを選択するかクエリを入力"], "Ctrl / ⌘ + Enter to run": ["Ctrl / ⌘ + Enter 运行", "Ctrl / ⌘ + Enter で実行"], "Results": ["结果", "結果"], "Run a query to see your data.": ["运行查询以查看数据。", "クエリを実行するとデータが表示されます。"], "Chart results": ["结果图表", "結果をグラフにする"], "Type": ["类型", "種類"], "Bar": ["柱状图", "棒グラフ"], "Line": ["折线图", "折れ線グラフ"], "Scatter": ["散点图", "散布図"], "Area": ["面积图", "面グラフ"], "3D scatter": ["三维散点图", "3D 散布図"], "X column": ["X 列", "X 列"], "Y column": ["Y 列", "Y 列"], "Z column": ["Z 列", "Z 列"], "Draw chart": ["绘制图表", "グラフを描画"], "Run a query to choose chart columns.": ["运行查询以选择图表列。", "クエリを実行してグラフの列を選択します。"], "Download results as CSV": ["下载 CSV 结果", "結果を CSV でダウンロード"], "No rows returned.": ["未返回任何行。", "結果は0行です。"], "No matching tables.": ["没有匹配的数据表。", "一致するテーブルがありません。"], "No tables found.": ["未找到数据表。", "テーブルがありません。"], "Tables updated.": ["数据表已更新。", "テーブルを更新しました。"], "Could not load tables. Use Refresh to retry.": ["无法加载数据表，请点击刷新重试。", "テーブルを読み込めません。更新して再試行してください。"], "Enter a SQL query first.": ["请先输入 SQL 查询。", "SQL クエリを入力してください。"], "Running…": ["正在运行…", "実行中…"], "Running query…": ["正在运行查询…", "クエリを実行中…"], "Query complete.": ["查询完成。", "クエリが完了しました。"], "Choose columns from the latest query results.": ["从最新查询结果中选择列。", "最新のクエリ結果から列を選択してください。"], "Chart uses the latest query results.": ["图表使用最新查询结果。", "最新のクエリ結果をグラフに表示しています。"], "Run a query before drawing a chart.": ["请先运行查询再绘制图表。", "グラフを描画する前にクエリを実行してください。"], "Chart library could not load. Check your connection and reload.": ["无法加载图表库，请检查连接并重新加载。", "グラフライブラリを読み込めません。接続を確認し再読込してください。"], "The server returned an invalid response.": ["服务器返回了无效响应。", "サーバーの応答が不正です。"], "Invalid table list.": ["无效的数据表列表。", "テーブル一覧が不正です。"], "Invalid query results.": ["无效的查询结果。", "クエリ結果が不正です。"], "Markdown preview": ["Markdown 预览", "Markdown プレビュー"], "Markdown column": ["Markdown 列", "Markdown の列"], "Preview Markdown": ["预览 Markdown", "Markdown を表示"], "Select a result column containing Markdown text.": ["选择包含 Markdown 文本的结果列。", "Markdown 本文を含む結果の列を選択してください。"], "No Markdown text in this column.": ["此列没有 Markdown 文本。", "この列に Markdown 本文はありません。"], "Markdown library could not load. Check your connection and reload.": ["无法加载 Markdown 库，请检查连接并重新加载。", "Markdown ライブラリを読み込めません。接続を確認し再読込してください。"], "Preview is limited to 200 rows.": ["预览最多显示 200 行。", "プレビューは200行まで表示します。"], "📊 Explore": ["📊 探索", "📊 データ探索"], "📈 Dashboard": ["📈 仪表板", "📈 ダッシュボード"], "📝 Report": ["📝 报告", "📝 レポート"], "💻 Query": ["💻 查询", "💻 クエリ"], "🔄 Refresh": ["🔄 刷新", "🔄 更新"], "📁 Tables": ["📁 数据表", "📁 テーブル"], "📊 Charts": ["📊 图表", "📊 グラフ"], "📄 Reports": ["📄 报告", "📄 レポート"], "+ Add": ["+ 添加", "+ 追加"], "+ New": ["+ 新建", "+ 新規"], "📥 Export PDF": ["📥 导出 PDF", "📥 PDF を出力"], "▶ Run": ["▶ 运行", "▶ 実行"], "💾 Save": ["💾 保存", "💾 保存"], "▶ Execute": ["▶ 执行", "▶ 実行"], "Format": ["格式化", "整形"], "Preview": ["预览", "プレビュー"], "Add Chart": ["添加图表", "グラフを追加"], "Chart Type": ["图表类型", "グラフの種類"], "Title": ["标题", "タイトル"], "My Chart": ["我的图表", "グラフのタイトル"], "X Axis Column": ["X 轴列", "X 軸の列"], "Y Axis Column": ["Y 轴列", "Y 軸の列"], "Cancel": ["取消", "キャンセル"], "Add": ["添加", "追加"], "Column Details": ["列详情", "列の詳細"], "Connected to DuckDB": ["已连接 DuckDB", "DuckDB に接続中"], "Select a table from the sidebar to explore": ["从侧栏选择数据表以探索", "サイドバーからテーブルを選択してください"], "Click \"Run\" to preview your report": ["点击“运行”预览报告", "「実行」でレポートを表示します"], "Execute a query to see results": ["执行查询以查看结果", "クエリを実行すると結果が表示されます"], "Pie": ["饼图", "円グラフ"], "Histogram": ["直方图", "ヒストグラム"], "Box": ["箱线图", "箱ひげ図"], "Heatmap": ["热力图", "ヒートマップ"], "No charts yet": ["尚无图表", "グラフはまだありません"], "No results": ["没有结果", "結果がありません"], "Executing...": ["正在执行…", "実行中…"], "Query failed": ["查询失败", "クエリに失敗しました"], "Generating PDF...": ["正在生成 PDF…", "PDF を生成中…"], "PDF exported": ["PDF 已导出", "PDF を出力しました"], "Report saved": ["报告已保存", "レポートを保存しました"]};
)DUCKJS") +
    std::string(R"DUCKJS(let language = 'en';
try { language = localStorage.getItem('duckdbi-language') || navigator.language || 'en'; } catch (_) { language = navigator.language || 'en'; }
language = language.toLowerCase().startsWith('zh') ? 'zh-CN' : language.toLowerCase().startsWith('ja') ? 'ja' : 'en';
function t(key) { return translations[key]?.[language === 'zh-CN' ? 0 : 1] && language !== 'en' ? translations[key][language === 'zh-CN' ? 0 : 1] : key; }
function translate(root = document) {
    for (const el of root.querySelectorAll('[data-i18n]')) el.textContent = t(el.dataset.i18n);
    for (const attr of ['placeholder','aria-label']) for (const el of root.querySelectorAll('[data-i18n-' + attr + ']')) el.setAttribute(attr, t(el.getAttribute('data-i18n-' + attr)));
}
function init() {
    const select = document.getElementById('ui-language');
    function apply() { document.documentElement.lang = language; select.value = language; translate(); }
    select.addEventListener('change', () => {
        language = select.value;
        try { localStorage.setItem('duckdbi-language', language); } catch (_) {}
        apply(); document.dispatchEvent(new Event('duckdbi-language-change'));
    });
    apply();
}
// Marked is also used by the existing report editor. Sanitize its output before
// insertion: database content must not execute HTML or javascript: links.
function markdown(value) {
    if (!window.marked) throw new Error(t('Markdown library could not load. Check your connection and reload.'));
    const template = document.createElement('template');
    template.innerHTML = marked.parse(String(value), {async:false});
    const allowed = new Set(['P','BR','HR','H1','H2','H3','H4','H5','H6','UL','OL','LI','BLOCKQUOTE','PRE','CODE','EM','STRONG','DEL','TABLE','THEAD','TBODY','TR','TH','TD','A']);
    const dangerous = new Set(['SCRIPT','STYLE','IFRAME','OBJECT','EMBED','SVG','MATH','FORM','INPUT','BUTTON','TEXTAREA','SELECT','TEMPLATE','LINK','META','BASE','IMG','VIDEO','AUDIO']);
    function clean(parent) {
        for (const node of [...parent.childNodes]) {
            if (node.nodeType === 8) { node.remove(); continue; }
            if (node.nodeType !== 1) continue;
            if (dangerous.has(node.tagName)) { node.remove(); continue; }
            const href = node.tagName === 'A' ? node.getAttribute('href') : null;
            for (const attr of [...node.attributes]) node.removeAttribute(attr.name);
            if (href) {
                try { const url = new URL(href, location.href); if (['http:','https:','mailto:'].includes(url.protocol)) { node.setAttribute('href',url.href); node.setAttribute('rel','noopener noreferrer'); } } catch (_) {}
            }
            clean(node);
            if (!allowed.has(node.tagName)) node.replaceWith(...node.childNodes);
        }
    }
    clean(template.content);
    return template.content;
}
function preview(target, rows, column) {
    target.replaceChildren();
    let count = 0;
    for (const row of rows.slice(0,200)) {
        if (typeof row[column] !== 'string' || !row[column].trim()) continue;
        const article = document.createElement('article'); article.className = 'markdown-document';
        article.appendChild(markdown(row[column])); target.appendChild(article); count++;
    }
    if (!count) target.textContent = t('No Markdown text in this column.');
    if (rows.length > 200) { const note = document.createElement('p'); note.textContent = t('Preview is limited to 200 rows.'); target.appendChild(note); }
}
return {t,translate,init,markdown,preview};
})();
)DUCKJS");
