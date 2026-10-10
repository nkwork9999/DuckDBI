const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const { chromium } = require('playwright');
const source = fs.readFileSync(path.join(__dirname, '../src/include/simple_ui.hpp'), 'utf8');
const html = [...source.matchAll(/R"(DUCKUI\d*)\(([\s\S]*?)\)\1"/g)].map(m=>m[2]).join('');
const helpers = fs.readFileSync(path.join(__dirname,'../src/include/ui_helpers.hpp'),'utf8');
const uiJS = [...helpers.matchAll(/R"DUCKJS\(([\s\S]*?)\)DUCKJS"/g)].map(m=>m[1]).join('');
const advancedSource = fs.readFileSync(path.join(__dirname,'../src/duckdbi_extension.cpp'),'utf8');
const advancedHTML = [...advancedSource.matchAll(/R"(HTML_PART\d)\(([\s\S]*?)\)\1"/g)].map(m=>m[2]).join('');
const isMap = html.includes('<title>DuckGL</title>');
let queryCount = 0, tablesFail = false, geoURL = '';
const markdownRows = [{title:'Tables', body:'# Blog title\n\n**Bold** and *italic*.\n\n- First\n- Second\n\n```sql\nSELECT 99;\n```\n\n[safe](https://example.com) [bad](javascript:alert(1))\n\n<img src=x onerror="window.injected=true"><script>window.injected=true</script>', nullable:null}, {title:'Second',body:'## Next post',nullable:null}];
const rows = [{name:'<img src=x onerror="window.injected=true">', amount:0}, {name:'quoted,"value"',amount:null}, {name:'Tokyo',amount:12.5}];
const server = http.createServer(async (request, response) => {
    const url = new URL(request.url, 'http://localhost');
    if (url.pathname === '/') { response.setHeader('Content-Type','text/html'); response.end(html); return; }
    if (url.pathname === '/ui.js') { response.setHeader('Content-Type','application/javascript'); response.end(uiJS); return; }
    if (url.pathname === '/advanced') { response.setHeader('Content-Type','text/html'); response.end(advancedHTML); return; }
    response.setHeader('Content-Type','application/json');
    if (url.pathname === '/api/tables') {
        response.statusCode = tablesFail ? 500 : 200;
        response.end(JSON.stringify(tablesFail ? {error:'Table connection failed'} : [{table_name:'orders',table_schema:'analytics'},{table_name:'odd"name<img>',table_schema:'main'}])); return;
    }
    if (url.pathname.startsWith('/api/geojson/')) {
        geoURL = request.url;
        response.end(JSON.stringify({type:'FeatureCollection',features:[{type:'Feature',geometry:{type:'Point',coordinates:[139.7,35.7]},properties:{}}]})); return;
    }
    if (url.pathname === '/api/query') {
        queryCount++; let body = ''; for await (const part of request) body += part;
        if (body === 'MARKDOWN') { response.end(JSON.stringify(markdownRows)); return; }
        if (body === 'BAD_RESPONSE') { response.end('not JSON'); return; }
        if (body === 'FAIL') { response.statusCode=400; response.end(JSON.stringify({error:'Syntax error: <script>bad</script>'})); return; }
        if (body === 'EMPTY') { response.end('[]'); return; }
        if (body === 'SLOW') await new Promise(resolve => setTimeout(resolve, 200));
        response.end(JSON.stringify(rows)); return;
    }
    response.statusCode=404; response.end('{}');
});
const markedJS = fs.readFileSync(require.resolve('marked/marked.min.js'),'utf8');
const libraries = `window.GridStack={init:()=>({on(){},addWidget(){}})}; window.Plotly={newPlot:async(id,traces)=>{window.chartTraces=traces;document.getElementById(id).textContent='Chart preview';}};
window.maplibregl={Map:class{addControl(){} on(event,callback){if(event==='load')setTimeout(callback,0)} fitBounds(bounds){window.mapBounds=bounds}},NavigationControl:class{}};
window.deck={MapboxOverlay:class{setProps(props){window.mapLayers=props.layers}},GeoJsonLayer:class{constructor(options){Object.assign(this,options)}}};`;
(async () => {
    await new Promise(resolve => server.listen(0,'127.0.0.1',resolve));
    let browser;
    try {
        browser = await chromium.launch({headless:true,channel:process.env.UI_BROWSER_CHANNEL || undefined});
        const page = await browser.newPage({viewport:{width:1280,height:900},acceptDownloads:true,locale:'en-US'});
        const errors=[]; page.on('pageerror', error => errors.push(error.message));
        await page.route('https://**/*', route => route.fulfill({contentType:route.request().url().endsWith('.css')?'text/css':'application/javascript',body:route.request().url().endsWith('.css')?'':route.request().url().includes('/marked@')?markedJS:libraries}));
        await page.goto('http://127.0.0.1:' + server.address().port);
        await page.getByRole('button',{name:'orders analytics'}).waitFor();
        await page.getByRole('button',{name:'orders analytics'}).click();
        assert.equal(await page.locator('#sql-editor').inputValue(), 'SELECT * FROM "analytics"."orders" LIMIT 200');
        if (isMap) {
            await page.waitForFunction(() => window.mapBounds);
            assert(geoURL.includes('schema=analytics'));
            assert.deepEqual(await page.evaluate(() => window.mapBounds),[[139.7,35.7],[139.7,35.7]]);
            await page.getByRole('button',{name:'Preview rows'}).click();
        }
        await page.locator('#result-content td').first().waitFor();
        assert.equal(await page.locator('#result-content img').count(),0);
        assert.equal(await page.locator('#result-content td').nth(1).textContent(),'0');
        assert.equal(await page.locator('#result-content td').nth(3).textContent(),'NULL');
        assert.equal(await page.evaluate(() => window.injected),undefined);
        await page.locator('#table-search').fill('odd');
        assert.equal(await page.locator('.table-button').count(),1);
        assert.equal(await page.locator('#tables img').count(),0);
        await page.locator('#table-search').fill('');
        if (!isMap) {
            await page.locator('summary[data-i18n="Chart results"]').click();
            await page.getByRole('button',{name:'Draw chart'}).click();
            await page.waitForFunction(() => window.chartTraces);
            assert.deepEqual(await page.evaluate(() => window.chartTraces[0].y),[0,null,12.5]);
            const downloadPromise=page.waitForEvent('download'); await page.getByRole('button',{name:'Download results as CSV'}).click();
            const download=await downloadPromise; const csv=fs.readFileSync(await download.path(),'utf8');
            assert(csv.includes('"quoted,""value"""')); assert(csv.includes(',"0"'));
            assert.equal(await page.getByRole('link',{name:'Dashboards & reports'}).getAttribute('href'),'/advanced');
        }
        const outputDir = process.env.UI_SCREENSHOT_DIR || os.tmpdir(); fs.mkdirSync(outputDir,{recursive:true});
        await page.screenshot({path:path.join(outputDir,(isMap?'duckgl':'duckdbi')+'-desktop.png'),fullPage:true});
        const beforeEmpty = queryCount;
        await page.locator('#sql-editor').fill(' '); await page.getByRole('button',{name:'Run query',exact:true}).click();
        assert.equal(queryCount,beforeEmpty); assert((await page.locator('#status').textContent()).includes('Enter a SQL query'));
        await page.locator('#sql-editor').fill('FAIL'); await page.locator('#sql-editor').press('Control+Enter');
        await page.waitForFunction(() => document.getElementById('status').textContent.includes('Syntax error'));
        assert.equal(await page.locator('#status script').count(),0);
        await page.locator('#sql-editor').fill('BAD_RESPONSE'); await page.getByRole('button',{name:'Run query',exact:true}).click();
        await page.waitForFunction(() => document.getElementById('status').textContent.includes('invalid response'));
        await page.locator('#sql-editor').fill('SLOW'); const beforeSlow=queryCount;
        await page.getByRole('button',{name:'Run query',exact:true}).click(); await page.locator('#sql-editor').press('Control+Enter');
        await page.waitForFunction(() => document.getElementById('run').disabled === false);
        assert.equal(queryCount,beforeSlow+1);
        await page.locator('#sql-editor').fill('EMPTY'); await page.getByRole('button',{name:'Run query',exact:true}).click();
        await page.getByText('No rows returned.',{exact:true}).waitFor();
        tablesFail=true; await page.getByRole('button',{name:'Refresh tables'}).click();
        await page.getByText('Could not load tables. Use Refresh to retry.',{exact:true}).waitFor();
        tablesFail=false; await page.getByRole('button',{name:'Refresh tables'}).click(); await page.getByRole('button',{name:'orders analytics'}).waitFor();
        assert.equal(await page.locator('#status').textContent(),'Tables updated.');
        await page.setViewportSize({width:375,height:812});
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        await page.screenshot({path:path.join(outputDir,(isMap?'duckgl':'duckdbi')+'-mobile.png'),fullPage:true});
        if (!isMap) {
            // Locale persists across reloads/routes, and never translates database content.
            await page.locator('#ui-language').selectOption('zh-CN');
            assert.equal(await page.locator('#run').textContent(),'运行查询');
            assert.equal(await page.locator('#refresh').getAttribute('aria-label'),'刷新数据表');
            assert.equal(await page.locator('html').getAttribute('lang'),'zh-CN');
            assert.equal(await page.locator('.table-button').count(),2);
            await page.reload();
            assert.equal(await page.locator('#ui-language').inputValue(),'zh-CN');
            await page.locator('#ui-language').selectOption('ja');
            assert.equal(await page.locator('#run').textContent(),'クエリを実行');
            await page.locator('#ui-language').selectOption('en');
            async function checkMarkdown(run) {
                await page.locator('#sql-editor').fill('MARKDOWN'); await run.click();
                await page.waitForFunction(() => document.getElementById('markdown-column').options.length === 2);
                if (!await page.locator('#markdown-column').isVisible()) await page.locator('summary[data-i18n="Markdown preview"]').click();
                await page.locator('#markdown-column').selectOption('body');
                const before = queryCount;
                await page.locator('#preview-markdown').click();
                assert.equal(await page.locator('#markdown-preview h1').textContent(),'Blog title');
                assert.equal(await page.locator('#markdown-preview strong').textContent(),'Bold');
                assert.equal(await page.locator('#markdown-preview li').count(),2);
                assert.equal(await page.locator('#markdown-preview pre code').textContent(),'SELECT 99;\n');
                assert.equal(await page.locator('#markdown-preview article').count(),2);
                assert.equal(await page.locator('#markdown-preview img, #markdown-preview script, #markdown-preview [onerror]').count(),0);
                assert.equal(await page.locator('#markdown-preview a').nth(1).getAttribute('href'),null);
                assert.equal(await page.evaluate(() => window.injected),undefined);
                await page.screenshot({path:path.join(outputDir,'duckdbi-markdown-'+(page.url().endsWith('/advanced')?'advanced':'simple')+'.png'),fullPage:true});
                assert.equal(queryCount,before); // SQL code in stored Markdown is never executed.
                await page.locator('#ui-language').selectOption('zh-CN');
                assert.equal(await page.locator('#markdown-preview h1').textContent(),'Blog title');
                await page.locator('#ui-language').selectOption('en');
                await page.locator('#sql-editor').fill('EMPTY'); await run.click();
                await page.waitForFunction(() => document.getElementById('preview-markdown').disabled);
                assert.equal(await page.locator('#markdown-preview article').count(),0);
            }
            await checkMarkdown(page.locator('#run'));
            // Missing parser reports an actionable error without inserting content.
            await page.locator('#sql-editor').fill('MARKDOWN'); await page.locator('#run').click();
            await page.waitForFunction(() => document.getElementById('markdown-column').options.length === 2);
            await page.evaluate(() => {window.savedMarked=window.marked;window.marked=undefined;});
            await page.locator('#preview-markdown').click();
            assert((await page.locator('#status').textContent()).includes('Markdown library could not load'));
            await page.evaluate(() => {window.marked=window.savedMarked;});
            await page.locator('#ui-language').selectOption('zh-CN');
            await page.goto('http://127.0.0.1:' + server.address().port + '/advanced');
            assert.equal(await page.locator('#ui-language').inputValue(),'zh-CN');
            assert.equal(await page.locator('.nav-tab').first().textContent(),'📊 探索');
            await page.locator('.nav-tab').nth(3).click();
            assert.equal(await page.locator('#sql-editor').inputValue(),"SELECT 1 as id, 'Hello DuckDBI!' as message;");
            await page.locator('#ui-language').selectOption('en');
            await checkMarkdown(page.getByRole('button',{name:'▶ Execute'}));
            // Automatic language detection and unavailable storage are supported.
            const chinese = await browser.newContext({locale:'zh-CN'});
            const zhPage = await chinese.newPage();
            await zhPage.route('https://**/*', route => route.fulfill({contentType:'application/javascript',body:route.request().url().includes('/marked@')?markedJS:libraries}));
            await zhPage.addInitScript(() => {Object.defineProperty(window,'localStorage',{get(){throw new Error('blocked');}});});
            await zhPage.goto('http://127.0.0.1:' + server.address().port);
            assert.equal(await zhPage.locator('#run').textContent(),'运行查询');
            await zhPage.locator('#ui-language').selectOption('en');
            assert.equal(await zhPage.locator('#run').textContent(),'Run query');
            await chinese.close();
        }
        assert.deepEqual(errors,[]);
        console.log('Browser regressions passed: rendering, identifiers, filtering, shortcuts, errors, retry, duplicate requests, charts/CSV, mobile layout, English/Chinese/Japanese, locale persistence/fallback, and safe Markdown on both routes.');
    } finally { if(browser) await browser.close(); await new Promise(resolve=>server.close(resolve)); }
})().catch(error=>{console.error(error);process.exitCode=1});
