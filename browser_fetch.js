#!/usr/bin/env node
/**
 * Headless browser fetcher via Playwright.
 * Usage: node browser_fetch.js <url> [proxy]
 *
 * Outputs JSON to stdout: { "httpCode": 200, "body": "...", "finalUrl": "...", "error": null }
 * On failure:             { "httpCode": 0,   "body": "",   "finalUrl": "",    "error": "message" }
 *
 * proxy format: http://login:pass@host:port
 */

const { chromium } = require('playwright');

const url   = process.argv[2];
const proxy = process.argv[3] || null;

if (!url) {
    process.stdout.write(JSON.stringify({ httpCode: 0, body: '', finalUrl: '', error: 'No URL provided' }));
    process.exit(1);
}

(async () => {
    let browser;
    try {
        const launchOpts = {
            headless: true,
            args: ['--disable-blink-features=AutomationControlled'],
        };
        if (proxy) {
            const parsed = new URL(proxy);
            launchOpts.proxy = {
                server: `${parsed.protocol}//${parsed.hostname}:${parsed.port}`,
                username: parsed.username || undefined,
                password: parsed.password || undefined,
            };
        }

        browser = await chromium.launch(launchOpts);
        const context = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            locale: 'en-US',
            timezoneId: 'America/New_York',
            viewport: { width: 1440, height: 900 },
        });

        const page = await context.newPage();
        const response = await page.goto(url, {
            waitUntil: 'domcontentloaded',
            timeout: 30000,
        });

        // Wait briefly for any deferred inline scripts
        await page.waitForTimeout(2000);

        const httpCode = response ? response.status() : 0;
        const body = await page.content();
        const finalUrl = page.url();

        process.stdout.write(JSON.stringify({ httpCode, body, finalUrl, error: null }));
    } catch (err) {
        process.stdout.write(JSON.stringify({ httpCode: 0, body: '', finalUrl: '', error: err.message }));
    } finally {
        if (browser) await browser.close();
    }
})();
