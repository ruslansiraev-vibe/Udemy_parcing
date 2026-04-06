<?php

/**
 * YouTube Channel Email Scraper
 *
 * Reads youtube_parcing from leads_copy, visits each channel's About page,
 * extracts email addresses from channel description and page content,
 * saves results to youtube_parcing_email column.
 *
 * Usage:
 *   php scrape_youtube_emails.php
 *   php scrape_youtube_emails.php --worker=0 --total-workers=10
 */

define('DB_HOST',  '127.0.0.1');
define('DB_PORT',  3306);
define('DB_NAME',  'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER',  'root');
define('DB_PASS',  '');

define('DELAY_MS',        500);
define('REQUEST_TIMEOUT',  10);
define('CONNECT_TIMEOUT',  5);
define('FETCH_BATCH_SIZE', 100);
define('MAX_RETRIES',      3);
define('MAX_HTML_BYTES',   3145728);

const BROWSER_PROFILES = [
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',        'ch_ua' => '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"macOS"'],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',               'ch_ua' => '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"Windows"'],
    ['ua' => 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',                         'ch_ua' => '"Google Chrome";v="135", "Chromium";v="135", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"Linux"'],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',         'ch_ua' => '"Google Chrome";v="134", "Chromium";v="134", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"macOS"'],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0',                                           'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',                                              'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',         'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
];

const ACCEPT_VARIANTS = [
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
];

const ACCEPT_ENCODING_VARIANTS = ['gzip, deflate, br, zstd', 'gzip, deflate, br', 'gzip, deflate'];
const ACCEPT_LANGUAGE_VARIANTS = ['en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en-US,en;q=0.8', 'en-US,en;q=0.5'];
const CACHE_CONTROL_VARIANTS   = ['max-age=0', 'no-cache'];

const EMAIL_BLACKLIST = [
    'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
    '@example.com', '@test.com', '@sentry.io', '@cloudflare.com',
    '@youtube.com', '@google.com', '@googleapis.com', '@googlevideo.com',
    '@ytimg.com', '@gstatic.com', '@schema.org', '@w3.org',
    '@wixpress.com', '@wordpress.com', '@wordpress.org',
    'yourname@', 'name@', 'email@', 'user@', 'your@email.',
    'example@domain.', 'example@mail.', 'test@mail.',
];

const COMMON_TLDS = [
    'com', 'org', 'net', 'edu', 'gov', 'mil', 'biz', 'info', 'co', 'io', 'ai',
    'app', 'dev', 'me', 'tv', 'cc', 'pro', 'xyz', 'live', 'site', 'online',
    'academy', 'training', 'school', 'coach', 'blog', 'digital', 'design',
    'studio', 'media', 'systems', 'solutions', 'software', 'services', 'group',
    'support', 'center', 'world', 'today', 'care', 'life', 'finance',
    'marketing', 'management', 'consulting', 'technology', 'international',
    'photography', 'events', 'works', 'agency', 'network', 'email', 'store',
    'space', 'cloud', 'trade', 'company',
];

// ── DB ───────────────────────────────────────────────────────────────────────

function getDb(): PDO
{
    static $pdo = null;
    if ($pdo === null) {
        $dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
        $pdo = new PDO($dsn, DB_USER, DB_PASS, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8mb4",
        ]);
    }
    return $pdo;
}

// ── HTTP ─────────────────────────────────────────────────────────────────────

function randomHeaders(): array
{
    $profile      = BROWSER_PROFILES[array_rand(BROWSER_PROFILES)];
    $accept       = ACCEPT_VARIANTS[array_rand(ACCEPT_VARIANTS)];
    $acceptEnc    = ACCEPT_ENCODING_VARIANTS[array_rand(ACCEPT_ENCODING_VARIANTS)];
    $acceptLang   = ACCEPT_LANGUAGE_VARIANTS[array_rand(ACCEPT_LANGUAGE_VARIANTS)];
    $cacheControl = CACHE_CONTROL_VARIANTS[array_rand(CACHE_CONTROL_VARIANTS)];

    $headers = [
        'Accept: '          . $accept,
        'Accept-Language: ' . $acceptLang,
        'Accept-Encoding: ' . $acceptEnc,
        'Cache-Control: '   . $cacheControl,
        'Connection: keep-alive',
        'Upgrade-Insecure-Requests: 1',
        'Sec-Fetch-Dest: document',
        'Sec-Fetch-Mode: navigate',
        'Sec-Fetch-Site: none',
        'Sec-Fetch-User: ?1',
    ];

    if ($profile['ch_ua'] !== null) {
        $headers[] = 'sec-ch-ua: '          . $profile['ch_ua'];
        $headers[] = 'sec-ch-ua-mobile: '   . $profile['ch_mobile'];
        $headers[] = 'sec-ch-ua-platform: ' . $profile['platform'];
    }

    return [$profile['ua'], $headers];
}

function fetchPage(string $url): array
{
    [$userAgent, $headers] = randomHeaders();

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS      => 5,
        CURLOPT_TIMEOUT        => REQUEST_TIMEOUT,
        CURLOPT_CONNECTTIMEOUT => CONNECT_TIMEOUT,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => 0,
        CURLOPT_ENCODING       => '',
        CURLOPT_USERAGENT      => $userAgent,
        CURLOPT_HTTPHEADER     => $headers,
    ]);

    $html     = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error    = curl_error($ch);
    curl_close($ch);

    if ($error !== '') {
        return ['html' => '', 'code' => 0, 'error' => $error];
    }

    $html = (string) $html;
    if (strlen($html) > MAX_HTML_BYTES) {
        $html = substr($html, 0, MAX_HTML_BYTES);
    }

    return ['html' => $html, 'code' => $httpCode, 'error' => ''];
}

// ── YouTube URL normalization ────────────────────────────────────────────────

function normalizeYoutubeUrl(string $raw): string
{
    $raw = trim($raw);

    // Fix double-prefix: https://www.youtube.com/https://www.youtube.com/channel/...
    if (preg_match('#https?://(?:www\.)?youtube\.com/(https?://(?:www\.)?youtube\.com/.+)#i', $raw, $m)) {
        $raw = $m[1];
    }

    // Remove query params like ?sub_confirmation=1
    $raw = preg_replace('/\?.*$/', '', $raw);
    $raw = rtrim($raw, '/');

    return $raw;
}

/**
 * Build the "about" page URL for a YouTube channel.
 * Handles: /@handle, /channel/UCxxx, /c/Name, /user/Name, /Name
 */
function youtubeAboutUrl(string $channelUrl): string
{
    $url = normalizeYoutubeUrl($channelUrl);

    // Already has /about
    if (str_ends_with($url, '/about')) {
        return $url;
    }

    return $url . '/about';
}

// ── Email extraction ─────────────────────────────────────────────────────────

function isValidEmail(string $email): bool
{
    if ($email === '' || strlen($email) > 160) {
        return false;
    }
    if (!filter_var($email, FILTER_VALIDATE_EMAIL)) {
        return false;
    }
    foreach (EMAIL_BLACKLIST as $bad) {
        if (str_contains($email, $bad)) {
            return false;
        }
    }
    if (preg_match('/@(.*\.)?(png|jpe?g|gif|webp|svg|css|js|woff2?|ttf|ico)$/i', $email)) {
        return false;
    }

    [$local, $domain] = array_pad(explode('@', $email, 2), 2, '');
    if ($local === '' || $domain === '') {
        return false;
    }
    if (preg_match('/^(example|test|sample|demo|user|username|yourname|admin)$/i', $local)) {
        return false;
    }

    $labels = explode('.', $domain);
    $tld = strtolower((string) end($labels));
    if ($tld === '') {
        return false;
    }
    if (strlen($tld) !== 2 && !in_array($tld, COMMON_TLDS, true)) {
        return false;
    }

    return true;
}

/**
 * Parse ytInitialData JSON from YouTube page HTML.
 * Returns ['description' => string, 'links' => array, 'has_business_email' => bool]
 */
function parseYoutubeAbout(string $html): array
{
    $result = ['description' => '', 'links' => [], 'has_business_email' => false];

    if (!preg_match('/var\s+ytInitialData\s*=\s*(\{.+?\});\s*<\/script>/s', $html, $m)) {
        return $result;
    }

    $data = @json_decode($m[1], true);
    if (!is_array($data)) {
        return $result;
    }

    $aboutModel = findNestedKey($data, 'aboutChannelViewModel');
    if ($aboutModel === null) {
        return $result;
    }

    $result['description'] = $aboutModel['description'] ?? '';

    if (isset($aboutModel['signInForBusinessEmail'])) {
        $result['has_business_email'] = true;
    }

    foreach ($aboutModel['links'] ?? [] as $link) {
        $vm = $link['channelExternalLinkViewModel'] ?? null;
        if ($vm) {
            $title = $vm['title']['content'] ?? '';
            $url   = $vm['link']['content']  ?? '';
            if ($url !== '') {
                $result['links'][] = ['title' => $title, 'url' => $url];
            }
        }
    }

    return $result;
}

function findNestedKey(array $data, string $key): ?array
{
    foreach ($data as $k => $v) {
        if ($k === $key && is_array($v)) {
            return $v;
        }
        if (is_array($v)) {
            $found = findNestedKey($v, $key);
            if ($found !== null) {
                return $found;
            }
        }
    }
    return null;
}

function extractEmailsFromText(string $text): array
{
    $emails = [];
    preg_match_all('/[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,255}\.[a-zA-Z]{2,24}/', $text, $matches);
    foreach ($matches[0] as $email) {
        $email = strtolower(trim($email));
        if (isValidEmail($email)) {
            $emails[] = $email;
        }
    }
    return $emails;
}

/**
 * Extract emails from YouTube page HTML.
 * Parses ytInitialData JSON to get channel description and external links,
 * then searches for email patterns in the description text.
 */
function extractYoutubeEmails(string $html): array
{
    $emails = [];

    $about = parseYoutubeAbout($html);

    // Extract emails from channel description
    if ($about['description'] !== '') {
        $emails = array_merge($emails, extractEmailsFromText($about['description']));
    }

    // Extract emails from link URLs and titles
    foreach ($about['links'] as $link) {
        $emails = array_merge($emails, extractEmailsFromText($link['url']));
        $emails = array_merge($emails, extractEmailsFromText($link['title']));
    }

    // Fallback: regex over raw HTML for emails (meta tags, etc.)
    $emails = array_merge($emails, extractEmailsFromText($html));

    return array_values(array_unique($emails));
}

function getRetryCount(string $mark): int
{
    if (preg_match('/^RETRY:(\d+)$/', $mark, $m)) {
        return (int) $m[1];
    }
    return 0;
}

// ── main ─────────────────────────────────────────────────────────────────────

$workerIdx    = 0;
$totalWorkers = 1;

foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/', $arg, $m))        $workerIdx    = (int) $m[1];
    if (preg_match('/^--total-workers=(\d+)$/', $arg, $m)) $totalWorkers = (int) $m[1];
}

$workerLabel = $totalWorkers > 1 ? "[W$workerIdx/$totalWorkers] " : "";

$pdo = getDb();

$eligibleCondition = "
    `youtube_parcing` IS NOT NULL
    AND `youtube_parcing` != ''
    AND `youtube_parcing_email` IS NULL
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

echo "{$workerLabel}YouTube Email Scraper\n";
echo "{$workerLabel}Delay: " . (DELAY_MS / 1000) . "s | Max retries: " . MAX_RETRIES . "\n";
echo "{$workerLabel}Total YouTube channels to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `youtube_parcing_email` = ?
    WHERE `_rowid` = ?
");

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `youtube_parcing`, `youtube_parcing_email`
    FROM `" . DB_TABLE . "`
    WHERE $eligibleCondition
      AND `_rowid` > :lastRowId
    ORDER BY `_rowid` ASC
    LIMIT " . FETCH_BATCH_SIZE . "
");

$processed    = 0;
$foundEmails  = 0;
$noEmail      = 0;
$errors       = 0;
$hasBizEmail  = 0;
$lastRowId    = 0;

while (true) {
    $fetchBatchStmt->execute([
        ':tw'        => $totalWorkers,
        ':wi'        => $workerIdx,
        ':lastRowId' => $lastRowId,
    ]);
    $rows = $fetchBatchStmt->fetchAll();
    if ($rows === []) {
        break;
    }

    foreach ($rows as $row) {
        $rowid      = (int) $row['_rowid'];
        $lastRowId  = $rowid;
        $instructor = $row['instructor'] ?? '';
        $ytRaw      = $row['youtube_parcing'] ?? '';

        // Может быть несколько URL через запятую
        $ytUrls = array_filter(array_map('trim', preg_split('/[,\s]+/', $ytRaw)));
        $ytUrls = array_filter($ytUrls, fn($u) => str_contains($u, 'youtube.com'));

        if (empty($ytUrls)) {
            $updateStmt->execute(['INVALID_URL', $rowid]);
            continue;
        }

        $processed++;
        echo "{$workerLabel}[$processed/$total] rowid=$rowid | $instructor\n";

        $allEmails = [];

        foreach ($ytUrls as $ytUrl) {
            $aboutUrl = youtubeAboutUrl($ytUrl);
            echo "  YouTube: $aboutUrl\n";

            $result = fetchPage($aboutUrl);
            $sizeBytes = strlen($result['html']);
            $sizeStr   = $sizeBytes >= 1024 ? round($sizeBytes / 1024, 1) . ' KB' : $sizeBytes . ' B';

            if ($result['error'] !== '') {
                echo "  ERROR: {$result['error']}\n";
                $errors++;
                continue;
            }

            echo "  HTTP {$result['code']} | $sizeStr\n";

            if ($result['code'] !== 200) {
                echo "  → пропускаем\n";
                $errors++;
                continue;
            }

            $about = parseYoutubeAbout($result['html']);
            if ($about['has_business_email']) {
                $hasBizEmail++;
                echo "  [biz-email: hidden behind login]\n";
            }
            if ($about['description'] !== '') {
                $descLen = strlen($about['description']);
                echo "  Описание: {$descLen} символов\n";
            }
            if (!empty($about['links'])) {
                $linkNames = array_map(fn($l) => $l['title'], $about['links']);
                echo "  Ссылки: " . implode(', ', $linkNames) . "\n";
            }

            $emails = extractYoutubeEmails($result['html']);
            foreach ($emails as $e) {
                if (!in_array($e, $allEmails, true)) {
                    $allEmails[] = $e;
                }
            }

            if ($emails) {
                echo "  Emails: " . implode(', ', $emails) . "\n";
            }
        }

        if (empty($allEmails)) {
            $updateStmt->execute(['NOT_FOUND', $rowid]);
            $noEmail++;
        } else {
            $emailStr = implode(';', $allEmails);
            echo "  ✓ Сохраняем: $emailStr\n";
            $updateStmt->execute([$emailStr, $rowid]);
            $foundEmails++;
        }

        usleep(DELAY_MS * 1000);
    }
}

echo "\n--- Done ---\n";
echo "{$workerLabel}Total channels       : $total\n";
echo "{$workerLabel}Processed            : $processed\n";
echo "{$workerLabel}With emails found    : $foundEmails\n";
echo "{$workerLabel}No email found       : $noEmail\n";
echo "{$workerLabel}Has biz email (hidden): $hasBizEmail\n";
echo "{$workerLabel}Errors               : $errors\n";
