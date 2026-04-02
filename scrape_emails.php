<?php

/**
 * Instructor Email Scraper
 *
 * Reads website_parcing from MySQL leads table,
 * visits each instructor's personal website,
 * extracts email addresses (main page + contact sub-pages),
 * and saves results to email_parcing column.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_USER', 'root');
define('DB_PASS', '');

define('COOKIE_FILE',   sys_get_temp_dir() . '/udemy_email_cookies.txt');
define('DELAY_SECONDS', 0);
define('SUBPAGE_DELAY', 0);
define('MAX_SUBPAGES',  6);
define('MIN_HTML_BYTES', 3000);

const CONTACT_KEYWORDS = [
    'contact', 'about', 'team', 'support', 'reach',
    'hire', 'get-in-touch', 'connect', 'write', 'email',
];

const EMAIL_BLACKLIST = [
    'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
    '@example.com', '@test.com', '@sentry.io', '@cloudflare.com',
    '@googletagmanager.com', '@schema.org', '@wpcf7.invalid',
    '@w3.org', '@wordpress.org', '@jquery.com', '@bootstrapcdn.com',
    'yourname@', 'name@', 'email@', 'info@info', 'user@',
];

// ── DB connection ─────────────────────────────────────────────────────────────

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

// ── helpers ───────────────────────────────────────────────────────────────────

function fetchPage(string $url, string $cookieFile, int $timeout = 20): array
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS      => 5,
        CURLOPT_TIMEOUT        => $timeout,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_ENCODING       => '',
        CURLOPT_COOKIEJAR      => $cookieFile,
        CURLOPT_COOKIEFILE     => $cookieFile,
        CURLOPT_HTTPHEADER     => [
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language: en-US,en;q=0.9',
            'Accept-Encoding: gzip, deflate, br',
            'Cache-Control: no-cache',
            'Sec-Fetch-Dest: document',
            'Sec-Fetch-Mode: navigate',
            'Sec-Fetch-Site: none',
            'Upgrade-Insecure-Requests: 1',
        ],
        CURLOPT_USERAGENT =>
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            . 'AppleWebKit/537.36 (KHTML, like Gecko) '
            . 'Chrome/123.0.0.0 Safari/537.36',
    ]);

    $html     = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $finalUrl = curl_getinfo($ch, CURLINFO_EFFECTIVE_URL);
    $error    = curl_error($ch);
    curl_close($ch);

    if ($error) {
        return ['html' => '', 'code' => 0, 'url' => $url, 'error' => $error];
    }

    return ['html' => (string) $html, 'code' => $httpCode, 'url' => $finalUrl, 'error' => ''];
}

function extractEmails(string $html): array
{
    $emails = [];

    $dom = new DOMDocument();
    @$dom->loadHTML($html, LIBXML_NOERROR | LIBXML_NOWARNING);
    $xpath = new DOMXPath($dom);

    $nodes = $xpath->query('//a[starts-with(translate(@href,"MAILTO","mailto"),"mailto:")]');
    if ($nodes !== false) {
        foreach ($nodes as $node) {
            $href  = $node->getAttribute('href');
            $email = strtolower(trim(preg_replace('/^mailto:/i', '', $href)));
            $email = strtok($email, '?');
            if (isValidEmail($email)) {
                $emails[] = $email;
            }
        }
    }

    preg_match_all('/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/', $html, $matches);
    foreach ($matches[0] as $email) {
        $email = strtolower(trim($email));
        if (isValidEmail($email)) {
            $emails[] = $email;
        }
    }

    return array_values(array_unique($emails));
}

function isValidEmail(string $email): bool
{
    if (!filter_var($email, FILTER_VALIDATE_EMAIL)) {
        return false;
    }
    foreach (EMAIL_BLACKLIST as $bad) {
        if (str_contains($email, $bad)) {
            return false;
        }
    }
    if (preg_match('/\.(png|jpg|gif|css|js|svg|woff|ttf|min)@/i', $email)) {
        return false;
    }
    if (preg_match('/^u00[0-9a-fA-F]{2}/i', $email)) {
        return false;
    }
    return true;
}

/**
 * Find contact/about sub-page URLs within the same domain.
 * Priority 1: keyword in href path. Priority 2: keyword in link text.
 */
function findContactPages(string $html, string $baseUrl): array
{
    $base   = rtrim($baseUrl, '/');
    $host   = parse_url($base, PHP_URL_HOST);
    $scheme = parse_url($base, PHP_URL_SCHEME) ?? 'https';

    $dom = new DOMDocument();
    @$dom->loadHTML($html, LIBXML_NOERROR | LIBXML_NOWARNING);
    $xpath = new DOMXPath($dom);
    $nodes = $xpath->query('//a[@href]');

    $priority1 = [];
    $priority2 = [];

    if ($nodes === false) {
        return [];
    }

    foreach ($nodes as $node) {
        /** @var DOMElement $node */
        $href = trim($node->getAttribute('href'));
        $text = strtolower(trim($node->textContent));

        if ($href === '' || str_starts_with($href, '#') || str_starts_with($href, 'mailto:')) {
            continue;
        }

        if (str_starts_with($href, '//')) {
            $href = $scheme . ':' . $href;
        } elseif (str_starts_with($href, '/')) {
            $href = $scheme . '://' . $host . $href;
        } elseif (!str_starts_with($href, 'http')) {
            $href = $base . '/' . ltrim($href, '/');
        }

        if (parse_url($href, PHP_URL_HOST) !== $host) {
            continue;
        }

        $path   = strtolower(parse_url($href, PHP_URL_PATH) ?? '');
        $inPath = false;
        $inText = false;

        foreach (CONTACT_KEYWORDS as $kw) {
            if (str_contains($path, $kw)) {
                $inPath = true;
                break;
            }
        }
        if (!$inPath) {
            foreach (CONTACT_KEYWORDS as $kw) {
                if (str_contains($text, $kw)) {
                    $inText = true;
                    break;
                }
            }
        }

        if ($inPath && !in_array($href, $priority1, true)) {
            $priority1[] = $href;
        } elseif ($inText && !in_array($href, $priority2, true) && !in_array($href, $priority1, true)) {
            $priority2[] = $href;
        }
    }

    $merged = array_values(array_unique(array_merge($priority1, $priority2)));
    return array_slice($merged, 0, MAX_SUBPAGES);
}

// ── main ──────────────────────────────────────────────────────────────────────

$pdo = getDb();

// Eligible rows: website is set and is a real URL (not a marker), email not yet processed
// RETRY:1 means first HTTP 4xx attempt — include to allow second try
$eligibleCondition = "
    `website_parcing` IS NOT NULL
    AND `website_parcing` != ''
    AND `website_parcing` NOT LIKE 'ERROR:%'
    AND `website_parcing` NOT LIKE 'HTTP_%'
    AND `website_parcing` != 'JS_REQUIRED'
    AND (
        `email_parcing` IS NULL
        OR `email_parcing` = 'RETRY:1'
    )
";

$totalStmt = $pdo->query("SELECT COUNT(*) FROM `leads` WHERE $eligibleCondition");
$total     = (int) $totalStmt->fetchColumn();

echo "Total rows to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `leads`
    SET `email_parcing` = ?
    WHERE `_rowid` = ?
");

$fetchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `website_parcing`, `email_parcing`
    FROM `leads`
    WHERE $eligibleCondition
    ORDER BY `_rowid` ASC
    LIMIT 1
");

$processed    = 0;
$foundEmails  = 0;
$noEmail      = 0;
$jsRequired   = 0;
$errors       = 0;

while (true) {
    $fetchStmt->execute();
    $row = $fetchStmt->fetch();
    if ($row === false) {
        break;
    }

    $rowid       = (int) $row['_rowid'];
    $instructor  = $row['instructor'] ?? '';
    $website     = $row['website_parcing'];
    $currentMark = $row['email_parcing'] ?? '';

    $processed++;
    echo "[$processed/$total] rowid=$rowid | $instructor\n";
    echo "  Site: $website\n";

    $allEmails = [];

    // ── Step 1: fetch main page ───────────────────────────────────────────────
    $result = fetchPage($website, COOKIE_FILE);

    if ($result['error'] !== '') {
        // Temporary network error — leave empty, will retry automatically
        echo "  ERROR (will retry): {$result['error']}\n";
        $errors++;
        continue;
    }

    if ($result['code'] !== 200) {
        $code = $result['code'];
        echo "  HTTP $code\n";
        // 403/404 — permanent, allow 2 attempts then give up
        if (in_array($code, [403, 404], true)) {
            if ($currentMark === 'RETRY:1') {
                echo "  HTTP $code (attempt 2/2) — giving up\n";
                $updateStmt->execute(['HTTP_' . $code, $rowid]);
            } else {
                echo "  HTTP $code (attempt 1/2) — will retry next run\n";
                $updateStmt->execute(['RETRY:1', $rowid]);
            }
        } else {
            $updateStmt->execute(['HTTP_' . $code, $rowid]);
        }
        $errors++;
        continue;
    }

    $htmlLen = strlen($result['html']);
    echo "  Main page: {$htmlLen} bytes\n";

    if ($htmlLen < MIN_HTML_BYTES) {
        echo "  WARNING: Very short HTML — likely JS-rendered\n";
        $updateStmt->execute(['JS_REQUIRED', $rowid]);
        $jsRequired++;
        continue;
    }

    // Extract emails from main page
    $mainEmails = extractEmails($result['html']);
    foreach ($mainEmails as $e) {
        $allEmails[] = $e;
    }
    if ($mainEmails) {
        echo "  Emails on main page: " . implode(', ', $mainEmails) . "\n";
    }

    // ── Step 2: contact sub-pages ─────────────────────────────────────────────
    $subPages = findContactPages($result['html'], $result['url']);
    if ($subPages) {
        echo "  Contact pages: " . implode(', ', $subPages) . "\n";
    }

    foreach ($subPages as $subUrl) {
        echo "  Checking: $subUrl\n";
        if (SUBPAGE_DELAY > 0) {
            sleep(SUBPAGE_DELAY);
        }

        $sub = fetchPage($subUrl, COOKIE_FILE);
        if ($sub['code'] !== 200 || strlen($sub['html']) < MIN_HTML_BYTES) {
            echo "    Skipped (HTTP {$sub['code']})\n";
            continue;
        }

        $subEmails = extractEmails($sub['html']);
        foreach ($subEmails as $e) {
            if (!in_array($e, $allEmails, true)) {
                $allEmails[] = $e;
                $pageName    = parse_url($subUrl, PHP_URL_PATH) ?: '/';
                echo "    Email: $e (on $pageName)\n";
            }
        }
    }

    if (empty($allEmails)) {
        echo "  No emails found\n";
        $updateStmt->execute(['NOT_FOUND', $rowid]);
        $noEmail++;
    } else {
        $emailStr = implode(';', $allEmails);
        echo "  Saving: $emailStr\n";
        $updateStmt->execute([$emailStr, $rowid]);
        $foundEmails++;
    }

    if (DELAY_SECONDS > 0) {
        sleep(DELAY_SECONDS);
    }
}

echo "\n--- Done ---\n";
echo "Total rows           : $total\n";
echo "Processed            : $processed\n";
echo "With emails found    : $foundEmails\n";
echo "No email found       : $noEmail\n";
echo "JS-rendered (manual) : $jsRequired\n";
echo "Errors               : $errors\n";
