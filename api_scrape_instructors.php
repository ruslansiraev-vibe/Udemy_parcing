<?php

/**
 * Udemy Social Links Scraper (via profile page HTML)
 *
 * Reads UdemyProfile1_parcing / UdemyProfile2_parcing / UdemyProfile3_parcing
 * from MySQL leads table, fetches each instructor profile page,
 * extracts social links from the embedded JSON block "social_links":{...},
 * and saves results to:
 *   website_parcing, linkedin_parcing, youtube_parcing, facebook_parcing,
 *   twitter_parcing, instagram_parcing, tiktok_parcing
 *
 * Uses proxy (packetstream.io) via curl-impersonate to bypass Cloudflare.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

define('DELAY_MS', 3000);
define('REQUEST_TIMEOUT', 40);
define('UDEMY_BASE', 'https://www.udemy.com');

define('CURL_IMPERSONATE_BIN', '/tmp/curl-impersonate');

define('PROXY_HOST', 'proxy.packetstream.io');
define('PROXY_PORT', 31113);
define('PROXY_USER', 'acrossoffwest');
define('PROXY_PASS', 'hu3m3xodmsNrEXUg');

const BAD_COUNTRIES = ['Mongolia', 'Mauritius', 'Panama'];

const PROXY_COUNTRIES = [
    0  => 'United States',
    1  => 'United Kingdom',
    2  => 'Germany',
    3  => 'France',
    4  => 'Canada',
    5  => 'Australia',
    6  => 'Netherlands',
    7  => 'Sweden',
    8  => 'Norway',
    9  => 'Denmark',
    10 => 'Finland',
    11 => 'Switzerland',
    12 => 'Austria',
    13 => 'Belgium',
    14 => 'Spain',
    15 => 'Italy',
    16 => 'Poland',
    17 => 'Czech Republic',
    18 => 'Portugal',
    19 => 'Hungary',
];

const PROFILE_SKIP_MARKERS = ['SKIP', 'NOT_FOUND', 'HTTP_ERROR', 'RETRY:1'];


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

/**
 * Fetch a URL via curl-impersonate with proxy, return [httpCode, body].
 */
function curlFetch(string $url, string $proxyAuth): array
{
    $cmd = sprintf(
        '%s -s -L --max-time %d --compressed '
        . '--proxy socks5h://%s:%d '
        . '--proxy-user %s '
        . '-o - -w "\\n%%{http_code}" '
        . '%s 2>/dev/null',
        escapeshellarg(CURL_IMPERSONATE_BIN),
        REQUEST_TIMEOUT,
        PROXY_HOST,
        PROXY_PORT,
        escapeshellarg($proxyAuth),
        escapeshellarg($url)
    );

    $outputLines = [];
    $exitCode    = 0;
    exec($cmd, $outputLines, $exitCode);
    $raw = implode("\n", $outputLines);

    if ($exitCode !== 0) {
        if ($exitCode === 28) {
            throw new RuntimeException("CURL_TIMEOUT for $url");
        }
        if (in_array($exitCode, [5, 6, 7, 52, 56, 97], true)) {
            throw new RuntimeException("CURL_CONNECT for $url: exit $exitCode");
        }
        throw new RuntimeException("CURL_ERROR for $url: exit $exitCode");
    }

    $lastNl = strrpos($raw, "\n");
    if ($lastNl === false) {
        throw new RuntimeException("CURL_ERROR for $url: unexpected output");
    }

    $httpCode = (int) trim(substr($raw, $lastNl + 1));
    $body     = substr($raw, 0, $lastNl);

    return [$httpCode, $body];
}

/**
 * Extract social links from instructor profile page HTML.
 * Looks for the embedded JSON block: "social_links":{"twitter":"...","facebook":"...",...}
 *
 * @return array{website:string, linkedin:string, youtube:string, facebook:string, twitter:string, instagram:string, tiktok:string}
 */
function extractSocialLinks(string $html): array
{
    $empty = [
        'website'   => '',
        'linkedin'  => '',
        'youtube'   => '',
        'facebook'  => '',
        'twitter'   => '',
        'instagram' => '',
        'tiktok'    => '',
    ];

    // The page embeds HTML-encoded JSON: &quot;social_links&quot;:{...}
    // After html_entity_decode it becomes: "social_links":{...}
    $decoded = html_entity_decode($html, ENT_QUOTES | ENT_HTML5, 'UTF-8');

    if (!preg_match('/"social_links"\s*:\s*(\{[^}]+\})/', $decoded, $m)) {
        return $empty;
    }

    $json = json_decode($m[1], true);
    if (!is_array($json)) {
        return $empty;
    }

    return [
        'website'   => trim($json['personal_website'] ?? ''),
        'linkedin'  => trim($json['linkedin']         ?? ''),
        'youtube'   => trim($json['youtube']          ?? ''),
        'facebook'  => trim($json['facebook']         ?? ''),
        'twitter'   => trim($json['twitter']          ?? ''),
        'instagram' => trim($json['instagram']        ?? ''),
        'tiktok'    => trim($json['tiktok']           ?? ''),
    ];
}

// ── main ──────────────────────────────────────────────────────────────────────

$workerIdx    = 0;
$totalWorkers = 1;

foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/', $arg, $m))         $workerIdx    = (int) $m[1];
    if (preg_match('/^--total-workers=(\d+)$/', $arg, $m))  $totalWorkers = (int) $m[1];
}

$country = PROXY_COUNTRIES[$workerIdx % count(PROXY_COUNTRIES)] ?? 'Germany';
if (in_array($country, BAD_COUNTRIES, true)) {
    $country = 'Germany';
}
$proxyAuth  = PROXY_USER . ':' . PROXY_PASS . '_country-' . $country;
$workerLabel = $totalWorkers > 1 ? "[W$workerIdx/$totalWorkers] " : "";

$pdo = getDb();

// Rows eligible: have at least one real profile URL, not yet scraped (social_scraped IS NULL)
$eligibleCondition = "
    (
        (`UdemyProfile1_parcing` IS NOT NULL AND `UdemyProfile1_parcing` != ''
            AND `UdemyProfile1_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
        OR (`UdemyProfile2_parcing` IS NOT NULL AND `UdemyProfile2_parcing` != ''
            AND `UdemyProfile2_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
        OR (`UdemyProfile3_parcing` IS NOT NULL AND `UdemyProfile3_parcing` != ''
            AND `UdemyProfile3_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
    )
    AND `social_scraped` IS NULL
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

echo "{$workerLabel}Country: $country | Delay: " . (DELAY_MS / 1000) . "s\n";
echo "{$workerLabel}Total rows to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `website_parcing`   = ?,
        `linkedin_parcing`  = ?,
        `youtube_parcing`   = ?,
        `facebook_parcing`  = ?,
        `twitter_parcing`   = ?,
        `instagram_parcing` = ?,
        `tiktok_parcing`    = ?,
        `social_scraped`    = 1
    WHERE `_rowid` = ?
");

$fetchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`,
           `UdemyProfile1_parcing`,
           `UdemyProfile2_parcing`,
           `UdemyProfile3_parcing`
    FROM `" . DB_TABLE . "`
    WHERE $eligibleCondition
    ORDER BY `_rowid` ASC
    LIMIT 1
");

$processed   = 0;
$found       = 0;
$errors      = 0;

// Cache already-visited profile URLs within this run
$visitedProfiles = [];

while (true) {
    $fetchStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
    $row = $fetchStmt->fetch();
    if ($row === false) {
        break;
    }

    $rowid      = (int) $row['_rowid'];
    $instructor = $row['instructor'] ?? '';

    // Collect real profile URLs
    $profileUrls = [];
    foreach (['UdemyProfile1_parcing', 'UdemyProfile2_parcing', 'UdemyProfile3_parcing'] as $col) {
        $url = trim($row[$col] ?? '');
        if ($url !== '' && !in_array($url, PROFILE_SKIP_MARKERS, true)) {
            $profileUrls[] = $url;
        }
    }

    if (empty($profileUrls)) {
        $updateStmt->execute(['', '', '', '', '', '', '', $rowid]);
        continue;
    }

    $processed++;
    echo "{$workerLabel}[$processed/$total] rowid=$rowid | $instructor\n";

    $merged = [
        'website'   => '',
        'linkedin'  => '',
        'youtube'   => '',
        'facebook'  => '',
        'twitter'   => '',
        'instagram' => '',
        'tiktok'    => '',
    ];

    $networkError = false; // true = temporary error, do NOT mark as done

    foreach ($profileUrls as $profileUrl) {
        echo "  Profile: $profileUrl\n";

        if (isset($visitedProfiles[$profileUrl])) {
            $social = $visitedProfiles[$profileUrl];
        } else {
            try {
                [$httpCode, $html] = curlFetch($profileUrl, $proxyAuth);

                if (in_array($httpCode, [404, 410], true)) {
                    // Profile deleted — treat as done, no data
                    echo "  HTTP $httpCode (profile gone) — skip\n";
                    $visitedProfiles[$profileUrl] = array_fill_keys(array_keys($merged), '');
                    continue;
                }

                if ($httpCode !== 200) {
                    // Temporary block (403, 429, 5xx) — do not mark as done
                    echo "  HTTP $httpCode (temporary) — will retry later\n";
                    $networkError = true;
                    $errors++;
                    continue;
                }

                $social = extractSocialLinks($html);
                $visitedProfiles[$profileUrl] = $social;

            } catch (RuntimeException $e) {
                $msg = $e->getMessage();
                echo "  ERROR: $msg\n";
                // CURL_TIMEOUT / CURL_CONNECT = network issue, retry later
                $networkError = true;
                $errors++;
                continue;
            }
        }

        // Fill empty slots from this profile
        foreach ($merged as $type => $existing) {
            if ($existing === '' && $social[$type] !== '') {
                $merged[$type] = $social[$type];
            }
        }
    }

    // If any URL had a network/proxy error — skip saving, row stays unprocessed
    if ($networkError) {
        echo "  ↺ Skipped (network error) — will retry on next run\n";
        usleep(DELAY_MS * 1000);
        continue;
    }

    $hasSocial = array_filter($merged, fn($v) => $v !== '');
    if ($hasSocial) {
        $found++;
        $str = implode(', ', array_map(fn($k, $v) => "$k=$v", array_keys($hasSocial), $hasSocial));
        echo "  ✓ Found: $str\n";
    } else {
        echo "  No social links found\n";
    }

    $updateStmt->execute([
        $merged['website'],
        $merged['linkedin'],
        $merged['youtube'],
        $merged['facebook'],
        $merged['twitter'],
        $merged['instagram'],
        $merged['tiktok'],
        $rowid,
    ]);

    usleep(DELAY_MS * 1000);
}

echo "\n--- Done ---\n";
echo "{$workerLabel}Total rows              : $total\n";
echo "{$workerLabel}Processed               : $processed\n";
echo "{$workerLabel}With social links found : $found\n";
echo "{$workerLabel}Errors                  : $errors\n";
