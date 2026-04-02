<?php

/**
 * Udemy Instructor Social Links Scraper
 *
 * Reads UdemyProfile1_parcing / UdemyProfile2_parcing / UdemyProfile3_parcing from MySQL leads table,
 * fetches each instructor profile page,
 * extracts social links from the sidebar social links block,
 * and saves results to *_parcing columns:
 *   website_parcing, linkedin_parcing, youtube_parcing, facebook_parcing,
 *   twitter_parcing, instagram_parcing, tiktok_parcing
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_USER', 'root');
define('DB_PASS', '');

define('PROXY_URL', 'socks5h://proxy.packetstream.io:31113');
define('UDEMY_BASE', 'https://www.udemy.com');

// Anti-detection constants
const REFERERS = [
    'https://www.udemy.com/',
    'https://www.udemy.com/courses/search/',
    'https://www.google.com/search?q=udemy+instructor',
    'https://www.udemy.com/home/my-courses/',
];

const ACCEPT_LANGUAGES = [
    'en-US,en;q=0.9',
    'en-GB,en;q=0.9',
    'en-US,en;q=0.8,ru;q=0.6',
];

const BAD_COUNTRIES = ['Mongolia', 'Mauritius', 'Panama'];

const USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',
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

function fetchPage(string $url, string $cookieFile, string $proxyAuth, string $userAgent): string
{
    $isFirefox = str_contains($userAgent, 'Firefox');
    $isSafari  = str_contains($userAgent, 'Safari') && !str_contains($userAgent, 'Chrome');
    $acceptLang = ACCEPT_LANGUAGES[array_rand(ACCEPT_LANGUAGES)];

    $headers = [
        'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language: ' . $acceptLang,
        'Accept-Encoding: gzip, deflate, br, zstd',
        'Cache-Control: max-age=0',
        'Sec-Fetch-Dest: document',
        'Sec-Fetch-Mode: navigate',
        'Sec-Fetch-Site: none',
        'Sec-Fetch-User: ?1',
        'Upgrade-Insecure-Requests: 1',
        'Referer: ' . REFERERS[array_rand(REFERERS)],
    ];

    if (!$isFirefox && !$isSafari) {
        $chromeVer = '136';
        if (preg_match('/Chrome\/(\d+)/', $userAgent, $m)) {
            $chromeVer = $m[1];
        }
        $headers[] = 'sec-ch-ua: "Google Chrome";v="' . $chromeVer . '", "Chromium";v="' . $chromeVer . '", "Not.A/Brand";v="99"';
        $headers[] = 'sec-ch-ua-mobile: ?0';
        $headers[] = 'sec-ch-ua-platform: "macOS"';
    }

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS      => 10,
        CURLOPT_TIMEOUT        => 40,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_ENCODING       => '',
        CURLOPT_COOKIEJAR      => $cookieFile,
        CURLOPT_COOKIEFILE     => $cookieFile,
        CURLOPT_PROXY          => PROXY_URL,
        CURLOPT_PROXYUSERPWD   => $proxyAuth,
        CURLOPT_PROXYTYPE      => CURLPROXY_SOCKS5_HOSTNAME,
        CURLOPT_USERAGENT      => $userAgent,
        CURLOPT_HTTPHEADER     => $headers,
        CURLOPT_HTTP_VERSION   => CURL_HTTP_VERSION_2_0,
    ]);

    $html     = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error    = curl_error($ch);
    curl_close($ch);

    if ($error) {
        throw new RuntimeException("cURL error for $url: $error");
    }
    if ($httpCode !== 200) {
        throw new RuntimeException("HTTP $httpCode for $url");
    }

    return (string) $html;
}

/**
 * Classify a URL into one of the known social network types.
 * Returns the type key or 'website' for unknown links.
 */
function classifyLink(string $url): string
{
    $lower = strtolower($url);

    $patterns = [
        'linkedin'  => ['linkedin.com'],
        'youtube'   => ['youtube.com', 'youtu.be'],
        'facebook'  => ['facebook.com', 'fb.com'],
        'twitter'   => ['twitter.com', 'x.com'],
        'instagram' => ['instagram.com'],
        'tiktok'    => ['tiktok.com'],
    ];

    foreach ($patterns as $type => $domains) {
        foreach ($domains as $domain) {
            if (str_contains($lower, $domain)) {
                return $type;
            }
        }
    }

    return 'website';
}

/**
 * Extract social links from the instructor profile page.
 * Looks for <div> whose class contains "sidebar-social-links-container".
 *
 * @return array{website:string, linkedin:string, youtube:string, facebook:string, twitter:string, instagram:string, tiktok:string}
 */
function extractSocialLinks(string $html): array
{
    $result = [
        'website'   => '',
        'linkedin'  => '',
        'youtube'   => '',
        'facebook'  => '',
        'twitter'   => '',
        'instagram' => '',
        'tiktok'    => '',
    ];

    $dom = new DOMDocument();
    @$dom->loadHTML($html, LIBXML_NOERROR | LIBXML_NOWARNING);

    $xpath = new DOMXPath($dom);

    $containers = $xpath->query('//div[contains(@class,"sidebar-social-links-container")]');

    if ($containers === false || $containers->length === 0) {
        return $result;
    }

    foreach ($containers as $container) {
        $anchors = $xpath->query('.//a[@href]', $container);
        if ($anchors === false) {
            continue;
        }
        foreach ($anchors as $anchor) {
            /** @var DOMElement $anchor */
            $href = trim($anchor->getAttribute('href'));
            if ($href === '') {
                continue;
            }
            $type = classifyLink($href);
            // Keep first found value for each type
            if ($result[$type] === '') {
                $result[$type] = $href;
            }
        }
    }

    return $result;
}

// ── main ──────────────────────────────────────────────────────────────────────

// Markers written by scrape_instructors.php that are not real profile URLs
const PROFILE_SKIP_MARKERS = ['SKIP', 'NOT_FOUND', 'HTTP_ERROR', 'RETRY:1'];

$pdo = getDb();

// Worker support for parallel execution
$workerIdx    = 0;
$totalWorkers = 1;
foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/', $arg, $m))         $workerIdx    = (int) $m[1];
    if (preg_match('/^--total-workers=(\d+)$/', $arg, $m))  $totalWorkers = (int) $m[1];
}

$country    = PROXY_COUNTRIES[$workerIdx] ?? 'Germany';
if (in_array($country, BAD_COUNTRIES, true)) {
    $country = 'Germany';
}
$proxyAuth  = 'acrossoffwest:hu3m3xodmsNrEXUg_country-' . $country;
$cookieFile = sys_get_temp_dir() . '/udemy_social_worker' . $workerIdx . '_' . time() . '.txt';

$workerLabel = $totalWorkers > 1 ? "[W$workerIdx/$totalWorkers] " : "";

// Fetch rows that have at least one real profile URL but social links not yet scraped
$eligibleCondition = "
    (
        (`UdemyProfile1_parcing` IS NOT NULL AND `UdemyProfile1_parcing` != '' AND `UdemyProfile1_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
        OR (`UdemyProfile2_parcing` IS NOT NULL AND `UdemyProfile2_parcing` != '' AND `UdemyProfile2_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
        OR (`UdemyProfile3_parcing` IS NOT NULL AND `UdemyProfile3_parcing` != '' AND `UdemyProfile3_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
    )
    AND `website_parcing` IS NULL
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `leads` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

echo "{$workerLabel}Country: $country | Total rows to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `leads`
    SET `website_parcing`   = ?,
        `linkedin_parcing`  = ?,
        `youtube_parcing`   = ?,
        `facebook_parcing`  = ?,
        `twitter_parcing`   = ?,
        `instagram_parcing` = ?,
        `tiktok_parcing`    = ?
    WHERE `_rowid` = ?
");

$fetchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`,
           `UdemyProfile1_parcing`,
           `UdemyProfile2_parcing`,
           `UdemyProfile3_parcing`
    FROM `leads`
    WHERE $eligibleCondition
    ORDER BY `_rowid` ASC
    LIMIT 1
");

$processed = 0;
$found     = 0;
$errors    = 0;

// Cache already-visited profile URLs within this run to avoid duplicate requests
$visitedProfiles = [];

while (true) {
    $fetchStmt->execute();
    $row = $fetchStmt->fetch();
    if ($row === false) {
        break;
    }

    $rowid      = (int) $row['_rowid'];
    $instructor = $row['instructor'] ?? '';

    // Collect real profile URLs (skip marker values)
    $profileUrls = [];
    foreach (['UdemyProfile1_parcing', 'UdemyProfile2_parcing', 'UdemyProfile3_parcing'] as $col) {
        $url = trim($row[$col] ?? '');
        if ($url !== '' && !in_array($url, PROFILE_SKIP_MARKERS, true)) {
            $profileUrls[] = $url;
        }
    }

    if (empty($profileUrls)) {
        // No real URLs — mark as processed with empty values
        $updateStmt->execute(['', '', '', '', '', '', '', $rowid]);
        continue;
    }

    $processed++;
    echo "[$processed/$total] rowid=$rowid | $instructor\n";

    // Aggregate social links across all profile URLs for this instructor
    $merged = [
        'website'   => '',
        'linkedin'  => '',
        'youtube'   => '',
        'facebook'  => '',
        'twitter'   => '',
        'instagram' => '',
        'tiktok'    => '',
    ];

    foreach ($profileUrls as $profileUrl) {
        echo "  Profile: $profileUrl\n";

        // Use cached result if we already fetched this profile URL
        if (isset($visitedProfiles[$profileUrl])) {
            $social = $visitedProfiles[$profileUrl];
        } else {
            // Pick random User-Agent for this request
            $userAgent = USER_AGENTS[array_rand(USER_AGENTS)];

            try {
                $html   = fetchPage($profileUrl, $cookieFile, $proxyAuth, $userAgent);
                $social = extractSocialLinks($html);
                $visitedProfiles[$profileUrl] = $social;
            } catch (RuntimeException $e) {
                $msg = $e->getMessage();
                if (preg_match('/^HTTP (403|404)/', $msg)) {
                    echo "  HTTP ERROR: $msg\n";
                } else {
                    echo "  ERROR (will retry): $msg\n";
                }
                $visitedProfiles[$profileUrl] = array_fill_keys(array_keys($merged), '');
                $errors++;
                continue;
            }
        }

        // Merge: fill empty slots from this profile's data
        foreach ($merged as $type => $existing) {
            if ($existing === '' && $social[$type] !== '') {
                $merged[$type] = $social[$type];
            }
        }
    }

    $hasAny = array_filter($merged, fn($v) => $v !== '');
    if ($hasAny) {
        echo "  Found: " . implode(', ', array_map(
            fn($k, $v) => "$k=$v",
            array_keys($hasAny),
            $hasAny
        )) . "\n";
        $found++;
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
}

echo "\n--- Done ---\n";
echo "{$workerLabel}Total rows              : $total\n";
echo "{$workerLabel}Processed               : $processed\n";
echo "{$workerLabel}With social links found : $found\n";
echo "{$workerLabel}Errors                  : $errors\n";
