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
define('REQUEST_TIMEOUT', 20);
define('CONNECT_TIMEOUT', 10);
define('FETCH_BATCH_SIZE', 100);
define('UDEMY_BASE', 'https://www.udemy.com');

define('PROXY_HOST', 'pool.proxy.market:10000');

$homeDir = getenv('HOME') ?: posix_getpwuid(posix_getuid())['dir'];
define('CURL_IMPERSONATE_BIN', $homeDir . '/.local/bin/curl-impersonate/curl-impersonate');
define('BROWSER_FETCH_SCRIPT', __DIR__ . '/browser_fetch.js');

const IMPERSONATE_PROFILES = [
    'chrome136',
    'chrome131',
    'chrome124',
    'safari184',
    'safari180',
    'firefox135',
];

const PROXY_CREDENTIALS = [
    // ['OIMJdnpjtT1c',  'cjZ39JKTb46rxGI0'],
    // ['1VaiOaQsoSrL',  'WAbz24gQmVU76qJr'],
    // ['p9EW1LsljUj0',  'rPSydjoNFkiHmI87'],
    // ['ihYEyLalonTh',  'Z0eoUKGbg7sjVTDw'],
    // ['ZNEXEhty5pOn',  'ol5s8WP7hZwSGXiB'],
    // ['mRf5WwIvQpl3',  'UQtnzsiWv7gEXRSV'],
    // ['6vTA1pLleULX',  'mnhyq3zMD8ZY5rRT'],
    // ['LWEaMenGYrBa',  'dAFsy0MopaUhEJZ3'],
    // ['UlSx7mp6OPP5',  'hwJAGPpyTj6i23v4'],
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
 * Менеджер активных прокси.
 * ban(login)  — исключает прокси из ротации навсегда в этом запуске.
 * next()      — возвращает следующий активный прокси (round-robin).
 * count()     — количество оставшихся активных прокси.
 * list()      — массив активных логинов.
 */
class ProxyPool
{
    private array $active;
    private array $banned = [];
    private int   $idx    = 0;

    public function __construct()
    {
        $this->active = array_values(PROXY_CREDENTIALS);
    }

    public function ban(string $login): void
    {
        $before = count($this->active);
        $this->active = array_values(
            array_filter($this->active, fn($c) => $c[0] !== $login)
        );
        if (count($this->active) < $before) {
            $this->banned[] = $login;
            // сдвигаем индекс чтобы не пропустить следующий
            if ($this->idx > 0) $this->idx--;
        }
    }

    /** Возвращает ['login:pass', 'login'] */
    public function next(): array
    {
        if (empty($this->active)) {
            return ['', ''];
        }
        $creds = $this->active[$this->idx % count($this->active)];
        $this->idx++;
        return [$creds[0] . ':' . $creds[1], $creds[0]];
    }

    public function count(): int  { return count($this->active); }
    public function banned(): array { return $this->banned; }
    public function isEmpty(): bool { return empty($this->active); }
}

class ProxyRequestException extends RuntimeException
{
    public function __construct(
        public string $type,
        public string $url,
        public string $proxyLogin,
        string $details = ''
    ) {
        $message = $type . ' for ' . $url;
        if ($proxyLogin !== '') {
            $message .= ' via proxy ' . $proxyLogin;
        }
        if ($details !== '') {
            $message .= ': ' . $details;
        }

        parent::__construct($message);
    }
}

// Глобальный пул прокси (инициализируется в main-блоке)
$proxyPool = null;

/**
 * Fetch a URL via curl-impersonate with browser TLS fingerprint.
 * Uses proxy from pool when available, falls back to direct connection.
 * Returns [httpCode, body, proxyLogin].
 */
function curlFetch(string $url): array
{
    global $proxyPool;

    $profile = IMPERSONATE_PROFILES[array_rand(IMPERSONATE_PROFILES)];

    [$proxyUserPass, $proxyLogin] = $proxyPool->next();

    $cmd = [
        CURL_IMPERSONATE_BIN,
        '--impersonate', $profile,
        '--max-time', (string) REQUEST_TIMEOUT,
        '--connect-timeout', (string) CONNECT_TIMEOUT,
        '-L',                    // follow redirects
        '--max-redirs', '5',
        '-s',                    // silent
        '-w', '\n__HTTP_CODE__:%{http_code}',
        '--compressed',
    ];

    if ($proxyUserPass !== '') {
        $cmd[] = '-x';
        $cmd[] = 'http://' . $proxyUserPass . '@' . PROXY_HOST;
    }

    $cmd[] = $url;

    $descriptors = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    $process = proc_open($cmd, $descriptors, $pipes);
    if (!is_resource($process)) {
        throw new ProxyRequestException('CURL_ERROR', $url, $proxyLogin, 'Failed to start curl-impersonate');
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $exitCode = proc_close($process);

    if ($exitCode !== 0) {
        $stderr = trim($stderr);
        if ($exitCode === 28) {
            throw new ProxyRequestException('CURL_TIMEOUT', $url, $proxyLogin);
        }
        if (in_array($exitCode, [7, 5, 6], true)) {
            throw new ProxyRequestException('CURL_CONNECT', $url, $proxyLogin, $stderr);
        }
        throw new ProxyRequestException('CURL_ERROR', $url, $proxyLogin, "exit=$exitCode $stderr");
    }

    $httpCode = 0;
    $body = $stdout;
    if (preg_match('/\n__HTTP_CODE__:(\d+)$/', $stdout, $m)) {
        $httpCode = (int) $m[1];
        $body = substr($stdout, 0, -strlen($m[0]));
    }

    return [$httpCode, $body, $proxyLogin];
}

/**
 * Fallback: fetch URL via headless Playwright browser.
 * Used when curl-impersonate gets a Cloudflare challenge (403).
 * Returns [httpCode, body] or throws on failure.
 */
function browserFetch(string $url): array
{
    global $proxyPool;

    $cmd = ['node', BROWSER_FETCH_SCRIPT, $url];

    [$proxyUserPass, ] = $proxyPool->next();
    if ($proxyUserPass !== '') {
        $cmd[] = 'http://' . $proxyUserPass . '@' . PROXY_HOST;
    }

    $descriptors = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    $process = proc_open($cmd, $descriptors, $pipes);
    if (!is_resource($process)) {
        throw new RuntimeException('Failed to start browser_fetch.js');
    }

    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    proc_close($process);

    $result = json_decode($stdout, true);
    if (!is_array($result) || isset($result['error']) && $result['error'] !== null) {
        $errMsg = $result['error'] ?? trim($stderr) ?: 'Unknown browser error';
        throw new RuntimeException("BROWSER_ERROR: $errMsg");
    }

    return [(int) ($result['httpCode'] ?? 0), (string) ($result['body'] ?? '')];
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

$proxyPool = new ProxyPool();

if ($proxyPool->isEmpty()) {
    echo "{$workerLabel}Mode: DIRECT (no proxies, curl-impersonate only) | Delay: " . (DELAY_MS / 1000) . "s\n";
} else {
    echo "{$workerLabel}Proxy: " . PROXY_HOST . " (" . $proxyPool->count() . " accounts, round-robin) | Delay: " . (DELAY_MS / 1000) . "s\n";
}
echo "{$workerLabel}Engine: curl-impersonate (browser TLS fingerprint)\n";
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

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`,
           `UdemyProfile1_parcing`,
           `UdemyProfile2_parcing`,
           `UdemyProfile3_parcing`
    FROM `" . DB_TABLE . "`
    WHERE $eligibleCondition
      AND `_rowid` > :lastRowId
    ORDER BY `_rowid` ASC
    LIMIT " . FETCH_BATCH_SIZE . "
");

$processed        = 0;
$found            = 0;
$errors           = 0;
$consecutiveErrors = 0;      // подряд идущих ошибок (признак бана IP)
$BAN_THRESHOLD    = 5;       // после N подряд — предупреждение о бане
$BAN_PAUSE_SEC    = 30;      // пауза при обнаружении бана (секунды)

// Счётчики по типу ошибки для итоговой статистики
$errorStats = [];

// Cache already-visited profile URLs within this run
$visitedProfiles = [];
$lastRowId = 0;
$stopProcessing = false;

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

        // Collect real profile URLs once per row.
        $profileUrls = [];
        foreach (['UdemyProfile1_parcing', 'UdemyProfile2_parcing', 'UdemyProfile3_parcing'] as $col) {
            $url = trim($row[$col] ?? '');
            if ($url !== '' && !in_array($url, PROFILE_SKIP_MARKERS, true)) {
                $profileUrls[] = $url;
            }
        }
        $profileUrls = array_values(array_unique($profileUrls));

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
                    [$httpCode, $html, $usedProxy] = curlFetch($profileUrl);

                    $sizeBytes = strlen($html);
                    $sizeStr   = $sizeBytes >= 1024
                        ? round($sizeBytes / 1024, 1) . ' KB'
                        : $sizeBytes . ' B';
                    echo "  HTTP $httpCode | $sizeStr | proxy: $usedProxy\n";

                    if (in_array($httpCode, [404, 410], true)) {
                        echo "  → профиль удалён, пропускаем\n";
                        $visitedProfiles[$profileUrl] = array_fill_keys(array_keys($merged), '');
                        $consecutiveErrors = 0;
                        continue;
                    }

                    if ($httpCode === 403) {
                        echo "  → 403 от curl-impersonate, пробуем headless browser...\n";
                        try {
                            [$httpCode, $html] = browserFetch($profileUrl);
                            $sizeBytes = strlen($html);
                            $sizeStr   = $sizeBytes >= 1024
                                ? round($sizeBytes / 1024, 1) . ' KB'
                                : $sizeBytes . ' B';
                            echo "  Browser: HTTP $httpCode | $sizeStr\n";
                        } catch (RuntimeException $be) {
                            echo "  Browser fallback failed: " . $be->getMessage() . "\n";
                            $httpCode = 403;
                        }
                    }

                    if ($httpCode === 403) {
                        $errorStats['HTTP_403'] = ($errorStats['HTTP_403'] ?? 0) + 1;
                        $consecutiveErrors++;
                        if ($usedProxy !== '') {
                            $proxyPool->ban($usedProxy);
                        }
                        $active = $proxyPool->count();
                        echo "  → Cloudflare блокировка";
                        if ($usedProxy !== '') {
                            echo " [$usedProxy] → исключён | активных: $active";
                        }
                        echo "\n";
                        $networkError = true;
                        $errors++;
                    } elseif ($httpCode === 429) {
                        $errorStats['HTTP_429'] = ($errorStats['HTTP_429'] ?? 0) + 1;
                        $consecutiveErrors++;
                        echo "  → Too Many Requests (подряд: $consecutiveErrors)\n";
                        $networkError = true;
                        $errors++;
                    } elseif ($httpCode >= 500) {
                        $errorStats["HTTP_$httpCode"] = ($errorStats["HTTP_$httpCode"] ?? 0) + 1;
                        $consecutiveErrors++;
                        echo "  → ошибка сервера (подряд: $consecutiveErrors)\n";
                        $networkError = true;
                        $errors++;
                    } elseif ($httpCode !== 200) {
                        $errorStats["HTTP_$httpCode"] = ($errorStats["HTTP_$httpCode"] ?? 0) + 1;
                        $consecutiveErrors++;
                        echo "  → неожиданный ответ (подряд: $consecutiveErrors)\n";
                        $networkError = true;
                        $errors++;
                    } else {
                        $consecutiveErrors = 0;
                        $social = extractSocialLinks($html);
                        $visitedProfiles[$profileUrl] = $social;
                    }

                    // Предупреждение если много ошибок подряд (не 403 — те уже исключаются)
                    if ($consecutiveErrors >= $BAN_THRESHOLD) {
                        echo "\n  ⚠️  ВНИМАНИЕ: $consecutiveErrors ошибок подряд!\n";
                        echo "  Статистика ошибок: " . json_encode($errorStats) . "\n";
                        echo "  Активных прокси: " . $proxyPool->count() . "\n";
                        echo "  Пауза $BAN_PAUSE_SEC сек перед продолжением...\n\n";
                        sleep($BAN_PAUSE_SEC);
                        $consecutiveErrors = 0;
                    }

                    if ($networkError || $stopProcessing) {
                        break;
                    }

                } catch (ProxyRequestException $e) {
                    $consecutiveErrors++;

                    if ($e->type === 'CURL_TIMEOUT') {
                        $errorStats['TIMEOUT'] = ($errorStats['TIMEOUT'] ?? 0) + 1;
                        echo "  TIMEOUT — превышено время ожидания";
                        if ($e->proxyLogin !== '') {
                            echo " | proxy: {$e->proxyLogin}";
                        }
                        echo " (подряд: $consecutiveErrors)\n";
                    } elseif ($e->type === 'CURL_CONNECT') {
                        $errorStats['CONNECT'] = ($errorStats['CONNECT'] ?? 0) + 1;
                        if ($e->proxyLogin !== '') {
                            $proxyPool->ban($e->proxyLogin);
                        }
                        echo "  CONNECT ERROR — нет соединения с прокси";
                        if ($e->proxyLogin !== '') {
                            echo " [{$e->proxyLogin}]";
                        }
                        echo " | активных: " . $proxyPool->count() . " (подряд: $consecutiveErrors)\n";
                        if ($proxyPool->isEmpty()) {
                            echo "\n  💀 ВСЕ ПРОКСИ ЗАБЛОКИРОВАНЫ! Завершаем работу.\n";
                            $stopProcessing = true;
                            break;
                        }
                    } else {
                        $errorStats['OTHER'] = ($errorStats['OTHER'] ?? 0) + 1;
                        echo "  ERROR: " . $e->getMessage() . " (подряд: $consecutiveErrors)\n";
                    }

                    if ($consecutiveErrors >= $BAN_THRESHOLD) {
                        echo "\n  ⚠️  ВНИМАНИЕ: $consecutiveErrors ошибок подряд!\n";
                        echo "  Статистика ошибок: " . json_encode($errorStats) . "\n";
                        echo "  Активных прокси: " . $proxyPool->count() . "\n";
                        echo "  Пауза $BAN_PAUSE_SEC сек перед продолжением...\n\n";
                        sleep($BAN_PAUSE_SEC);
                        $consecutiveErrors = 0;
                    }

                    $networkError = true;
                    $errors++;
                    break;
                }
            }

            if ($stopProcessing) {
                break;
            }

            // Fill empty slots from this profile
            foreach ($merged as $type => $existing) {
                if ($existing === '' && $social[$type] !== '') {
                    $merged[$type] = $social[$type];
                }
            }
        }

        if ($stopProcessing) {
            break;
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

    if ($stopProcessing) {
        break;
    }
}

echo "\n--- Done ---\n";
echo "{$workerLabel}Total rows              : $total\n";
echo "{$workerLabel}Processed               : $processed\n";
echo "{$workerLabel}With social links found : $found\n";
echo "{$workerLabel}Errors total            : $errors\n";
if (!empty($errorStats)) {
    echo "{$workerLabel}Error breakdown:\n";
    arsort($errorStats);
    foreach ($errorStats as $type => $count) {
        echo "{$workerLabel}  $type: $count\n";
    }
}
$banned = $proxyPool->banned();
if (!empty($banned)) {
    echo "{$workerLabel}Banned proxies (" . count($banned) . "): " . implode(', ', $banned) . "\n";
}
echo "{$workerLabel}Active proxies remaining: " . $proxyPool->count() . "\n";
