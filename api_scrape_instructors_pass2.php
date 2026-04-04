<?php

/**
 * Second-pass social scraper.
 *
 * Goes through rows that already have social_scraped=1 but still have
 * unvisited UdemyProfile2_parcing / UdemyProfile3_parcing URLs.
 *
 * For each new profile found, appends new social links to existing values
 * via comma-separated merge (no duplicates, no overwriting).
 *
 * Marks completed rows with social_scraped=2.
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
    // ...
];

const PROFILE_SKIP_MARKERS = ['SKIP', 'NOT_FOUND', 'HTTP_ERROR', 'RETRY:1'];

const SOCIAL_COLUMNS = [
    'website'   => 'website_parcing',
    'linkedin'  => 'linkedin_parcing',
    'youtube'   => 'youtube_parcing',
    'facebook'  => 'facebook_parcing',
    'twitter'   => 'twitter_parcing',
    'instagram' => 'instagram_parcing',
    'tiktok'    => 'tiktok_parcing',
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

// ── Proxy ────────────────────────────────────────────────────────────────────

class ProxyPool
{
    private array $active;
    private array $banned = [];
    private int   $idx    = 0;

    public function __construct() { $this->active = array_values(PROXY_CREDENTIALS); }

    public function ban(string $login): void
    {
        $before = count($this->active);
        $this->active = array_values(array_filter($this->active, fn($c) => $c[0] !== $login));
        if (count($this->active) < $before) {
            $this->banned[] = $login;
            if ($this->idx > 0) $this->idx--;
        }
    }

    public function next(): array
    {
        if (empty($this->active)) return ['', ''];
        $c = $this->active[$this->idx % count($this->active)];
        $this->idx++;
        return [$c[0] . ':' . $c[1], $c[0]];
    }

    public function count(): int   { return count($this->active); }
    public function isEmpty(): bool { return empty($this->active); }
    public function banned(): array { return $this->banned; }
}

class ProxyRequestException extends RuntimeException
{
    public function __construct(
        public string $type,
        public string $url,
        public string $proxyLogin,
        string $details = ''
    ) {
        $msg = $type . ' for ' . $url;
        if ($proxyLogin !== '') $msg .= ' via proxy ' . $proxyLogin;
        if ($details !== '')    $msg .= ': ' . $details;
        parent::__construct($msg);
    }
}

$proxyPool = null;

// ── Fetch ────────────────────────────────────────────────────────────────────

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
        '-L', '--max-redirs', '5', '-s',
        '-w', '\n__HTTP_CODE__:%{http_code}',
        '--compressed',
    ];
    if ($proxyUserPass !== '') {
        $cmd[] = '-x';
        $cmd[] = 'http://' . $proxyUserPass . '@' . PROXY_HOST;
    }
    $cmd[] = $url;

    $process = proc_open($cmd, [0 => ['pipe','r'], 1 => ['pipe','w'], 2 => ['pipe','w']], $pipes);
    if (!is_resource($process)) {
        throw new ProxyRequestException('CURL_ERROR', $url, $proxyLogin, 'Failed to start curl-impersonate');
    }
    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]); fclose($pipes[2]);
    $exitCode = proc_close($process);

    if ($exitCode !== 0) {
        $stderr = trim($stderr);
        if ($exitCode === 28) throw new ProxyRequestException('CURL_TIMEOUT', $url, $proxyLogin);
        if (in_array($exitCode, [7, 5, 6], true)) throw new ProxyRequestException('CURL_CONNECT', $url, $proxyLogin, $stderr);
        throw new ProxyRequestException('CURL_ERROR', $url, $proxyLogin, "exit=$exitCode $stderr");
    }

    $httpCode = 0; $body = $stdout;
    if (preg_match('/\n__HTTP_CODE__:(\d+)$/', $stdout, $m)) {
        $httpCode = (int) $m[1];
        $body = substr($stdout, 0, -strlen($m[0]));
    }
    return [$httpCode, $body, $proxyLogin];
}

function browserFetch(string $url): array
{
    global $proxyPool;
    $cmd = ['node', BROWSER_FETCH_SCRIPT, $url];
    [$proxyUserPass, ] = $proxyPool->next();
    if ($proxyUserPass !== '') $cmd[] = 'http://' . $proxyUserPass . '@' . PROXY_HOST;

    $process = proc_open($cmd, [0=>['pipe','r'],1=>['pipe','w'],2=>['pipe','w']], $pipes);
    if (!is_resource($process)) throw new RuntimeException('Failed to start browser_fetch.js');
    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]); fclose($pipes[2]);
    proc_close($process);

    $result = json_decode($stdout, true);
    if (!is_array($result) || isset($result['error']) && $result['error'] !== null) {
        throw new RuntimeException("BROWSER_ERROR: " . ($result['error'] ?? trim($stderr) ?: 'Unknown'));
    }
    return [(int) ($result['httpCode'] ?? 0), (string) ($result['body'] ?? '')];
}

function extractSocialLinks(string $html): array
{
    $empty = array_fill_keys(array_keys(SOCIAL_COLUMNS), '');
    $decoded = html_entity_decode($html, ENT_QUOTES | ENT_HTML5, 'UTF-8');
    if (!preg_match('/"social_links"\s*:\s*(\{[^}]+\})/', $decoded, $m)) return $empty;
    $json = json_decode($m[1], true);
    if (!is_array($json)) return $empty;
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

/**
 * Merge new value into existing comma-separated field.
 * Returns merged string or original if nothing new.
 */
function mergeField(string $existing, string $new): string
{
    $new = trim($new);
    if ($new === '') return $existing;

    $existing = trim($existing);
    if ($existing === '') return $new;

    $parts = array_map('trim', explode(',', $existing));
    if (in_array($new, $parts, true)) return $existing;

    return $existing . ', ' . $new;
}

function fetchProfile(string $url): ?array
{
    try {
        [$httpCode, $html, $usedProxy] = curlFetch($url);

        $sizeStr = strlen($html) >= 1024
            ? round(strlen($html) / 1024, 1) . ' KB'
            : strlen($html) . ' B';
        echo "    HTTP $httpCode | $sizeStr | proxy: $usedProxy\n";

        if (in_array($httpCode, [404, 410], true)) {
            echo "    → профиль удалён\n";
            return array_fill_keys(array_keys(SOCIAL_COLUMNS), '');
        }

        if ($httpCode === 403) {
            echo "    → 403, пробуем headless browser...\n";
            try {
                [$httpCode, $html] = browserFetch($url);
                echo "    Browser: HTTP $httpCode | " . round(strlen($html)/1024,1) . " KB\n";
            } catch (RuntimeException $e) {
                echo "    Browser fallback failed: " . $e->getMessage() . "\n";
                return null;
            }
        }

        if ($httpCode !== 200) {
            echo "    → HTTP $httpCode — пропускаем\n";
            return null;
        }

        return extractSocialLinks($html);

    } catch (ProxyRequestException $e) {
        echo "    ERROR: " . $e->getMessage() . "\n";
        return null;
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

$workerIdx    = 0;
$totalWorkers = 1;

foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/', $arg, $m))         $workerIdx    = (int) $m[1];
    if (preg_match('/^--total-workers=(\d+)$/', $arg, $m))  $totalWorkers = (int) $m[1];
}

$workerLabel = $totalWorkers > 1 ? "[W$workerIdx/$totalWorkers] " : "";
$pdo = getDb();

$eligibleCondition = "
    `social_scraped` = 1
    AND (
        (`UdemyProfile2_parcing` IS NOT NULL AND `UdemyProfile2_parcing` != ''
            AND `UdemyProfile2_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
        OR (`UdemyProfile3_parcing` IS NOT NULL AND `UdemyProfile3_parcing` != ''
            AND `UdemyProfile3_parcing` NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR','RETRY:1'))
    )
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

$proxyPool = new ProxyPool();

echo "{$workerLabel}=== PASS 2: дополнение из Profile2/Profile3 ===\n";
if ($proxyPool->isEmpty()) {
    echo "{$workerLabel}Mode: DIRECT (curl-impersonate) | Delay: " . (DELAY_MS / 1000) . "s\n";
} else {
    echo "{$workerLabel}Proxy: " . PROXY_HOST . " (" . $proxyPool->count() . " accounts) | Delay: " . (DELAY_MS / 1000) . "s\n";
}
echo "{$workerLabel}Total rows to process: $total\n\n";

$socialColList = implode(', ', array_map(fn($c) => "`$c`", array_values(SOCIAL_COLUMNS)));

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`,
           `UdemyProfile1_parcing`,
           `UdemyProfile2_parcing`,
           `UdemyProfile3_parcing`,
           $socialColList
    FROM `" . DB_TABLE . "`
    WHERE $eligibleCondition
      AND `_rowid` > :lastRowId
    ORDER BY `_rowid` ASC
    LIMIT " . FETCH_BATCH_SIZE . "
");

$setClauses = [];
foreach (SOCIAL_COLUMNS as $col) {
    $setClauses[] = "`$col` = ?";
}
$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET " . implode(', ', $setClauses) . ",
        `social_scraped` = 2
    WHERE `_rowid` = ?
");

$processed = 0;
$enriched  = 0;
$errors    = 0;
$lastRowId = 0;
$visitedProfiles = [];

while (true) {
    $fetchBatchStmt->execute([
        ':tw'        => $totalWorkers,
        ':wi'        => $workerIdx,
        ':lastRowId' => $lastRowId,
    ]);
    $rows = $fetchBatchStmt->fetchAll();
    if ($rows === []) break;

    foreach ($rows as $row) {
        $rowid     = (int) $row['_rowid'];
        $lastRowId = $rowid;
        $instructor = $row['instructor'] ?? '';

        $profile1 = trim($row['UdemyProfile1_parcing'] ?? '');
        $extraUrls = [];
        foreach (['UdemyProfile2_parcing', 'UdemyProfile3_parcing'] as $col) {
            $url = trim($row[$col] ?? '');
            if ($url !== '' && !in_array($url, PROFILE_SKIP_MARKERS, true) && $url !== $profile1) {
                $extraUrls[] = $url;
            }
        }
        $extraUrls = array_values(array_unique($extraUrls));

        if (empty($extraUrls)) {
            $params = [];
            foreach (SOCIAL_COLUMNS as $dbCol) {
                $params[] = $row[$dbCol] ?? '';
            }
            $params[] = $rowid;
            $updateStmt->execute($params);
            continue;
        }

        $processed++;
        echo "{$workerLabel}[$processed/$total] rowid=$rowid | $instructor\n";

        $existing = [];
        foreach (SOCIAL_COLUMNS as $key => $dbCol) {
            $existing[$key] = trim($row[$dbCol] ?? '');
        }

        $anyNew = false;

        foreach ($extraUrls as $profileUrl) {
            echo "  Extra profile: $profileUrl\n";

            if (isset($visitedProfiles[$profileUrl])) {
                $social = $visitedProfiles[$profileUrl];
            } else {
                $social = fetchProfile($profileUrl);
                if ($social === null) {
                    $errors++;
                    usleep(DELAY_MS * 1000);
                    continue;
                }
                $visitedProfiles[$profileUrl] = $social;
            }

            foreach (SOCIAL_COLUMNS as $key => $dbCol) {
                $newVal = $social[$key] ?? '';
                if ($newVal === '') continue;
                $merged = mergeField($existing[$key], $newVal);
                if ($merged !== $existing[$key]) {
                    $existing[$key] = $merged;
                    $anyNew = true;
                }
            }

            usleep(DELAY_MS * 1000);
        }

        if ($anyNew) {
            $enriched++;
            $newFields = [];
            foreach (SOCIAL_COLUMNS as $key => $dbCol) {
                if ($existing[$key] !== trim($row[$dbCol] ?? '')) {
                    $newFields[] = "$key={$existing[$key]}";
                }
            }
            echo "  ✓ Enriched: " . implode(', ', $newFields) . "\n";
        } else {
            echo "  No new social links\n";
        }

        $params = [];
        foreach (SOCIAL_COLUMNS as $key => $dbCol) {
            $params[] = $existing[$key];
        }
        $params[] = $rowid;
        $updateStmt->execute($params);
    }
}

echo "\n--- Pass 2 Done ---\n";
echo "{$workerLabel}Total rows processed : $processed\n";
echo "{$workerLabel}Rows enriched        : $enriched\n";
echo "{$workerLabel}Errors               : $errors\n";
