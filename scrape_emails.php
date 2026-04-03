<?php

/**
 * Instructor Email Scraper
 *
 * Reads website_parcing from MySQL leads_copy table,
 * visits each instructor's personal website,
 * extracts email addresses (main page + contact sub-pages),
 * and saves results to email_parcing column.
 *
 * Usage:
 *   php scrape_emails.php
 *   php scrape_emails.php --worker=0 --total-workers=4
 */

define('DB_HOST',  '127.0.0.1');
define('DB_PORT',  3306);
define('DB_NAME',  'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER',  'root');
define('DB_PASS',  '');

define('DELAY_MS',       0);      // пауза между строками отключена
define('SUBPAGE_DELAY',  0);
define('MAX_SUBPAGES',   6);
define('MIN_HTML_BYTES', 3000);
define('REQUEST_TIMEOUT', 5);
define('CONNECT_TIMEOUT', 3);
define('FETCH_BATCH_SIZE', 100);
define('MAX_EMAIL_SCAN_BYTES', 400000);
define('MAX_TEXT_SCAN_BYTES', 250000);
define('MAX_ATTR_SCAN_BYTES', 2000);
define('MAX_EMAIL_CANDIDATE_LEN', 160);
define('BAN_THRESHOLD',  5);      // подряд идущих ошибок — предупреждение
define('BAN_PAUSE_SEC',  0);      // пауза отключена

$homeDir = getenv('HOME') ?: '';
define('CURL_IMPERSONATE_BIN', $homeDir !== '' ? $homeDir . '/.local/bin/curl-impersonate/curl-impersonate' : '');
define('BROWSER_FETCH_SCRIPT', __DIR__ . '/browser_fetch.js');
define('PROXY_HOST', 'pool.proxy.market:10000');

const IMPERSONATE_PROFILES = [
    'chrome136',
    'chrome131',
    'chrome124',
    'safari184',
    'safari180',
    'firefox135',
];

// If credentials are configured later, the pool logic will start using them automatically.
const PROXY_CREDENTIALS = [];

const CONTACT_KEYWORDS = [
    'contact', 'about', 'team', 'support', 'reach',
    'hire', 'get-in-touch', 'connect', 'write', 'email',
];

const EMAIL_BLACKLIST = [
    'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
    '@example.com', '@test.com', '@sentry.io', '@cloudflare.com',
    '@wixpress.com', '@sentry.wixpress.com', '@sentry-next.wixpress.com',
    '@ingest.sentry.io', '@payhip.com', '@medium.com', '@wordpress.com',
    '@github.com', '@amazon.com', '@smugmug.com', '@udemy.com',
    '@teachable.com', '@patreon.com', '@buymeacoffee.com', '@mastodon.',
    '@googletagmanager.com', '@schema.org', '@wpcf7.invalid',
    '@w3.org', '@wordpress.org', '@jquery.com', '@bootstrapcdn.com',
    'yourname@', 'name@', 'email@', 'info@info', 'user@',
    'your@email.', 'me@email.', 'you@email.', 'example@domain.',
    'example@mail.', 'test@mail.', 'test@gmail.', 'john.smith@domain.',
    'john@examples.', 'john@mycompany.', 'johndoe@email.', 'your@website.',
    'you@yourcompany.', 'you@company.', 'info@mysite.', 'example@mysite.',
    'blog@wordpress.com', 'website@wordpress.com', 'pressinquiries@medium.com',
    'api-services-support@amazon.com', 'copilot-safety@github.com',
    'contact@payhip.com', 'help@smugmug.com', 'support@helpshift.com',
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

class ProxyPool
{
    private array $active;
    private array $banned = [];
    private int $idx = 0;

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
            if ($this->idx > 0) {
                $this->idx--;
            }
        }
    }

    /** @return array{0:string,1:string} */
    public function next(): array
    {
        if (empty($this->active)) {
            return ['', ''];
        }

        $creds = $this->active[$this->idx % count($this->active)];
        $this->idx++;

        return [$creds[0] . ':' . $creds[1], $creds[0]];
    }

    public function count(): int
    {
        return count($this->active);
    }

    public function isEmpty(): bool
    {
        return empty($this->active);
    }
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

$proxyPool = null;

const BROWSER_PROFILES = [
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',        'ch_ua' => '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"macOS"'],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',               'ch_ua' => '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"Windows"'],
    ['ua' => 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',                         'ch_ua' => '"Google Chrome";v="135", "Chromium";v="135", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"Linux"'],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',         'ch_ua' => '"Google Chrome";v="134", "Chromium";v="134", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"macOS"'],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0', 'ch_ua' => '"Microsoft Edge";v="136", "Chromium";v="136", "Not.A/Brand";v="99"',  'ch_mobile' => '?0', 'platform' => '"Windows"'],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0',                                           'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',                                              'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',         'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
];

const ACCEPT_VARIANTS = [
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
];

const ACCEPT_ENCODING_VARIANTS = [
    'gzip, deflate, br, zstd',
    'gzip, deflate, br',
    'gzip, deflate',
    'br, gzip, deflate',
];

const ACCEPT_LANGUAGE_VARIANTS = [
    'en-US,en;q=0.9',
    'en-GB,en;q=0.9',
    'en-US,en;q=0.8',
    'en-US,en;q=0.5',
    'en-US,en;q=0.9,de;q=0.7',
    'en-GB,en;q=0.8,fr;q=0.6',
];

const CACHE_CONTROL_VARIANTS = ['max-age=0', 'no-cache', 'no-store', 'max-age=0, no-cache'];
const CONNECTION_VARIANTS    = ['keep-alive', 'keep-alive', 'keep-alive', 'close'];

function randomHeaders(): array
{
    $profile        = BROWSER_PROFILES[array_rand(BROWSER_PROFILES)];
    $accept         = ACCEPT_VARIANTS[array_rand(ACCEPT_VARIANTS)];
    $acceptEncoding = ACCEPT_ENCODING_VARIANTS[array_rand(ACCEPT_ENCODING_VARIANTS)];
    $acceptLang     = ACCEPT_LANGUAGE_VARIANTS[array_rand(ACCEPT_LANGUAGE_VARIANTS)];
    $cacheControl   = CACHE_CONTROL_VARIANTS[array_rand(CACHE_CONTROL_VARIANTS)];
    $connection     = CONNECTION_VARIANTS[array_rand(CONNECTION_VARIANTS)];

    $headers = [
        'Accept: '          . $accept,
        'Accept-Language: ' . $acceptLang,
        'Accept-Encoding: ' . $acceptEncoding,
        'Cache-Control: '   . $cacheControl,
        'Connection: '      . $connection,
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

function canUseCurlImpersonate(): bool
{
    return CURL_IMPERSONATE_BIN !== ''
        && is_file(CURL_IMPERSONATE_BIN)
        && is_executable(CURL_IMPERSONATE_BIN);
}

function isSslRelatedCurlError(string $error): bool
{
    $error = strtolower($error);

    return str_contains($error, 'ssl')
        || str_contains($error, 'tls')
        || str_contains($error, 'certificate')
        || str_contains($error, 'cert ')
        || str_contains($error, 'subject name')
        || str_contains($error, 'hostname');
}

function getAlternateSchemeUrl(string $url): string
{
    $parts = parse_url($url);
    if (!is_array($parts)) {
        return '';
    }

    $scheme = strtolower((string) ($parts['scheme'] ?? ''));
    if ($scheme !== 'http' && $scheme !== 'https') {
        return '';
    }

    $host = $parts['host'] ?? '';
    if ($host === '') {
        return '';
    }

    $alternateScheme = $scheme === 'http' ? 'https' : 'http';
    $auth = '';
    if (isset($parts['user'])) {
        $auth = $parts['user'];
        if (isset($parts['pass'])) {
            $auth .= ':' . $parts['pass'];
        }
        $auth .= '@';
    }

    $port = isset($parts['port']) ? ':' . $parts['port'] : '';
    $path = $parts['path'] ?? '';
    $query = isset($parts['query']) ? '?' . $parts['query'] : '';
    $fragment = isset($parts['fragment']) ? '#' . $parts['fragment'] : '';

    return $alternateScheme . '://' . $auth . $host . $port . $path . $query . $fragment;
}

function shouldTryAlternateScheme(string $url, array $result): bool
{
    if (getAlternateSchemeUrl($url) === '') {
        return false;
    }

    if (($result['error'] ?? '') !== '') {
        return true;
    }

    return ((int) ($result['code'] ?? 0)) === 0;
}

function legacyFetchPage(string $url): array
{
    [$userAgent, $headers] = randomHeaders();

    $runRequest = static function (bool $disableVerifyHost) use ($url, $userAgent, $headers): array {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_MAXREDIRS      => 5,
            CURLOPT_TIMEOUT        => REQUEST_TIMEOUT,
            CURLOPT_CONNECTTIMEOUT => CONNECT_TIMEOUT,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => $disableVerifyHost ? 0 : 2,
            CURLOPT_ENCODING       => '',
            CURLOPT_USERAGENT      => $userAgent,
            CURLOPT_HTTPHEADER     => $headers,
        ]);

        $html     = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $finalUrl = curl_getinfo($ch, CURLINFO_EFFECTIVE_URL);
        $error    = curl_error($ch);
        curl_close($ch);

        return [
            'html'    => (string) $html,
            'code'    => (int) $httpCode,
            'url'     => (string) $finalUrl,
            'error'   => (string) $error,
            'insecure'=> $disableVerifyHost,
        ];
    };

    $result = $runRequest(false);

    if ($result['error'] !== '' && isSslRelatedCurlError($result['error'])) {
        $retry = $runRequest(true);
        if ($retry['error'] === '') {
            return [
                'html'   => $retry['html'],
                'code'   => $retry['code'],
                'url'    => $retry['url'] !== '' ? $retry['url'] : $url,
                'error'  => '',
                'proxy'  => '',
                'engine' => 'curl-insecure',
            ];
        }

        $result['error'] .= '; insecure-retry: ' . $retry['error'];
    }

    if ($result['error'] !== '') {
        return [
            'html'   => '',
            'code'   => 0,
            'url'    => $url,
            'error'  => $result['error'],
            'proxy'  => '',
            'engine' => 'curl',
        ];
    }

    return [
        'html'   => $result['html'],
        'code'   => $result['code'],
        'url'    => $result['url'] !== '' ? $result['url'] : $url,
        'error'  => '',
        'proxy'  => '',
        'engine' => 'curl',
    ];
}

/**
 * @return array{0:int,1:string,2:string,3:string}
 */
function curlFetch(string $url): array
{
    global $proxyPool;

    $profile = IMPERSONATE_PROFILES[array_rand(IMPERSONATE_PROFILES)];
    [$proxyUserPass, $proxyLogin] = $proxyPool instanceof ProxyPool ? $proxyPool->next() : ['', ''];

    $cmd = [
        CURL_IMPERSONATE_BIN,
        '--impersonate', $profile,
        '--max-time', (string) REQUEST_TIMEOUT,
        '--connect-timeout', (string) CONNECT_TIMEOUT,
        '-L',
        '--max-redirs', '5',
        '-s',
        '-w', '\n__HTTP_CODE__:%{http_code}\n__EFFECTIVE_URL__:%{url_effective}',
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
        if (in_array($exitCode, [5, 6, 7], true)) {
            throw new ProxyRequestException('CURL_CONNECT', $url, $proxyLogin, $stderr);
        }
        throw new ProxyRequestException('CURL_ERROR', $url, $proxyLogin, "exit=$exitCode $stderr");
    }

    $httpCode = 0;
    $finalUrl = $url;
    $body = $stdout;

    if (preg_match('/\n__EFFECTIVE_URL__:(.+)$/', $stdout, $urlMatch)) {
        $finalUrl = trim($urlMatch[1]);
        $body = preg_replace('/\n__EFFECTIVE_URL__:.+$/', '', $body) ?? $body;
    }
    if (preg_match('/\n__HTTP_CODE__:(\d+)$/', $body, $codeMatch)) {
        $httpCode = (int) $codeMatch[1];
        $body = substr($body, 0, -strlen($codeMatch[0]));
    }

    return [$httpCode, $body, $finalUrl !== '' ? $finalUrl : $url, $proxyLogin];
}

/**
 * @return array{0:int,1:string,2:string}
 */
function browserFetch(string $url): array
{
    global $proxyPool;

    $cmd = ['node', BROWSER_FETCH_SCRIPT, $url];
    [$proxyUserPass, ] = $proxyPool instanceof ProxyPool ? $proxyPool->next() : ['', ''];

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
    if (!is_array($result) || (isset($result['error']) && $result['error'] !== null)) {
        $errMsg = is_array($result) ? ($result['error'] ?? '') : '';
        $errMsg = $errMsg !== '' ? $errMsg : trim($stderr);
        if ($errMsg === '') {
            $errMsg = 'Unknown browser error';
        }
        throw new RuntimeException("BROWSER_ERROR: $errMsg");
    }

    return [
        (int) ($result['httpCode'] ?? 0),
        (string) ($result['body'] ?? ''),
        (string) (($result['finalUrl'] ?? '') ?: $url),
    ];
}

function fetchPageOnce(string $url): array
{
    global $proxyPool;

    if (!canUseCurlImpersonate()) {
        return legacyFetchPage($url);
    }

    try {
        [$httpCode, $html, $finalUrl, $proxyLogin] = curlFetch($url);
        $engine = 'curl-impersonate';

        if ($httpCode === 403 && is_file(BROWSER_FETCH_SCRIPT)) {
            try {
                [$httpCode, $html, $finalUrl] = browserFetch($url);
                $engine = 'browser';
            } catch (RuntimeException $e) {
                return [
                    'html'   => $html,
                    'code'   => $httpCode,
                    'url'    => $finalUrl,
                    'error'  => '',
                    'proxy'  => $proxyLogin,
                    'engine' => $engine,
                ];
            }
        }

        return [
            'html'   => $html,
            'code'   => $httpCode,
            'url'    => $finalUrl,
            'error'  => '',
            'proxy'  => $proxyLogin,
            'engine' => $engine,
        ];
    } catch (ProxyRequestException $e) {
        if ($e->type === 'CURL_CONNECT' && $e->proxyLogin !== '' && $proxyPool instanceof ProxyPool) {
            $proxyPool->ban($e->proxyLogin);
        }

        if (in_array($e->type, ['CURL_TIMEOUT', 'CURL_CONNECT'], true)) {
            return [
                'html'   => '',
                'code'   => 0,
                'url'    => $url,
                'error'  => $e->getMessage(),
                'proxy'  => $e->proxyLogin,
                'engine' => 'curl-impersonate',
            ];
        }

        $fallback = legacyFetchPage($url);
        if ($fallback['error'] !== '') {
            $fallback['error'] = $e->getMessage() . '; fallback: ' . $fallback['error'];
        }
        return $fallback;
    }
}

function fetchPage(string $url): array
{
    $result = fetchPageOnce($url);
    if (!shouldTryAlternateScheme($url, $result)) {
        return $result;
    }

    $alternateUrl = getAlternateSchemeUrl($url);
    if ($alternateUrl === '') {
        return $result;
    }

    $alternateResult = fetchPageOnce($alternateUrl);
    if (($alternateResult['error'] ?? '') === '' && ((int) ($alternateResult['code'] ?? 0)) > 0) {
        return $alternateResult;
    }

    if (($alternateResult['error'] ?? '') !== '' && ($result['error'] ?? '') !== '') {
        $result['error'] .= '; alternate-scheme(' . $alternateUrl . '): ' . $alternateResult['error'];
    }

    return $result;
}

/**
 * Extract social media profile links from any HTML page.
 * Returns only non-empty values. Does NOT overwrite if already set.
 */
function extractSocialFromHtml(string $html): array
{
    $result = [
        'linkedin'  => '',
        'youtube'   => '',
        'facebook'  => '',
        'twitter'   => '',
        'instagram' => '',
        'tiktok'    => '',
    ];

    // Patterns: domain → key, regex to validate path is a profile (not homepage)
    $patterns = [
        'linkedin'  => '#https?://(?:www\.)?linkedin\.com/in/([^"\'\s<>/]+)#i',
        'youtube'   => '#https?://(?:www\.)?youtube\.com/(?:@|channel/|user/)([^"\'\s<>?&]+)#i',
        'facebook'  => '#https?://(?:www\.)?facebook\.com/(?!sharer|share|dialog|pages/category)([^"\'\s<>?&/]{3,})#i',
        'twitter'   => '#https?://(?:www\.)?(?:twitter|x)\.com/(?!share|intent|home|search)([^"\'\s<>?&/]{1,50})#i',
        'instagram' => '#https?://(?:www\.)?instagram\.com/([^"\'\s<>?&/]{2,50})/?(?:["\'\s<>]|$)#i',
        'tiktok'    => '#https?://(?:www\.)?tiktok\.com/@([^"\'\s<>?&/]+)#i',
    ];

    foreach ($patterns as $key => $pattern) {
        if (preg_match($pattern, $html, $m)) {
            // Reconstruct clean URL
            $slug = $m[1];
            $result[$key] = match($key) {
                'linkedin'  => 'https://www.linkedin.com/in/' . $slug,
                'youtube'   => 'https://www.youtube.com/' . (str_starts_with($slug, '@') ? $slug : '@' . $slug),
                'facebook'  => 'https://www.facebook.com/' . $slug,
                'twitter'   => 'https://x.com/' . $slug,
                'instagram' => 'https://www.instagram.com/' . $slug,
                'tiktok'    => 'https://www.tiktok.com/@' . $slug,
            };
        }
    }

    return $result;
}

function looksLikeJsRenderedPage(string $html): bool
{
    $signals = [
        '__NEXT_DATA__',
        '__NUXT__',
        'id="__next"',
        "id='__next'",
        'id="app"',
        "id='app'",
        'data-reactroot',
        'ng-version',
        'webpack',
        'bundle.js',
        'chunks/',
        'javascript is required',
        'enable javascript',
        'please turn on javascript',
    ];

    foreach ($signals as $signal) {
        if (stripos($html, $signal) !== false) {
            return true;
        }
    }

    return false;
}

function normalizeEmailCandidate(string $value): string
{
    if ($value === '' || !str_contains($value, '@')) {
        return '';
    }
    if (strlen($value) > MAX_EMAIL_CANDIDATE_LEN) {
        return '';
    }

    $value = html_entity_decode($value, ENT_QUOTES | ENT_HTML5, 'UTF-8');
    $value = rawurldecode($value);
    $value = html_entity_decode($value, ENT_QUOTES | ENT_HTML5, 'UTF-8');
    $value = preg_replace('/^mailto:/i', '', $value) ?? $value;
    $value = strtok($value, '?') ?: $value;
    $value = preg_replace('/\s+/', ' ', $value) ?? $value;
    $value = preg_replace('/(?:\s+|["\'<>()[\]{};,])+/', '', $value) ?? $value;
    $value = trim($value, " \t\n\r\0\x0B\"'<>(),;:");
    $value = preg_replace('/(?:copy|copied|click|share|social|about|contact|support|team|home|first|last|name|phone|mobile|address|subject|message|submit|thanks|thankyou|reply|instagram|facebook|linkedin|youtube|twitter|tiktok|newsletter|follow|book|read|log|page|company|global|group|join|our|now|text|form|call|send|service|services|training|course|courses|phoneemail|nameemailphoneaddresssubjectmessagesubmitthanks)+$/i', '', $value) ?? $value;
    $value = trim($value, ".-_ \t\n\r\0\x0B\"'<>(),;:");

    return strtolower($value);
}

function deobfuscateEmailText(string $text): string
{
    $markers = ['[at]', '(at)', '{at}', ' at ', '[dot]', '(dot)', '{dot}', ' dot ', '%40', '%2e', '&#64;', '&commat;', '&#46;'];
    $hasMarker = false;
    $textLower = strtolower($text);
    foreach ($markers as $marker) {
        if (str_contains($textLower, $marker)) {
            $hasMarker = true;
            break;
        }
    }
    if (!$hasMarker) {
        return $text;
    }

    $text = html_entity_decode($text, ENT_QUOTES | ENT_HTML5, 'UTF-8');
    $text = rawurldecode($text);

    $patterns = [
        '/\s*(?:\[\s*at\s*\]|\(\s*at\s*\)|\{\s*at\s*\})\s*/i' => '@',
        '/\s+(?:at)\s+/i' => '@',
        '/\s*(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\{\s*dot\s*\})\s*/i' => '.',
        '/\s+(?:dot)\s+/i' => '.',
        '/\s*\(\s*@\s*\)\s*/i' => '@',
        '/\s*\(\s*\.\s*\)\s*/i' => '.',
    ];

    foreach ($patterns as $pattern => $replacement) {
        $text = preg_replace($pattern, $replacement, $text) ?? $text;
    }

    return $text;
}

function decodeCloudflareEmail(string $hex): string
{
    if ($hex === '' || strlen($hex) < 4 || (strlen($hex) % 2) !== 0 || !ctype_xdigit($hex)) {
        return '';
    }

    $key = hexdec(substr($hex, 0, 2));
    $decoded = '';

    for ($i = 2; $i < strlen($hex); $i += 2) {
        $decoded .= chr(hexdec(substr($hex, $i, 2)) ^ $key);
    }

    return $decoded;
}

function collectEmailsFromText(string $text): array
{
    $emails = [];
    preg_match_all('/[a-zA-Z0-9._%+\-]{1,64}@[a-zA-Z0-9.\-]{1,255}\.[a-zA-Z]{2,24}/', $text, $matches);

    foreach ($matches[0] as $email) {
        $email = normalizeEmailCandidate($email);
        if (isValidEmail($email)) {
            $emails[] = $email;
        }
    }

    return $emails;
}

function extractEmails(string $html): array
{
    if (trim($html) === '') {
        return [];
    }

    $emails = [];
    $normalized = [];

    $dom = new DOMDocument();
    @$dom->loadHTML($html, LIBXML_NOERROR | LIBXML_NOWARNING);
    $xpath = new DOMXPath($dom);

    $nodes = $xpath->query('//a[starts-with(translate(@href,"MAILTO","mailto"),"mailto:")]');
    if ($nodes !== false) {
        foreach ($nodes as $node) {
            $href  = $node->getAttribute('href');
            $email = normalizeEmailCandidate($href);
            if (isValidEmail($email)) {
                $emails[] = $email;
            }
        }
    }

    $cfNodes = $xpath->query('//*[@data-cfemail]');
    if ($cfNodes !== false) {
        foreach ($cfNodes as $node) {
            $decoded = normalizeEmailCandidate(decodeCloudflareEmail($node->getAttribute('data-cfemail')));
            if (isValidEmail($decoded)) {
                $emails[] = $decoded;
            }
        }
    }

    if (strlen($html) <= MAX_EMAIL_SCAN_BYTES) {
        foreach (collectEmailsFromText($html) as $email) {
            $emails[] = $email;
        }
    }

    $bodyText = $dom->textContent ?? '';
    if (strlen($bodyText) > MAX_TEXT_SCAN_BYTES) {
        $bodyText = substr($bodyText, 0, MAX_TEXT_SCAN_BYTES);
    }
    foreach ([deobfuscateEmailText($bodyText)] as $variant) {
        foreach (collectEmailsFromText($variant) as $email) {
            $emails[] = $email;
        }
    }

    $attrNodes = $xpath->query('//*[@aria-label or @content or @data-email or @data-mail or @title]');
    if ($attrNodes !== false) {
        foreach ($attrNodes as $node) {
            foreach (['aria-label', 'content', 'data-email', 'data-mail', 'title'] as $attr) {
                $value = $node->getAttribute($attr);
                if ($value === '') {
                    continue;
                }
                if (strlen($value) > MAX_ATTR_SCAN_BYTES) {
                    continue;
                }

                foreach (collectEmailsFromText(deobfuscateEmailText($value)) as $email) {
                    $emails[] = $email;
                }
            }
        }
    }

    foreach ($emails as $email) {
        $email = normalizeEmailCandidate($email);
        if (isValidEmail($email)) {
            $normalized[] = $email;
        }
    }

    return array_values(array_unique($normalized ?? []));
}

function isValidEmail(string $email): bool
{
    if ($email === '' || strlen($email) > MAX_EMAIL_CANDIDATE_LEN) {
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
    if (preg_match('/@(.*\.)?(png|jpe?g|gif|webp|svg|css|js|woff2?|ttf|ico|bmp|avif)$/i', $email)) {
        return false;
    }
    if (preg_match('/\.(png|jpg|gif|css|js|svg|woff|ttf|min)@/i', $email)) {
        return false;
    }
    if (preg_match('/^u00[0-9a-fA-F]{2}/i', $email)) {
        return false;
    }

    [$local, $domain] = array_pad(explode('@', $email, 2), 2, '');
    if ($local === '' || $domain === '') {
        return false;
    }
    if (preg_match('/^(example|test|sample|demo|user|username|yourname|your-email|you|me|john|jane|johndoe|janedoe|admin|contact|mail|info)$/i', $local)) {
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
 * Find contact/about sub-page URLs within the same domain.
 * Priority 1: keyword in href path. Priority 2: keyword in link text.
 */
function findContactPages(string $html, string $baseUrl): array
{
    if (trim($html) === '') {
        return [];
    }

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

        if (
            $href === '' ||
            str_starts_with($href, '#') ||
            str_starts_with($href, 'mailto:') ||
            str_starts_with($href, 'tel:') ||
            str_starts_with($href, 'javascript:') ||
            str_starts_with($href, 'data:')
        ) {
            continue;
        }

        $hrefLower = strtolower($href);
        if (preg_match('/\.(?:png|jpe?g|gif|webp|svg|pdf|zip|rar|mp4|webm|mov|avi)(?:$|\?)/i', $hrefLower)) {
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

$workerIdx    = 0;
$totalWorkers = 1;

foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/', $arg, $m))        $workerIdx    = (int) $m[1];
    if (preg_match('/^--total-workers=(\d+)$/', $arg, $m)) $totalWorkers = (int) $m[1];
}

$workerLabel = $totalWorkers > 1 ? "[W$workerIdx/$totalWorkers] " : "";

$pdo = getDb();
$proxyPool = new ProxyPool();

$eligibleCondition = "
    `website_parcing` IS NOT NULL
    AND `website_parcing` != ''
    AND `website_parcing` NOT LIKE 'ERROR:%'
    AND `website_parcing` NOT REGEXP '^HTTP_[0-9]'
    AND `website_parcing` != 'JS_REQUIRED'
    AND `website_parcing` != 'NOT_FOUND'
    AND `website_parcing` NOT REGEXP 'instagram\.com|twitter\.com|x\.com/|facebook\.com|linkedin\.com|youtube\.com|tiktok\.com|linktr\.ee|t\.me|vk\.com|telegram\.me'
    AND `email_scraped` IS NULL
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

// Пометить соцсети/мессенджеры в website_parcing как SOCIAL_URL — не парсим email с них
if ($workerIdx === 0) {
    $skipped = $pdo->exec("
        UPDATE `" . DB_TABLE . "`
        SET `email_parcing` = 'SOCIAL_URL', `email_scraped` = 1
        WHERE `email_scraped` IS NULL
          AND `website_parcing` REGEXP 'instagram\\.com|twitter\\.com|x\\.com/|facebook\\.com|linkedin\\.com|youtube\\.com|tiktok\\.com|linktr\\.ee|t\\.me|vk\\.com|telegram\\.me'
    ");
    if ($skipped > 0) {
        echo "{$workerLabel}Marked $skipped social/messenger URLs as SOCIAL_URL (skipped)\n";
    }
}

if (canUseCurlImpersonate()) {
    if ($proxyPool->isEmpty()) {
        echo "{$workerLabel}Engine: curl-impersonate (direct) | Delay: " . (DELAY_MS / 1000) . "s | Table: " . DB_TABLE . "\n";
    } else {
        echo "{$workerLabel}Engine: curl-impersonate + proxy pool | Delay: " . (DELAY_MS / 1000) . "s | Table: " . DB_TABLE . "\n";
    }
    if (is_file(BROWSER_FETCH_SCRIPT)) {
        echo "{$workerLabel}Browser fallback: enabled on HTTP 403\n";
    }
} else {
    echo "{$workerLabel}Engine: native curl fallback | Delay: " . (DELAY_MS / 1000) . "s | Table: " . DB_TABLE . "\n";
}
echo "{$workerLabel}Total rows to process: $total\n\n";

// Финальное обновление — email + соцсети (добавляет через запятую если уже есть) + email_scraped = 1
$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `email_parcing`   = ?,
        `email_scraped`   = 1,
        `linkedin_parcing`  = CASE WHEN (? = '')                                    THEN `linkedin_parcing`
                                   WHEN (`linkedin_parcing`  IS NULL OR `linkedin_parcing`  = '') THEN ?
                                   WHEN FIND_IN_SET(?, `linkedin_parcing`)  > 0     THEN `linkedin_parcing`
                                   ELSE CONCAT(`linkedin_parcing`,  ', ', ?) END,
        `youtube_parcing`   = CASE WHEN (? = '')                                    THEN `youtube_parcing`
                                   WHEN (`youtube_parcing`   IS NULL OR `youtube_parcing`   = '') THEN ?
                                   WHEN FIND_IN_SET(?, `youtube_parcing`)   > 0     THEN `youtube_parcing`
                                   ELSE CONCAT(`youtube_parcing`,   ', ', ?) END,
        `facebook_parcing`  = CASE WHEN (? = '')                                    THEN `facebook_parcing`
                                   WHEN (`facebook_parcing`  IS NULL OR `facebook_parcing`  = '') THEN ?
                                   WHEN FIND_IN_SET(?, `facebook_parcing`)  > 0     THEN `facebook_parcing`
                                   ELSE CONCAT(`facebook_parcing`,  ', ', ?) END,
        `twitter_parcing`   = CASE WHEN (? = '')                                    THEN `twitter_parcing`
                                   WHEN (`twitter_parcing`   IS NULL OR `twitter_parcing`   = '') THEN ?
                                   WHEN FIND_IN_SET(?, `twitter_parcing`)   > 0     THEN `twitter_parcing`
                                   ELSE CONCAT(`twitter_parcing`,   ', ', ?) END,
        `instagram_parcing` = CASE WHEN (? = '')                                    THEN `instagram_parcing`
                                   WHEN (`instagram_parcing` IS NULL OR `instagram_parcing` = '') THEN ?
                                   WHEN FIND_IN_SET(?, `instagram_parcing`) > 0     THEN `instagram_parcing`
                                   ELSE CONCAT(`instagram_parcing`, ', ', ?) END,
        `tiktok_parcing`    = CASE WHEN (? = '')                                    THEN `tiktok_parcing`
                                   WHEN (`tiktok_parcing`    IS NULL OR `tiktok_parcing`    = '') THEN ?
                                   WHEN FIND_IN_SET(?, `tiktok_parcing`)    > 0     THEN `tiktok_parcing`
                                   ELSE CONCAT(`tiktok_parcing`,    ', ', ?) END
    WHERE `_rowid` = ?
");

// Промежуточное обновление — только email_parcing, email_scraped остаётся NULL (для повтора)
$updateRetryStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `email_parcing` = ?
    WHERE `_rowid` = ?
");

// Финальный статус без соцсетей — для HTTP ошибок, JS_REQUIRED (email_scraped = 1)
$updateStatusStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `email_parcing` = ?,
        `email_scraped` = 1
    WHERE `_rowid` = ?
");

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `website_parcing`, `email_parcing`
    FROM `" . DB_TABLE . "`
    WHERE $eligibleCondition
      AND `_rowid` > :lastRowId
    ORDER BY `_rowid` ASC
    LIMIT " . FETCH_BATCH_SIZE . "
");

$processed         = 0;
$foundEmails       = 0;
$noEmail           = 0;
$jsRequired        = 0;
$errors            = 0;
$consecutiveErrors = 0;
$errorStats        = [];
$visitedPages      = [];
$lastRowId         = 0;

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
        $rowid       = (int) $row['_rowid'];
        $lastRowId   = $rowid;
        $instructor  = $row['instructor'] ?? '';
        $website     = $row['website_parcing'];
        $currentMark = $row['email_parcing'] ?? '';

        $processed++;
        echo "{$workerLabel}[$processed/$total] rowid=$rowid | $instructor\n";
        echo "  Site: $website\n";

        $allEmails   = [];
        $socialFound = ['linkedin' => '', 'youtube' => '', 'facebook' => '', 'twitter' => '', 'instagram' => '', 'tiktok' => ''];

        // ── Step 1: fetch main page ───────────────────────────────────────────────
        if (isset($visitedPages[$website])) {
            $result = $visitedPages[$website];
        } else {
            $result = fetchPage($website);
            if ($result['error'] === '' && $result['code'] === 200) {
                $visitedPages[$website] = $result;
            }
        }

        $sizeBytes = strlen($result['html']);
        $sizeStr   = $sizeBytes >= 1024 ? round($sizeBytes / 1024, 1) . ' KB' : $sizeBytes . ' B';

        if ($result['error'] !== '') {
            $consecutiveErrors++;
            $errorStats['NETWORK'] = ($errorStats['NETWORK'] ?? 0) + 1;
            echo "  HTTP 0 | 0 B | ERROR: {$result['error']} (подряд: $consecutiveErrors)\n";
            if ($consecutiveErrors >= BAN_THRESHOLD) {
                echo "\n  ⚠️  ВНИМАНИЕ: $consecutiveErrors ошибок подряд!\n";
                echo "  Статистика: " . json_encode($errorStats) . "\n";
                echo "\n";
                $consecutiveErrors = 0;
            }
            $errors++;
            usleep(DELAY_MS * 1000);
            continue;
        }

        $code = $result['code'];
        $engine = $result['engine'] ?? 'curl';
        $proxyLabel = ($result['proxy'] ?? '') !== '' ? " | proxy: {$result['proxy']}" : '';
        echo "  HTTP $code | $sizeStr | engine: $engine$proxyLabel\n";

        if ($code !== 200) {
            $consecutiveErrors++;
            $errorStats["HTTP_$code"] = ($errorStats["HTTP_$code"] ?? 0) + 1;

            if (in_array($code, [403, 429], true) || $code >= 500) {
                echo "  → временная блокировка/ошибка сервера, оставляем на повтор\n";
            } elseif (in_array($code, [404, 410], true)) {
                if ($currentMark === 'RETRY:1') {
                    // вторая попытка — помечаем как обработанную (больше не повторять)
                    echo "  → попытка 2/2 — сохраняем HTTP_$code\n";
                    $updateStatusStmt->execute(['HTTP_' . $code, $rowid]);
                } else {
                    // первая попытка — оставляем email_scraped = NULL для повтора
                    echo "  → попытка 1/2 — повтор при следующем запуске\n";
                    $updateRetryStmt->execute(['RETRY:1', $rowid]);
                }
            } else {
                // прочие коды (5xx и т.д.) — помечаем как обработанную
                $updateStatusStmt->execute(['HTTP_' . $code, $rowid]);
            }

            if ($consecutiveErrors >= BAN_THRESHOLD) {
                echo "\n  ⚠️  ВНИМАНИЕ: $consecutiveErrors ошибок подряд!\n";
                echo "  Статистика: " . json_encode($errorStats) . "\n";
                echo "\n";
                $consecutiveErrors = 0;
            }

            $errors++;
            usleep(DELAY_MS * 1000);
            continue;
        }

        $consecutiveErrors = 0;

        // Extract emails + social links from main page
        $mainEmails = extractEmails($result['html']);
        foreach ($mainEmails as $e) {
            $allEmails[] = $e;
        }
        if ($mainEmails) {
            echo "  Emails на главной: " . implode(', ', $mainEmails) . "\n";
        }

        $pageSocial = extractSocialFromHtml($result['html']);
        foreach ($pageSocial as $k => $v) {
            if ($v !== '' && $socialFound[$k] === '') {
                $socialFound[$k] = $v;
            }
        }

        // ── Step 2: contact sub-pages ─────────────────────────────────────────────
        $subPages = findContactPages($result['html'], $result['url']);
        if ($subPages) {
            echo "  Contact pages: " . implode(', ', $subPages) . "\n";
        }

        if ($sizeBytes < MIN_HTML_BYTES && empty($allEmails) && empty($subPages) && looksLikeJsRenderedPage($result['html'])) {
            echo "  → короткий HTML без email/contact links, похоже на JS-рендеринг\n";
            $updateStatusStmt->execute(['JS_REQUIRED', $rowid]);
            $jsRequired++;
            usleep(DELAY_MS * 1000);
            continue;
        }

        foreach ($subPages as $subUrl) {
            echo "  Checking: $subUrl\n";
            if (SUBPAGE_DELAY > 0) {
                sleep(SUBPAGE_DELAY);
            }

            if (isset($visitedPages[$subUrl])) {
                $sub = $visitedPages[$subUrl];
            } else {
                $sub = fetchPage($subUrl);
                if ($sub['error'] === '' && $sub['code'] === 200) {
                    $visitedPages[$subUrl] = $sub;
                }
            }
            $subSize = strlen($sub['html']);
            $subSizeStr = $subSize >= 1024 ? round($subSize / 1024, 1) . ' KB' : $subSize . ' B';
            $subEngine = $sub['engine'] ?? 'curl';
            echo "    HTTP {$sub['code']} | $subSizeStr | engine: $subEngine\n";

            if ($sub['error'] !== '') {
                echo "    → network error: {$sub['error']}\n";
                continue;
            }

            if ($sub['code'] !== 200) {
                echo "    → пропускаем\n";
                continue;
            }

            $subEmails = extractEmails($sub['html']);
            foreach ($subEmails as $e) {
                if (!in_array($e, $allEmails, true)) {
                    $allEmails[] = $e;
                    $pageName    = parse_url($subUrl, PHP_URL_PATH) ?: '/';
                    echo "    Email: $e (на $pageName)\n";
                }
            }

            $subSocial = extractSocialFromHtml($sub['html']);
            foreach ($subSocial as $k => $v) {
                if ($v !== '' && $socialFound[$k] === '') {
                    $socialFound[$k] = $v;
                }
            }
        }

        // Вывод найденных соцсетей
        $newSocial = array_filter($socialFound, fn($v) => $v !== '');
        if ($newSocial) {
            echo "  Соцсети с сайта: " . implode(', ', array_map(fn($k, $v) => "$k=$v", array_keys($newSocial), $newSocial)) . "\n";
        }

        // Каждая соцсеть передаётся 4 раза для веток CASE:
        // 1) проверка пустоты нового значения, 2) запись если поле пустое,
        // 3) проверка дубликата через FIND_IN_SET, 4) значение для CONCAT
        $socialParams = [];
        foreach (['linkedin', 'youtube', 'facebook', 'twitter', 'instagram', 'tiktok'] as $k) {
            $v = $socialFound[$k];
            $socialParams[] = $v; // ветка 1: WHEN (? = '')
            $socialParams[] = $v; // ветка 2: THEN ? (поле было пустым)
            $socialParams[] = $v; // ветка 3: FIND_IN_SET(?, ...)
            $socialParams[] = $v; // ветка 4: CONCAT(..., ', ', ?)
        }

        if (empty($allEmails)) {
            echo "  Emails не найдены\n";
            $updateStmt->execute(array_merge(['NOT_FOUND'], $socialParams, [$rowid]));
            $noEmail++;
        } else {
            $emailStr = implode(';', $allEmails);
            echo "  ✓ Сохраняем email: $emailStr\n";
            $updateStmt->execute(array_merge([$emailStr], $socialParams, [$rowid]));
            $foundEmails++;
        }

        usleep(DELAY_MS * 1000);
    }
}

echo "\n--- Done ---\n";
echo "{$workerLabel}Total rows           : $total\n";
echo "{$workerLabel}Processed            : $processed\n";
echo "{$workerLabel}With emails found    : $foundEmails\n";
echo "{$workerLabel}No email found       : $noEmail\n";
echo "{$workerLabel}JS-rendered          : $jsRequired\n";
echo "{$workerLabel}Errors total         : $errors\n";
if (!empty($errorStats)) {
    echo "{$workerLabel}Error breakdown:\n";
    arsort($errorStats);
    foreach ($errorStats as $type => $count) {
        echo "{$workerLabel}  $type: $count\n";
    }
}
