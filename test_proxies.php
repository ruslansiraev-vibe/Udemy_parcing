<?php
/**
 * Тест пула прокси — проверяет каждый аккаунт запросом к Udemy
 */

define('PROXY_HOST', 'pool.proxy.market:10000');
define('TEST_URL',   'https://www.udemy.com/user/udemy/');  // публичный профиль
define('TIMEOUT',    20);

const PROXY_CREDENTIALS = [
    ['869VZDfif7nQ',  'WKaYj7Jyp6hnOAB4'],
    ['fZxzeOPDE0m8',  'dJylrcnVOtp6C1NH'],
    ['qijZsytiFobc',  'GU6xH7NdorkSB42c'],
    ['k6dDvUzrGdCi',  'LBEQ0OUxkVfWMg83'],
    ['9apEYMJ81mwI',  'aibtPrK7sz2ZdW1J'],
    ['G2h7zOFCJTgB',  'dmaSvinDRe4XyQVG'],
    ['gmSl1JiJ1jUf',  '3H8bumsBYfJgCo2c'],
    ['HKIEiqouH8df',  'cnyeQimM9JzXBvdx'],
    ['ktS5Q7aTyRa2',  '2bNL1snQlZCOFe3T'],
    ['JavkGTg9ArD8',  'mZKluEWJ3zh7oaL6'],
    ['2nz6lenY8EBr',  'XeFRkZxNSI6jC5Pf'],
    ['4GQdbfkf41C2',  'ZwqrJvy83CkuHo94'],
    ['PeY4dvQj4TpH',  'UKxmtF8XOSG2QciZ'],
    ['CtggDMGMPaeJ',  'pCbcARJxwtzvhsNy'],
    ['WHAtrKmJNfzw',  'BM2LHGF98iuD5Z4I'],
    ['wGZ7UcBFOL9T',  'BN8lnS1IE0f3rLG6'],
    ['H1dYccLI4ajg',  'JTBtkQEzZKhmO863'],
];

function testProxy(string $login, string $pass): array
{
    $ch = curl_init(TEST_URL);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS      => 3,
        CURLOPT_TIMEOUT        => TIMEOUT,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_ENCODING       => '',
        CURLOPT_PROXY          => 'http://' . PROXY_HOST,
        CURLOPT_PROXYUSERPWD   => "$login:$pass",
        CURLOPT_USERAGENT      => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        CURLOPT_HTTPHEADER     => [
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language: en-US,en;q=0.9',
            'Accept-Encoding: gzip, deflate, br',
            'Referer: https://www.google.com/',
            'Sec-Fetch-Dest: document',
            'Sec-Fetch-Mode: navigate',
            'Sec-Fetch-Site: cross-site',
        ],
    ]);

    $start    = microtime(true);
    $body     = curl_exec($ch);
    $elapsed  = round((microtime(true) - $start) * 1000);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlErr  = curl_error($ch);
    $curlErrNo = curl_errno($ch);
    curl_close($ch);

    $size = strlen((string)$body);
    $sizeStr = $size >= 1024 ? round($size / 1024, 1) . ' KB' : $size . ' B';

    if ($curlErr) {
        $errLabel = match($curlErrNo) {
            CURLE_OPERATION_TIMEDOUT         => 'TIMEOUT',
            CURLE_COULDNT_CONNECT            => 'CONNECT_FAILED',
            CURLE_COULDNT_RESOLVE_HOST       => 'DNS_FAILED',
            CURLE_COULDNT_RESOLVE_PROXY      => 'PROXY_DNS_FAILED',
            default                          => "CURL_ERR_$curlErrNo",
        };
        return ['status' => 'ERROR', 'detail' => $errLabel, 'ms' => $elapsed, 'size' => '—'];
    }

    $status = match(true) {
        $httpCode === 200 => 'OK',
        $httpCode === 403 => 'BANNED',
        $httpCode === 429 => 'RATE_LIMIT',
        $httpCode === 407 => 'PROXY_AUTH_FAIL',
        $httpCode >= 500  => "SERVER_ERR_$httpCode",
        default           => "HTTP_$httpCode",
    };

    return ['status' => $status, 'detail' => "HTTP $httpCode", 'ms' => $elapsed, 'size' => $sizeStr];
}

// ── Run tests ─────────────────────────────────────────────────────────────────

$total   = count(PROXY_CREDENTIALS);
$ok      = 0;
$banned  = 0;
$errors  = 0;

echo "Проверка " . $total . " прокси → " . TEST_URL . "\n";
echo str_repeat('─', 72) . "\n";
printf("%-4s %-16s %-16s %-12s %6s %s\n", '#', 'Login', 'Status', 'Detail', 'ms', 'Size');
echo str_repeat('─', 72) . "\n";

foreach (PROXY_CREDENTIALS as $i => [$login, $pass]) {
    $n = $i + 1;
    $r = testProxy($login, $pass);

    $statusIcon = match($r['status']) {
        'OK'              => '✅',
        'BANNED'          => '🚫',
        'RATE_LIMIT'      => '⏳',
        'PROXY_AUTH_FAIL' => '🔑',
        default           => '❌',
    };

    printf(
        "%-4s %-16s %-16s %-12s %6s %s\n",
        $n . '.',
        $login,
        $statusIcon . ' ' . $r['status'],
        $r['detail'],
        $r['ms'] . 'ms',
        $r['size']
    );

    if ($r['status'] === 'OK')     $ok++;
    elseif ($r['status'] === 'BANNED') $banned++;
    else $errors++;

    // небольшая пауза между запросами чтобы не триггерить rate limit
    if ($n < $total) usleep(800_000);
}

echo str_repeat('─', 72) . "\n";
echo "Итого: ✅ OK: $ok  🚫 Заблокировано: $banned  ❌ Ошибки: $errors  из $total\n";
