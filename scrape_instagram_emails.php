<?php

/**
 * Instagram Bio Email Scraper
 *
 * Reads instagram_parcing from leads_copy, fetches each profile via
 * Instagram's internal API (i.instagram.com), extracts email addresses
 * from biography text, and saves results to instagram_bio_email column.
 *
 * Also captures business_email/public_email if exposed by the API.
 *
 * Usage:
 *   php scrape_instagram_emails.php
 *   php scrape_instagram_emails.php --worker=0 --total-workers=5
 */

define('DB_HOST',  '127.0.0.1');
define('DB_PORT',  3306);
define('DB_NAME',  'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER',  'root');
define('DB_PASS',  '');

define('DELAY_MS',         1000);
define('REQUEST_TIMEOUT',  15);
define('CONNECT_TIMEOUT',  10);
define('FETCH_BATCH_SIZE', 100);

const IG_API_BASE = 'https://i.instagram.com/api/v1/users/web_profile_info/';
const IG_APP_ID   = '936619743392459';

const IG_USER_AGENTS = [
    'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-S911B; dm1q; exynos2200; en_US; 458229258)',
    'Instagram 275.0.0.27.98 Android (31/12; 480dpi; 1080x2340; Google; Pixel 6; oriole; oriole; en_US; 458229258)',
    'Instagram 275.0.0.27.98 Android (34/14; 440dpi; 1080x2340; samsung; SM-A546B; a54x; exynos1380; en_US; 458229258)',
    'Instagram 275.0.0.27.98 Android (30/11; 420dpi; 1080x2220; Google; Pixel 5; redfin; redfin; en_US; 458229258)',
    'Instagram 275.0.0.27.98 Android (33/13; 480dpi; 1440x3088; samsung; SM-S918B; dm3q; qcom; en_US; 458229258)',
];

const EMAIL_BLACKLIST = [
    'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
    '@example.com', '@test.com', '@sentry.io', '@cloudflare.com',
    '@instagram.com', '@facebook.com', '@meta.com', '@fbcdn.net',
    '@schema.org', '@w3.org', '@wixpress.com', '@wordpress.com',
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

// ── Instagram API ────────────────────────────────────────────────────────────

function fetchInstagramProfile(string $username): array
{
    $ua  = IG_USER_AGENTS[array_rand(IG_USER_AGENTS)];
    $url = IG_API_BASE . '?username=' . urlencode($username);

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_MAXREDIRS      => 3,
        CURLOPT_TIMEOUT        => REQUEST_TIMEOUT,
        CURLOPT_CONNECTTIMEOUT => CONNECT_TIMEOUT,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => 0,
        CURLOPT_ENCODING       => '',
        CURLOPT_USERAGENT      => $ua,
        CURLOPT_HTTPHEADER     => [
            'X-IG-App-ID: ' . IG_APP_ID,
            'Accept: application/json',
            'Accept-Language: en-US,en;q=0.9',
        ],
    ]);

    $body     = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error    = curl_error($ch);
    curl_close($ch);

    if ($error !== '') {
        return ['user' => null, 'code' => 0, 'error' => $error];
    }

    if ($httpCode === 404) {
        return ['user' => null, 'code' => 404, 'error' => 'not found'];
    }

    if ($httpCode !== 200) {
        return ['user' => null, 'code' => $httpCode, 'error' => "HTTP $httpCode"];
    }

    $data = @json_decode((string) $body, true);
    if (!is_array($data)) {
        return ['user' => null, 'code' => $httpCode, 'error' => 'Invalid JSON'];
    }

    $user = $data['data']['user'] ?? $data['user'] ?? null;
    if (!is_array($user)) {
        return ['user' => null, 'code' => $httpCode, 'error' => 'No user in response'];
    }

    return ['user' => $user, 'code' => $httpCode, 'error' => ''];
}

// ── Username extraction ──────────────────────────────────────────────────────

function extractInstagramUsername(string $raw): string
{
    $raw = trim($raw);

    if (preg_match('#instagram\.com/(https?://.*instagram\.com/.+)#i', $raw, $m)) {
        $raw = $m[1];
    }

    $raw = preg_replace('/[?#].*$/', '', $raw);
    $raw = rtrim($raw, '/');

    if (preg_match('#instagram\.com/([a-zA-Z0-9_.]{1,30})$#i', $raw, $m)) {
        return strtolower($m[1]);
    }

    $raw = ltrim($raw, '@');
    if (preg_match('/^[a-zA-Z0-9_.]{1,30}$/', $raw)) {
        return strtolower($raw);
    }

    return '';
}

// ── Email extraction ─────────────────────────────────────────────────────────

function isValidEmail(string $email): bool
{
    if ($email === '' || strlen($email) > 160) return false;
    if (!filter_var($email, FILTER_VALIDATE_EMAIL)) return false;

    foreach (EMAIL_BLACKLIST as $bad) {
        if (str_contains($email, $bad)) return false;
    }

    if (preg_match('/@(.*\.)?(png|jpe?g|gif|webp|svg|css|js|woff2?|ttf|ico)$/i', $email)) return false;

    [$local, $domain] = array_pad(explode('@', $email, 2), 2, '');
    if ($local === '' || $domain === '') return false;
    if (preg_match('/^(example|test|sample|demo|user|username|yourname|admin)$/i', $local)) return false;

    $labels = explode('.', $domain);
    $tld = strtolower((string) end($labels));
    if ($tld === '') return false;
    if (strlen($tld) !== 2 && !in_array($tld, COMMON_TLDS, true)) return false;

    return true;
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

function extractInstagramEmails(array $user): array
{
    $emails = [];

    $fields = [
        $user['biography']      ?? '',
        $user['full_name']      ?? '',
        $user['external_url']   ?? '',
        $user['business_email'] ?? '',
        $user['public_email']   ?? '',
    ];

    // bio_links array (newer API)
    foreach ($user['bio_links'] ?? [] as $link) {
        $fields[] = $link['url']   ?? '';
        $fields[] = $link['title'] ?? '';
    }

    foreach ($fields as $text) {
        if (is_string($text) && $text !== '') {
            $emails = array_merge($emails, extractEmailsFromText($text));
        }
    }

    // business_email / public_email directly (may be valid email strings)
    foreach (['business_email', 'public_email'] as $key) {
        $val = $user[$key] ?? '';
        if (is_string($val) && $val !== '' && isValidEmail(strtolower($val))) {
            $emails[] = strtolower($val);
        }
    }

    return array_values(array_unique($emails));
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
    `instagram_parcing` IS NOT NULL
    AND `instagram_parcing` != ''
    AND `instagram_bio_email` IS NULL
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

echo "{$workerLabel}Instagram Bio Email Scraper (via i.instagram.com API)\n";
echo "{$workerLabel}Delay: " . (DELAY_MS / 1000) . "s\n";
echo "{$workerLabel}Total Instagram profiles to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `instagram_bio_email` = ?
    WHERE `_rowid` = ?
");

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `instagram_parcing`
    FROM `" . DB_TABLE . "`
    WHERE $eligibleCondition
      AND `_rowid` > :lastRowId
    ORDER BY `_rowid` ASC
    LIMIT " . FETCH_BATCH_SIZE . "
");

$processed       = 0;
$foundEmails     = 0;
$noEmail         = 0;
$errors          = 0;
$notFound        = 0;
$consecutiveErrs = 0;
$lastRowId       = 0;

while (true) {
    $fetchBatchStmt->execute([
        ':tw'        => $totalWorkers,
        ':wi'        => $workerIdx,
        ':lastRowId' => $lastRowId,
    ]);
    $rows = $fetchBatchStmt->fetchAll();
    if ($rows === []) break;

    foreach ($rows as $row) {
        $rowid      = (int) $row['_rowid'];
        $lastRowId  = $rowid;
        $instructor = $row['instructor'] ?? '';
        $igRaw      = $row['instagram_parcing'] ?? '';

        // Multiple URLs separated by comma/space — take unique usernames
        $igParts = array_filter(array_map('trim', preg_split('/[,;\s]+/', $igRaw)));
        $usernames = [];
        foreach ($igParts as $part) {
            $name = extractInstagramUsername($part);
            if ($name !== '' && !in_array($name, $usernames, true)) {
                $usernames[] = $name;
            }
        }

        if (empty($usernames)) {
            $updateStmt->execute(['INVALID_URL', $rowid]);
            continue;
        }

        $processed++;
        echo "{$workerLabel}[$processed/$total] rowid=$rowid | $instructor\n";

        $allEmails = [];
        $rowError  = false;

        foreach ($usernames as $username) {
            echo "  @$username → ";

            $result = fetchInstagramProfile($username);

            if ($result['user'] === null) {
                if ($result['code'] === 404) {
                    echo "не найден\n";
                    $notFound++;
                    continue;
                }
                if ($result['code'] === 429 || $result['code'] === 401) {
                    $consecutiveErrs++;
                    echo "rate limit ({$result['code']})\n";
                    if ($consecutiveErrs >= 3) {
                        $pauseSec = min(300, 30 * $consecutiveErrs);
                        echo "  ⚠️ Rate limit, пауза {$pauseSec} сек...\n";
                        sleep($pauseSec);
                    }
                    $rowError = true;
                    $errors++;
                    break;
                }
                echo "ERROR: {$result['error']}\n";
                $errors++;
                $consecutiveErrs++;
                $rowError = true;
                break;
            }

            $consecutiveErrs = 0;
            $user = $result['user'];

            $bio     = $user['biography']    ?? '';
            $extUrl  = $user['external_url'] ?? '';
            $bioLen  = mb_strlen($bio);

            echo "bio={$bioLen}ch";
            if ($extUrl !== '') {
                $dispUrl = parse_url($extUrl, PHP_URL_HOST) ?? $extUrl;
                echo " | url=$dispUrl";
            }
            echo "\n";

            $emails = extractInstagramEmails($user);
            foreach ($emails as $e) {
                if (!in_array($e, $allEmails, true)) {
                    $allEmails[] = $e;
                }
            }

            if ($emails) {
                echo "  ✓ Emails: " . implode(', ', $emails) . "\n";
            }
        }

        if ($rowError) {
            usleep(DELAY_MS * 2000);
            continue;
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
echo "{$workerLabel}Total profiles       : $total\n";
echo "{$workerLabel}Processed            : $processed\n";
echo "{$workerLabel}With emails found    : $foundEmails\n";
echo "{$workerLabel}No email found       : $noEmail\n";
echo "{$workerLabel}Profile not found    : $notFound\n";
echo "{$workerLabel}Errors               : $errors\n";
