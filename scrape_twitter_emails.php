<?php

/**
 * Twitter/X Bio Email Scraper
 *
 * Reads twitter_parcing from leads_copy, fetches each profile via fxtwitter API,
 * extracts email addresses from bio/description text,
 * saves results to twitter_parcing_email column.
 *
 * Uses https://api.fxtwitter.com/{username} — public API, no auth required.
 *
 * Usage:
 *   php scrape_twitter_emails.php
 *   php scrape_twitter_emails.php --worker=0 --total-workers=10
 */

define('DB_HOST',  '127.0.0.1');
define('DB_PORT',  3306);
define('DB_NAME',  'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER',  'root');
define('DB_PASS',  '');

define('DELAY_MS',         300);
define('REQUEST_TIMEOUT',  10);
define('CONNECT_TIMEOUT',  5);
define('FETCH_BATCH_SIZE', 100);
define('MAX_RETRIES',      3);

const BROWSER_PROFILES = [
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',        'ch_ua' => '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"macOS"'],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',               'ch_ua' => '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"Windows"'],
    ['ua' => 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',                         'ch_ua' => '"Google Chrome";v="135", "Chromium";v="135", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"Linux"'],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',         'ch_ua' => '"Google Chrome";v="134", "Chromium";v="134", "Not.A/Brand";v="99"', 'ch_mobile' => '?0', 'platform' => '"macOS"'],
    ['ua' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0',                                           'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
    ['ua' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',                                              'ch_ua' => null, 'ch_mobile' => null, 'platform' => null],
];

const EMAIL_BLACKLIST = [
    'noreply', 'no-reply', 'donotreply', 'mailer-daemon',
    '@example.com', '@test.com', '@sentry.io', '@cloudflare.com',
    '@twitter.com', '@x.com', '@t.co', '@twimg.com',
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

// ── HTTP ─────────────────────────────────────────────────────────────────────

function fetchTwitterProfile(string $username): array
{
    $profile = BROWSER_PROFILES[array_rand(BROWSER_PROFILES)];
    $url = 'https://api.fxtwitter.com/' . urlencode($username);

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
        CURLOPT_USERAGENT      => $profile['ua'],
        CURLOPT_HTTPHEADER     => ['Accept: application/json'],
    ]);

    $body     = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error    = curl_error($ch);
    curl_close($ch);

    if ($error !== '') {
        return ['user' => null, 'code' => 0, 'error' => $error];
    }

    if ($httpCode !== 200) {
        return ['user' => null, 'code' => $httpCode, 'error' => "HTTP $httpCode"];
    }

    $data = @json_decode((string) $body, true);
    if (!is_array($data) || !isset($data['user'])) {
        return ['user' => null, 'code' => $httpCode, 'error' => 'Invalid JSON'];
    }

    return ['user' => $data['user'], 'code' => $httpCode, 'error' => ''];
}

// ── Twitter URL → username ───────────────────────────────────────────────────

function extractTwitterUsername(string $raw): string
{
    $raw = trim($raw);

    // Fix double-prefix: https://twitter.com/https://twitter.com/user
    if (preg_match('#https?://(?:www\.)?(?:twitter\.com|x\.com)/(https?://.+)#i', $raw, $m)) {
        $raw = $m[1];
    }

    // Remove query params and hash
    $raw = preg_replace('/[?#].*$/', '', $raw);
    $raw = rtrim($raw, '/');

    // Extract username from URL
    if (preg_match('#(?:twitter\.com|x\.com)/(@?[a-zA-Z0-9_]{1,15})$#i', $raw, $m)) {
        return ltrim($m[1], '@');
    }

    // Bare @username or username
    $raw = ltrim($raw, '@');
    if (preg_match('/^[a-zA-Z0-9_]{1,15}$/', $raw)) {
        return $raw;
    }

    return '';
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
 * Extract emails from Twitter user data.
 * Searches bio/description, name, location, and website URL.
 */
function extractTwitterEmails(array $user): array
{
    $emails = [];

    $fields = [
        $user['description'] ?? '',
        $user['name']        ?? '',
        $user['location']    ?? '',
    ];

    // Website can be a string or an object with 'url'
    $website = $user['website'] ?? null;
    if (is_array($website)) {
        $fields[] = $website['url']         ?? '';
        $fields[] = $website['display_url'] ?? '';
    } elseif (is_string($website)) {
        $fields[] = $website;
    }

    foreach ($fields as $text) {
        if ($text !== '' && $text !== null) {
            $emails = array_merge($emails, extractEmailsFromText((string) $text));
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
    `twitter_parcing` IS NOT NULL
    AND `twitter_parcing` != ''
    AND `twitter_parcing_email` IS NULL
    AND (`_rowid` % :tw) = :wi
";

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

echo "{$workerLabel}Twitter/X Bio Email Scraper (via fxtwitter API)\n";
echo "{$workerLabel}Delay: " . (DELAY_MS / 1000) . "s | Max retries: " . MAX_RETRIES . "\n";
echo "{$workerLabel}Total Twitter accounts to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `twitter_parcing_email` = ?
    WHERE `_rowid` = ?
");

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `twitter_parcing`
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
$suspended       = 0;
$consecutiveErrs = 0;
$lastRowId       = 0;

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
        $twRaw      = $row['twitter_parcing'] ?? '';

        // Multiple URLs separated by comma/space
        $twUrls = array_filter(array_map('trim', preg_split('/[,\s]+/', $twRaw)));

        // Extract unique usernames
        $usernames = [];
        foreach ($twUrls as $u) {
            $name = extractTwitterUsername($u);
            if ($name !== '' && !in_array(strtolower($name), array_map('strtolower', $usernames), true)) {
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

            $result = fetchTwitterProfile($username);

            if ($result['error'] !== '' && $result['user'] === null) {
                if ($result['code'] === 404) {
                    echo "не найден\n";
                    continue;
                }
                if ($result['code'] === 429) {
                    $consecutiveErrs++;
                    echo "rate limit (429)\n";
                    if ($consecutiveErrs >= 5) {
                        echo "  ⚠️ Слишком много 429, пауза 60 сек...\n";
                        sleep(60);
                        $consecutiveErrs = 0;
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

            if ($user === null || ($user['name'] ?? null) === null) {
                echo "аккаунт удалён/заблокирован\n";
                $suspended++;
                continue;
            }

            $bio     = $user['description'] ?? '';
            $website = $user['website'] ?? null;
            $webStr  = '';
            if (is_array($website)) {
                $webStr = $website['display_url'] ?? $website['url'] ?? '';
            } elseif (is_string($website)) {
                $webStr = $website;
            }

            $bioLen = mb_strlen($bio);
            echo "bio={$bioLen}ch";
            if ($webStr !== '') {
                echo " | web=$webStr";
            }
            echo "\n";

            $emails = extractTwitterEmails($user);
            foreach ($emails as $e) {
                if (!in_array($e, $allEmails, true)) {
                    $allEmails[] = $e;
                }
            }

            if ($emails) {
                echo "  Emails: " . implode(', ', $emails) . "\n";
            }
        }

        if ($rowError) {
            // Network/rate-limit error — leave NULL for retry on next run
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
echo "{$workerLabel}Total accounts       : $total\n";
echo "{$workerLabel}Processed            : $processed\n";
echo "{$workerLabel}With emails found    : $foundEmails\n";
echo "{$workerLabel}No email found       : $noEmail\n";
echo "{$workerLabel}Suspended/deleted    : $suspended\n";
echo "{$workerLabel}Errors               : $errors\n";
