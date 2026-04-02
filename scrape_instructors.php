<?php

/**
 * Udemy Instructor Profile Scraper
 *
 * Reads course_url from MySQL leads table,
 * fetches each course page, extracts instructor profile links,
 * and saves results to UdemyProfile1_parcing / UdemyProfile2_parcing / UdemyProfile3_parcing columns.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_USER', 'root');
define('DB_PASS', '');

define('DELAY_MS', 5000);
define('REQUEST_TIMEOUT', 30);
define('UDEMY_BASE', 'https://www.udemy.com');

define('CURL_IMPERSONATE_BIN', '/tmp/curl-impersonate');


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
 * Extract course slug from URL.
 * "https://www.udemy.com/course/my-course/" → "my-course"
 */
function extractSlug(string $courseUrl): string
{
    if (preg_match('#/course/([^/?]+)#', $courseUrl, $m)) {
        return $m[1];
    }
    throw new RuntimeException("BAD_URL: cannot extract slug from $courseUrl");
}

/**
 * Execute curl-impersonate command, return [httpCode, body].
 * Throws RuntimeException on curl errors.
 */
function curlFetch(string $url): array
{
    $cmd = sprintf(
        '%s -s -L --max-time %d --compressed '
        . '-o - -w "\\n%%{http_code}" '
        . '%s 2>/dev/null',
        escapeshellarg(CURL_IMPERSONATE_BIN),
        REQUEST_TIMEOUT,
        escapeshellarg($url)
    );

    $outputLines = [];
    $exitCode = 0;
    exec($cmd, $outputLines, $exitCode);
    $raw = implode("\n", $outputLines);

    if ($exitCode !== 0) {
        if ($exitCode === 28) {
            throw new RuntimeException("CURL_TIMEOUT for $url: timed out after " . REQUEST_TIMEOUT . "s");
        }
        if (in_array($exitCode, [5, 6, 7, 52, 56, 97], true)) {
            throw new RuntimeException("CURL_CONNECT for $url: exit code $exitCode");
        }
        throw new RuntimeException("CURL_ERROR for $url: exit code $exitCode");
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
 * Parse instructor profile URLs from HTML page.
 * Looks for /user/slug/ patterns in the page source.
 *
 * @return string[]
 */
function extractFromHtml(string $html): array
{
    $links = [];
    if (preg_match_all('#"url"\s*:\s*"(/user/[^"]+)"#', $html, $m)) {
        foreach ($m[1] as $path) {
            $url = UDEMY_BASE . $path;
            if (!in_array($url, $links, true)) {
                $links[] = $url;
            }
            if (count($links) >= 3) break;
        }
    }
    return $links;
}

/**
 * Fetch instructor profiles:
 * 1. Try Udemy API (~150 bytes, fast)
 * 2. If API returns 403/404 — fallback to HTML page parsing
 *
 * @return string[]  e.g. ["https://www.udemy.com/user/365careers/"]
 */
function fetchInstructors(string $slug): array
{
    // — Step 1: API —
    $apiUrl = UDEMY_BASE . '/api-2.0/courses/' . $slug
        . '/?fields%5Bcourse%5D=visible_instructors&fields%5Buser%5D=url,display_name';

    [$httpCode, $body] = curlFetch($apiUrl);

    if ($httpCode === 200) {
        $data = json_decode($body, true);
        if (is_array($data) && !empty($data['visible_instructors'])) {
            $links = [];
            foreach ($data['visible_instructors'] as $instructor) {
                $url = trim($instructor['url'] ?? '');
                if ($url === '') continue;
                if (str_starts_with($url, '/')) {
                    $url = UDEMY_BASE . $url;
                }
                if (!in_array($url, $links, true)) {
                    $links[] = $url;
                }
                if (count($links) >= 3) break;
            }
            return $links;
        }
        return [];
    }

    // — Step 2: fallback to HTML page —
    if (in_array($httpCode, [403, 404], true)) {
        echo "  API returned $httpCode, trying HTML fallback...\n";

        $pageUrl = UDEMY_BASE . '/course/' . $slug . '/';
        [$pageCode, $pageBody] = curlFetch($pageUrl);

        if ($pageCode === 200) {
            $links = extractFromHtml($pageBody);
            if (!empty($links)) {
                return $links;
            }
            return [];
        }

        throw new RuntimeException("HTTP $pageCode for $slug (API: $httpCode, HTML: $pageCode)");
    }

    throw new RuntimeException("HTTP $httpCode for $slug");
}

// ── main ──────────────────────────────────────────────────────────────────────

// ── Worker / parallel support ─────────────────────────────────────────────────
// Usage: php scrape_instructors.php [--worker=N] [--total-workers=M]
// Example (5 parallel workers): run_parallel.php will spawn 5 processes

$workerIdx    = 0;
$totalWorkers = 1;

foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/', $arg, $m))         $workerIdx    = (int) $m[1];
    if (preg_match('/^--total-workers=(\d+)$/', $arg, $m))  $totalWorkers = (int) $m[1];
}

$pdo = getDb();

// Rows eligible for processing:
//   - not yet touched:          UdemyProfile1_parcing IS NULL or ''
//   - first HTTP error attempt: UdemyProfile1_parcing = 'RETRY:1'
//   - temporary network issue:  UdemyProfile1_parcing = 'RETRY_NET'
// Rows permanently excluded:
//   - SKIP        — corrupted instructor name
//   - NOT_FOUND   — page loaded but no instructor links
//   - HTTP_ERROR  — got 403/404 twice in a row
$eligibleCondition = "
    `course_url` IS NOT NULL
    AND `course_url` != ''
    AND (
        COALESCE(`UdemyProfile1_parcing`, '') = ''
        OR `UdemyProfile1_parcing` = 'RETRY:1'
        OR `UdemyProfile1_parcing` = 'RETRY_NET'
    )
    AND COALESCE(`UdemyProfile1_parcing`, '') != 'SKIP'
    AND (`_rowid` % :tw) = :wi
";

$workerLabel = $totalWorkers > 1 ? "[Worker $workerIdx/$totalWorkers] " : "";

// Bulk-mark corrupted instructor names before counting (only worker 0 to avoid races)
if ($workerIdx === 0) {
    $bulkSkip = $pdo->exec("
        UPDATE `leads`
        SET `UdemyProfile1_parcing` = 'SKIP'
        WHERE CHAR_LENGTH(`instructor`) > 200
          AND COALESCE(`UdemyProfile1_parcing`, '') != 'SKIP'
    ");
    if ($bulkSkip > 0) {
        echo "{$workerLabel}Bulk-marked $bulkSkip corrupted instructor rows as SKIP\n";
    }
}

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `leads` WHERE $eligibleCondition");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int) $totalStmt->fetchColumn();

echo "{$workerLabel}Udemy API (no proxy), delay " . (DELAY_MS/1000) . "s\n";
echo "{$workerLabel}Total rows to process: $total\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `leads`
    SET `UdemyProfile1_parcing` = ?,
        `UdemyProfile2_parcing` = ?,
        `UdemyProfile3_parcing` = ?
    WHERE `_rowid` = ?
");

// Fetch one row at a time (this worker's slice only) to avoid memory exhaustion
$fetchStmt = $pdo->prepare("
    SELECT `_rowid`, `course_url`, `instructor`, `UdemyProfile1_parcing`
    FROM `leads`
    WHERE $eligibleCondition
    ORDER BY CASE
                 WHEN `UdemyProfile1_parcing` = 'RETRY:1' THEN 1
                 WHEN `UdemyProfile1_parcing` = 'RETRY_NET' THEN 2
                 ELSE 0
             END ASC,
             `_rowid` ASC
    LIMIT 1
");

$processed  = 0;
$found      = 0;
$skipped    = 0;
$errors     = 0;
$deadCourse = false;

while (true) {
    $fetchStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
    $row = $fetchStmt->fetch();
    if ($row === false) {
        break;
    }
    $rowid        = (int) $row['_rowid'];
    $courseUrl    = $row['course_url'];
    $instructor   = $row['instructor'] ?? '';
    $currentMark  = $row['UdemyProfile1_parcing'] ?? '';

    if (mb_strlen($instructor) > 200) {
        $updateStmt->execute(['SKIP', '', '', $rowid]);
        $skipped++;
        continue;
    }

    $processed++;
    echo "{$workerLabel}[$processed/$total] rowid=$rowid | $instructor\n";
    echo "  URL: $courseUrl\n";

    try {
        $slug  = extractSlug($courseUrl);
        $links = fetchInstructors($slug);

        if (empty($links)) {
            echo "  WARNING: No instructor links found\n";
            $updateStmt->execute(['NOT_FOUND', '', '', $rowid]);
        } else {
            $updateStmt->execute([$links[0] ?? '', $links[1] ?? '', $links[2] ?? '', $rowid]);
            $found++;
            echo "  ✓ rowid=$rowid saved " . count($links) . " profile(s): " . implode(' | ', $links) . "\n";
        }

    } catch (RuntimeException $e) {
        $msg = $e->getMessage();
        if (str_starts_with($msg, 'BAD_URL')) {
            echo "  SKIP: $msg\n";
            $updateStmt->execute(['SKIP', '', '', $rowid]);
            $skipped++;
        } elseif (preg_match('/^HTTP (\d+).*API: (403|404), HTML: (403|404)/', $msg)) {
            // Both API and HTML returned 4xx — course is gone, mark final
            echo "  HTTP ERROR (final, both API+HTML failed): $msg\n";
            $updateStmt->execute(['HTTP_ERROR', '', '', $rowid]);
            $skipped++;
            $deadCourse = true;
        } elseif (preg_match('/^HTTP (403|404)/', $msg)) {
            if ($currentMark === 'RETRY:1') {
                echo "  HTTP ERROR (attempt 2/2, final): $msg\n";
                $updateStmt->execute(['HTTP_ERROR', '', '', $rowid]);
                $skipped++;
            } else {
                echo "  HTTP 403/404 (attempt 1/2): $msg\n";
                $updateStmt->execute(['RETRY:1', '', '', $rowid]);
            }
        } elseif (str_starts_with($msg, 'CURL_TIMEOUT') || str_starts_with($msg, 'CURL_CONNECT')) {
            echo "  TIMEOUT/CONNECT: $msg\n";
            $updateStmt->execute(['RETRY_NET', '', '', $rowid]);
        } else {
            echo "  ERROR (will retry): $msg\n";
            $updateStmt->execute(['RETRY_NET', '', '', $rowid]);
        }
        $errors++;
    }

    // No delay if course was dead (HTTP_ERROR) — skip waiting
    if (!$deadCourse) {
        usleep(DELAY_MS * 1000);
    }
    $deadCourse = false;
}

echo "\n--- Done ---\n";
echo "Total rows         : $total\n";
echo "Processed          : $processed\n";
echo "With profiles found: $found\n";
echo "Skipped (corrupted): $skipped\n";
echo "Errors (will retry): $errors\n";
