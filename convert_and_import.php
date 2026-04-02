<?php
/**
 * convert_and_import.php
 *
 * Reads the PostgreSQL dump (udemy_leads_full_dump.sql),
 * creates the MySQL table, and imports all rows via PDO batch inserts.
 *
 * Usage: php convert_and_import.php [--fresh]
 *   --fresh  Drop and recreate the table before importing
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_USER', 'root');
define('DB_PASS', '');
define('DUMP_FILE', __DIR__ . '/udemy_leads_full_dump.sql');
define('BATCH_SIZE', 500);

$fresh = in_array('--fresh', $argv ?? []);

// ─── Connect ────────────────────────────────────────────────────────────────
try {
    $dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
    $pdo = new PDO($dsn, DB_USER, DB_PASS, [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8mb4",
    ]);
    echo "[OK] Connected to MySQL\n";
} catch (PDOException $e) {
    die("[ERROR] DB connection failed: " . $e->getMessage() . "\n");
}

// ─── Create / recreate table ─────────────────────────────────────────────────
if ($fresh) {
    $pdo->exec("DROP TABLE IF EXISTS `outreach_events`");
    $pdo->exec("DROP TABLE IF EXISTS `leads`");
    echo "[OK] Dropped existing tables (--fresh)\n";
}

$pdo->exec("
CREATE TABLE IF NOT EXISTS `leads` (
    `_rowid`               BIGINT NOT NULL AUTO_INCREMENT,
    `course_id`            TEXT,
    `course_title`         TEXT,
    `course_description`   TEXT,
    `price_current`        TEXT,
    `price_original`       TEXT,
    `course_url`           TEXT,
    `instructor`           TEXT,
    `raw_instructor`       TEXT,
    `scraped_at`           TEXT,
    `search_url`           TEXT,
    `course_img`           TEXT,
    `ribbon`               TEXT,
    `rating`               TEXT,
    `rating_count`         TEXT,
    `total_hours`          TEXT,
    `lectures_count`       TEXT,
    `level`                TEXT,
    `UdemyProfile1`        TEXT,
    `UdemyProfile2`        TEXT,
    `UdemyProfile3`        TEXT,
    `profile_links_status` TEXT,
    `GoogleLink1`          TEXT,
    `GoogleLink2`          TEXT,
    `GoogleLink3`          TEXT,
    `GoogleLink4`          TEXT,
    `GoogleLink5`          TEXT,
    `GoogleLink6`          TEXT,
    `GoogleLink7`          TEXT,
    `GoogleLink8`          TEXT,
    `GoogleLink9`          TEXT,
    `GoogleLink10`         TEXT,
    `GoogleLink11`         TEXT,
    `GoogleLink12`         TEXT,
    `GoogleLink13`         TEXT,
    `GoogleLink14`         TEXT,
    `GoogleLink15`         TEXT,
    `email`                TEXT,
    `email2`               TEXT,
    `email3`               TEXT,
    `facebook`             TEXT,
    `instagram`            TEXT,
    `linkedin`             TEXT,
    `phone`                TEXT,
    `pinterest`            TEXT,
    `tiktok`               TEXT,
    `twitter`              TEXT,
    `website`              TEXT,
    `youtube`              TEXT,
    `pipeline_stage`       TEXT,
    `apify_run_id_udemy`   TEXT,
    `apify_run_id_google`  TEXT,
    `apify_run_id_extract` TEXT,
    `email_sent_at`        TEXT,
    `email_opened`         TEXT,
    `email_replied`        TEXT,
    `email_bounced`        TEXT,
    `course_url_hash`      VARCHAR(32) AS (MD5(COALESCE(`course_url`, ''))) STORED,
    PRIMARY KEY (`_rowid`),
    UNIQUE KEY `idx_leads_course_url_hash` (`course_url_hash`),
    KEY `idx_leads_pipeline_stage` (`pipeline_stage`(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
");

$pdo->exec("
CREATE TABLE IF NOT EXISTS `outreach_events` (
    `id`          BIGINT NOT NULL AUTO_INCREMENT,
    `lead_id`     BIGINT NOT NULL,
    `campaign_id` TEXT,
    `sent_at`     TEXT,
    `opened`      INT DEFAULT 0,
    `replied`     INT DEFAULT 0,
    `bounced`     INT DEFAULT 0,
    PRIMARY KEY (`id`),
    KEY `idx_outreach_lead` (`lead_id`),
    CONSTRAINT `fk_outreach_lead` FOREIGN KEY (`lead_id`) REFERENCES `leads` (`_rowid`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
");

echo "[OK] Tables ready\n";

// ─── Column list from COPY header ────────────────────────────────────────────
// We parse it from the dump so we always stay in sync.
$columns = [];
$inLeadsCopy = false;
$inOutreachCopy = false;
$leadsColumns = [];
$outreachColumns = [];

// ─── Parse & import ──────────────────────────────────────────────────────────
$fh = fopen(DUMP_FILE, 'r');
if (!$fh) {
    die("[ERROR] Cannot open dump file: " . DUMP_FILE . "\n");
}

$leadsInserted   = 0;
$leadsSkipped    = 0;
$outreachInserted = 0;
$batch           = [];
$currentTable    = null;
$currentColumns  = [];

// Build INSERT template lazily
$leadsStmt    = null;
$outreachStmt = null;

function buildStmt(PDO $pdo, string $table, array $cols): PDOStatement
{
    $colList  = implode(', ', array_map(fn($c) => "`$c`", $cols));
    $phList   = implode(', ', array_fill(0, count($cols), '?'));
    return $pdo->prepare("INSERT IGNORE INTO `$table` ($colList) VALUES ($phList)");
}

function flushBatch(PDOStatement $stmt, array &$batch, string $label): int
{
    if (empty($batch)) {
        return 0;
    }
    $inserted = 0;
    $pdo = $stmt->getIterator(); // not needed, just flush
    foreach ($batch as $row) {
        try {
            $stmt->execute($row);
            $inserted += $stmt->rowCount();
        } catch (PDOException $e) {
            // skip duplicate / bad rows silently
        }
    }
    $batch = [];
    return $inserted;
}

$lineNum = 0;
$startTime = microtime(true);

while (($line = fgets($fh)) !== false) {
    $lineNum++;
    $line = rtrim($line, "\r\n");

    // Detect start of COPY block
    if (preg_match('/^COPY public\.leads\s*\((.+)\)\s+FROM stdin/i', $line, $m)) {
        $currentTable   = 'leads';
        $currentColumns = parseCopyColumns($m[1]);
        $leadsColumns   = $currentColumns;
        $leadsStmt      = buildStmt($pdo, 'leads', $leadsColumns);
        echo "[INFO] Importing leads columns: " . count($leadsColumns) . "\n";
        continue;
    }

    if (preg_match('/^COPY public\.outreach_events\s*\((.+)\)\s+FROM stdin/i', $line, $m)) {
        $currentTable   = 'outreach_events';
        $currentColumns = parseCopyColumns($m[1]);
        $outreachColumns = $currentColumns;
        $outreachStmt   = buildStmt($pdo, 'outreach_events', $outreachColumns);
        echo "[INFO] Importing outreach_events columns: " . count($outreachColumns) . "\n";
        continue;
    }

    // End of COPY block
    if ($line === '\\.') {
        if ($currentTable === 'leads' && !empty($batch)) {
            $leadsInserted += flushBatchDirect($pdo, $leadsStmt, $batch);
            $batch = [];
        } elseif ($currentTable === 'outreach_events' && !empty($batch)) {
            $outreachInserted += flushBatchDirect($pdo, $outreachStmt, $batch);
            $batch = [];
        }
        $currentTable = null;
        continue;
    }

    // Data rows
    if ($currentTable !== null && $line !== '') {
        $values = parseCsvLine($line);

        if ($currentTable === 'leads') {
            // Map values to column names; pad/trim if needed
            $row = mapValues($leadsColumns, $values);
            $batch[] = $row;

            if (count($batch) >= BATCH_SIZE) {
                $leadsInserted += flushBatchDirect($pdo, $leadsStmt, $batch);
                $batch = [];
                $elapsed = round(microtime(true) - $startTime);
                echo "\r[leads] inserted: $leadsInserted  elapsed: {$elapsed}s   ";
            }
        } elseif ($currentTable === 'outreach_events') {
            $row = mapValues($outreachColumns, $values);
            $batch[] = $row;
            if (count($batch) >= BATCH_SIZE) {
                $outreachInserted += flushBatchDirect($pdo, $outreachStmt, $batch);
                $batch = [];
            }
        }
    }
}

fclose($fh);

echo "\n[OK] Import complete\n";
echo "     leads inserted   : $leadsInserted\n";
echo "     outreach inserted: $outreachInserted\n";
echo "     total lines read : $lineNum\n";
echo "     elapsed          : " . round(microtime(true) - $startTime) . "s\n";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function parseCopyColumns(string $raw): array
{
    // "col1", "col2", ... → [col1, col2, ...]
    preg_match_all('/"([^"]+)"/', $raw, $m);
    return $m[1];
}

/**
 * Parse a CSV line respecting quoted fields (PostgreSQL COPY csv format).
 */
function parseCsvLine(string $line): array
{
    // Use PHP's built-in CSV parser via a temp stream
    $stream = fopen('php://memory', 'r+');
    fwrite($stream, $line);
    rewind($stream);
    $row = fgetcsv($stream, 0, ',', '"', '\\');
    fclose($stream);
    return $row === false ? [] : $row;
}

function mapValues(array $columns, array $values): array
{
    $row = [];
    foreach ($columns as $i => $col) {
        $val = $values[$i] ?? null;
        // PostgreSQL empty string "" → store as empty string; \N → NULL
        if ($val === '\\N' || $val === null) {
            $val = null;
        }
        $row[] = $val;
    }
    return $row;
}

function flushBatchDirect(PDO $pdo, PDOStatement $stmt, array $batch): int
{
    $inserted = 0;
    foreach ($batch as $row) {
        try {
            $stmt->execute($row);
            $inserted += $stmt->rowCount();
        } catch (PDOException $e) {
            // silently skip duplicates and bad rows
        }
    }
    return $inserted;
}
