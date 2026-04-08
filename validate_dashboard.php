<?php
/**
 * validate_dashboard.php — веб-интерфейс для валидации контактных данных.
 *
 * Статистика, таблица результатов, фильтры, запуск batch/single, CSV-экспорт.
 *
 * Usage:
 *   php -S localhost:8089 validate_dashboard.php
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

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

require_once __DIR__ . '/validate_migrate.php';
validate_ensure_columns(getDb(), DB_TABLE);

function validate_php_cli(): string
{
    $env = getenv('PHP_CLI');
    if ($env !== false && $env !== '' && is_executable($env)) return $env;
    if (defined('PHP_BINDIR') && PHP_BINDIR !== '') {
        $p = PHP_BINDIR . DIRECTORY_SEPARATOR . 'php';
        if (is_executable($p)) return $p;
    }
    foreach (['/usr/bin/php', '/usr/local/bin/php', '/bin/php'] as $p) {
        if (is_executable($p)) return $p;
    }
    return 'php';
}

function validate_log_file(): string
{
    return __DIR__ . DIRECTORY_SEPARATOR . 'validate_leads.log';
}

function validate_log_is_writable(string $path): bool
{
    return is_file($path) ? is_writable($path) : is_writable(dirname($path));
}

// ── Actions (POST) ───────────────────────────────────────────────────────────

$flash = '';

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') === 'POST') {
    $action = $_POST['action'] ?? '';

    if ($action === 'validate_single') {
        $rowid = (int)($_POST['rowid'] ?? 0);
        if ($rowid > 0) {
            $php    = validate_php_cli();
            $script = __DIR__ . '/validate_leads.php';
            $cmd    = escapeshellarg($php) . ' ' . escapeshellarg($script)
                    . ' --rowid=' . $rowid . ' 2>&1';
            $output = '';
            $code   = -1;
            exec($cmd, $lines, $code);
            $output = implode("\n", $lines);
            @file_put_contents(validate_log_file(), date('[Y-m-d H:i:s] ') . "single rowid={$rowid} code={$code}\n{$output}\n\n", FILE_APPEND);

            if ($code === 0) {
                $flash = "Валидация rowid={$rowid} завершена.";
            } else {
                $flash = "Ошибка валидации rowid={$rowid} (код {$code}). См. validate_leads.log.";
            }
        }
    }

    if ($action === 'start_batch') {
        $limit     = max(0, (int)($_POST['limit'] ?? 0));
        $batchFrom = max(0, (int)($_POST['batch_from'] ?? 0));
        $batchTo   = max(0, (int)($_POST['batch_to'] ?? 0));
        $revalidate = !empty($_POST['revalidate']);
        $logFile    = validate_log_file();
        $phpBin     = validate_php_cli();

        if (!validate_log_is_writable($logFile)) {
            $flash = 'Лог недоступен для записи: ' . $logFile;
        } else {
            $marker = "\n" . date('c') . " [dashboard] batch: limit={$limit} from={$batchFrom} to={$batchTo} revalidate="
                . ($revalidate ? '1' : '0') . "\n";
            @file_put_contents($logFile, $marker, FILE_APPEND | LOCK_EX);

            $script = __DIR__ . '/validate_leads.php';
            $args = '';
            if ($limit > 0)     $args .= ' --limit=' . $limit;
            if ($batchFrom > 0) $args .= ' --from=' . $batchFrom;
            if ($batchTo > 0)   $args .= ' --to=' . $batchTo;
            if ($revalidate)    $args .= ' --revalidate';

            $cmd = 'cd ' . escapeshellarg(__DIR__)
                . ' && nohup ' . escapeshellarg($phpBin) . ' ' . escapeshellarg($script) . $args
                . ' >> ' . escapeshellarg($logFile) . ' 2>&1 &';
            exec($cmd);
            $rangeInfo = ($batchFrom > 0 || $batchTo > 0) ? " (rowid {$batchFrom}–{$batchTo})" : '';
            $flash = "Валидация запущена{$rangeInfo}. Лог: " . basename($logFile);
        }
    }

    if ($action === 'run_all') {
        $logFile = validate_log_file();
        $phpBin  = validate_php_cli();

        if (!validate_log_is_writable($logFile)) {
            $flash = 'Лог недоступен для записи: ' . $logFile;
        } else {
            $marker = "\n" . date('c') . " [dashboard] RUN ALL: revalidate from scratch\n";
            @file_put_contents($logFile, $marker, FILE_APPEND | LOCK_EX);

            $script = __DIR__ . '/validate_leads.php';
            $cmd = 'cd ' . escapeshellarg(__DIR__)
                . ' && nohup ' . escapeshellarg($phpBin) . ' ' . escapeshellarg($script) . ' --revalidate'
                . ' >> ' . escapeshellarg($logFile) . ' 2>&1 &';
            exec($cmd);
            $flash = "Запущена полная валидация всех строк с Instagram. Лог: " . basename($logFile);
        }
    }

    if ($action === 'stop_batch') {
        exec("pkill -f 'validate_leads.php' 2>/dev/null");
        $flash = 'Процесс валидации остановлен.';
    }

    if ($action === 'export_csv') {
        exportCsv();
        exit;
    }

    header('Location: ' . strtok($_SERVER['REQUEST_URI'], '?') . '?flash=' . urlencode($flash));
    exit;
}

// ── CSV export ───────────────────────────────────────────────────────────────

function exportCsv(): void
{
    $pdo = getDb();
    $filter = $_POST['export_filter'] ?? 'all';

    $where = "WHERE `validated_at` IS NOT NULL";
    if ($filter === 'valid')      $where .= " AND `validate_verdict` = 'valid'";
    if ($filter === 'suspicious') $where .= " AND `validate_verdict` = 'suspicious'";
    if ($filter === 'mismatch')   $where .= " AND `validate_verdict` = 'mismatch'";

    $rows = $pdo->query("
        SELECT `instructor`, `email_parcing`, `website_parcing`, `instagram_parcing`, `linkedin_parcing`,
               `validate_email_match`, `validate_email_reason`,
               `validate_website_match`, `validate_website_reason`,
               `validate_instagram_match`, `validate_instagram_reason`,
               `validate_linkedin_match`, `validate_linkedin_reason`,
               `validate_confidence`, `validate_verdict`, `validate_summary`, `validated_at`
        FROM `" . DB_TABLE . "` {$where}
        ORDER BY `validate_confidence` DESC
    ")->fetchAll();

    $filename = "validation_{$filter}_" . date('Ymd_His') . ".csv";
    header('Content-Type: text/csv; charset=utf-8');
    header("Content-Disposition: attachment; filename=\"{$filename}\"");
    $fh = fopen('php://output', 'w');
    fprintf($fh, chr(0xEF) . chr(0xBB) . chr(0xBF));
    if ($rows) {
        fputcsv($fh, array_keys($rows[0]));
        foreach ($rows as $r) fputcsv($fh, $r);
    }
    fclose($fh);
}

// ── Data ─────────────────────────────────────────────────────────────────────

$pdo = getDb();
$flash = $_GET['flash'] ?? '';

$page    = max(1, (int)($_GET['page'] ?? 1));
$perPage = 50;
$search  = trim($_GET['search'] ?? '');
$filter  = $_GET['filter'] ?? 'all';
$sort    = $_GET['sort'] ?? 'validate_confidence';
$dir     = strtolower($_GET['dir'] ?? 'desc') === 'asc' ? 'ASC' : 'DESC';

$allowedSorts = ['validate_confidence','validate_verdict','instructor','validated_at',
                 'validate_email_match','validate_website_match','validate_instagram_match','validate_linkedin_match'];
if (!in_array($sort, $allowedSorts)) $sort = 'validate_confidence';

// ── Stats ────────────────────────────────────────────────────────────────────

$stats = $pdo->query("
    SELECT
        COUNT(CASE WHEN `validated_at` IS NOT NULL THEN 1 END) AS validated,
        COUNT(CASE WHEN `validate_verdict` = 'valid' THEN 1 END) AS valid,
        COUNT(CASE WHEN `validate_verdict` = 'suspicious' THEN 1 END) AS suspicious,
        COUNT(CASE WHEN `validate_verdict` = 'mismatch' THEN 1 END) AS mismatch,
        COUNT(CASE WHEN `validate_verdict` = 'insufficient_data' THEN 1 END) AS insufficient,
        AVG(CASE WHEN `validated_at` IS NOT NULL THEN `validate_confidence` END) AS avg_conf,
        COUNT(CASE WHEN `validate_email_match` = 'yes' THEN 1 END) AS email_yes,
        COUNT(CASE WHEN `validate_email_match` = 'likely' THEN 1 END) AS email_likely,
        COUNT(CASE WHEN `validate_email_match` = 'no' THEN 1 END) AS email_no,
        COUNT(CASE WHEN `validate_website_match` = 'yes' THEN 1 END) AS web_yes,
        COUNT(CASE WHEN `validate_website_match` = 'likely' THEN 1 END) AS web_likely,
        COUNT(CASE WHEN `validate_website_match` = 'no' THEN 1 END) AS web_no,
        COUNT(CASE WHEN `validate_instagram_match` = 'yes' THEN 1 END) AS ig_yes,
        COUNT(CASE WHEN `validate_instagram_match` = 'likely' THEN 1 END) AS ig_likely,
        COUNT(CASE WHEN `validate_instagram_match` = 'no' THEN 1 END) AS ig_no,
        COUNT(CASE WHEN `validate_linkedin_match` = 'yes' THEN 1 END) AS li_yes,
        COUNT(CASE WHEN `validate_linkedin_match` = 'likely' THEN 1 END) AS li_likely,
        COUNT(CASE WHEN `validate_linkedin_match` = 'no' THEN 1 END) AS li_no
    FROM `" . DB_TABLE . "`
")->fetch();

$eligible = (int)$pdo->query("
    SELECT COUNT(*) FROM `" . DB_TABLE . "`
    WHERE `instructor` IS NOT NULL AND TRIM(`instructor`) != ''
      AND `instagram_parcing` IS NOT NULL AND TRIM(`instagram_parcing`) != ''
")->fetchColumn();

$progress = $eligible > 0 ? round($stats['validated'] / $eligible * 100, 1) : 0;

// Process status
$isRunning = false;
$output = @shell_exec("ps aux | grep 'validate_leads.php' | grep -v grep 2>/dev/null");
if ($output && trim($output) !== '') $isRunning = true;

// ── Results table ────────────────────────────────────────────────────────────

$where = "`validated_at` IS NOT NULL";
if ($filter === 'valid')        $where .= " AND `validate_verdict` = 'valid'";
elseif ($filter === 'suspicious') $where .= " AND `validate_verdict` = 'suspicious'";
elseif ($filter === 'mismatch')   $where .= " AND `validate_verdict` = 'mismatch'";
elseif ($filter === 'insufficient') $where .= " AND `validate_verdict` = 'insufficient_data'";
elseif ($filter === 'email_yes')  $where .= " AND `validate_email_match` = 'yes'";
elseif ($filter === 'email_no')   $where .= " AND `validate_email_match` = 'no'";
elseif ($filter === 'web_yes')    $where .= " AND `validate_website_match` = 'yes'";
elseif ($filter === 'web_no')     $where .= " AND `validate_website_match` = 'no'";
elseif ($filter === 'ig_yes')     $where .= " AND `validate_instagram_match` = 'yes'";
elseif ($filter === 'ig_no')      $where .= " AND `validate_instagram_match` = 'no'";
elseif ($filter === 'pending')    { $where = "`validated_at` IS NULL AND `instructor` IS NOT NULL AND TRIM(`instructor`) != ''"; }

$params = [];
if ($search !== '') {
    $where .= " AND (`instructor` LIKE :s OR `email_parcing` LIKE :s2 OR `website_parcing` LIKE :s3 OR `validate_summary` LIKE :s4)";
    $params[':s']  = "%{$search}%";
    $params[':s2'] = "%{$search}%";
    $params[':s3'] = "%{$search}%";
    $params[':s4'] = "%{$search}%";
}

$countStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE {$where}");
$countStmt->execute($params);
$totalRows = (int)$countStmt->fetchColumn();
$totalPages = max(1, (int)ceil($totalRows / $perPage));
$page = min($page, $totalPages);
$offset = ($page - 1) * $perPage;

$sortCol = $filter === 'pending' && $sort === 'validate_confidence' ? '_rowid' : $sort;
$dataStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `email_parcing`, `website_parcing`, `instagram_parcing`, `linkedin_parcing`,
           `validate_email_match`, `validate_email_reason`,
           `validate_website_match`, `validate_website_reason`,
           `validate_instagram_match`, `validate_instagram_reason`,
           `validate_linkedin_match`, `validate_linkedin_reason`,
           `validate_confidence`, `validate_verdict`, `validate_summary`, `validated_at`
    FROM `" . DB_TABLE . "` WHERE {$where}
    ORDER BY `{$sortCol}` {$dir}
    LIMIT {$perPage} OFFSET {$offset}
");
$dataStmt->execute($params);
$rows = $dataStmt->fetchAll();

// Log tail
$logFile = validate_log_file();
$logTail = '';
if (file_exists($logFile) && filesize($logFile) > 0) {
    $lines = file($logFile);
    if ($lines !== false) $logTail = implode('', array_slice($lines, -40));
}
if ($logTail === '') $logTail = "Пока нет строк в логе. Запустите валидацию.";

// ── URL helpers ──────────────────────────────────────────────────────────────

function sortUrl(string $col, string $currentSort, string $currentDir): string {
    $newDir = ($col === $currentSort && $currentDir === 'DESC') ? 'asc' : 'desc';
    $p = $_GET; $p['sort'] = $col; $p['dir'] = $newDir; $p['page'] = 1;
    return '?' . http_build_query($p);
}
function sortIcon(string $col, string $currentSort, string $currentDir): string {
    if ($col !== $currentSort) return '';
    return $currentDir === 'DESC' ? ' ▼' : ' ▲';
}
function filterUrl(string $f): string {
    $p = $_GET; $p['filter'] = $f; $p['page'] = 1;
    return '?' . http_build_query($p);
}

$h  = fn($s) => htmlspecialchars((string)$s, ENT_QUOTES, 'UTF-8');
$nf = fn($n) => number_format((int)$n, 0, '.', ' ');

function matchIcon(string $m): string {
    return match ($m) {
        'yes'            => '<span style="color:#4ade80" title="yes">&#x2705;</span>',
        'likely'         => '<span style="color:#fbbf24" title="likely">&#x1F7E1;</span>',
        'no'             => '<span style="color:#f87171" title="no">&#x274C;</span>',
        'not_applicable' => '<span style="color:#475569" title="n/a">&#x2B1C;</span>',
        default          => '<span style="color:#64748b" title="unclear">&#x2753;</span>',
    };
}

function verdictTag(string $v): string {
    $cls = match ($v) {
        'valid'      => 'tag-green',
        'suspicious' => 'tag-yellow',
        'mismatch'   => 'tag-red',
        default      => 'tag-gray',
    };
    return '<span class="tag ' . $cls . '">' . htmlspecialchars($v) . '</span>';
}

function confBar(float $c): string {
    $pct = round($c * 100);
    $color = $c >= 0.7 ? '#4ade80' : ($c >= 0.4 ? '#fbbf24' : '#f87171');
    return '<div style="display:flex;align-items:center;gap:6px">'
        . '<div style="width:50px;height:6px;background:#2d3148;border-radius:3px;overflow:hidden">'
        . '<div style="width:' . $pct . '%;height:100%;background:' . $color . ';border-radius:3px"></div></div>'
        . '<span style="font-size:0.72rem;color:' . $color . ';font-weight:600">' . number_format($c, 2) . '</span></div>';
}
?>
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Validation Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh;padding:24px}
a{color:#60a5fa;text-decoration:none}a:hover{text-decoration:underline}
h1{font-size:1.5rem;font-weight:700;color:#fff}
.subtitle{color:#64748b;font-size:0.82rem;margin-bottom:24px}
.flash{background:#1e3a5f;border:1px solid #3b82f6;border-radius:8px;padding:12px 16px;margin-bottom:20px;color:#93c5fd;font-size:0.85rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:24px}
.card{background:#1e2130;border:1px solid #2d3148;border-radius:10px;padding:16px}
.card-label{font-size:0.7rem;color:#64748b;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:6px}
.card-value{font-size:1.8rem;font-weight:700;color:#fff;line-height:1}
.card-value.green{color:#4ade80}.card-value.blue{color:#60a5fa}.card-value.yellow{color:#fbbf24}.card-value.red{color:#f87171}.card-value.purple{color:#c084fc}.card-value.cyan{color:#22d3ee}
.card-sub{font-size:0.72rem;color:#64748b;margin-top:4px}
.progress-wrap{background:#1e2130;border:1px solid #2d3148;border-radius:10px;padding:16px;margin-bottom:24px}
.progress-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.progress-title{font-size:0.85rem;font-weight:600;color:#cbd5e1}
.progress-pct{font-size:1.3rem;font-weight:700;color:#60a5fa}
.progress-bar-bg{background:#2d3148;border-radius:999px;height:10px;overflow:hidden}
.progress-bar-fill{height:100%;border-radius:999px;background:linear-gradient(90deg,#3b82f6,#8b5cf6);transition:width .5s}
.progress-nums{display:flex;justify-content:space-between;font-size:0.75rem;color:#64748b;margin-top:6px}
.panel{background:#1e2130;border:1px solid #2d3148;border-radius:10px;padding:16px;margin-bottom:24px}
.section-title{font-size:0.82rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:12px}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
@media(max-width:900px){.two-col{grid-template-columns:1fr}}
.four-col{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.four-col{grid-template-columns:1fr 1fr}}
table{width:100%;border-collapse:collapse;font-size:0.78rem}
th{text-align:left;padding:6px 8px;color:#64748b;font-weight:500;border-bottom:1px solid #2d3148;white-space:nowrap;cursor:pointer;user-select:none}
th:hover{color:#94a3b8}
td{padding:7px 8px;border-bottom:1px solid #1a1f2e;vertical-align:top}
tr:last-child td{border-bottom:none}
tr:hover td{background:#252a3a}
.mono{font-family:'Courier New',monospace;font-size:0.76rem}
.tag{display:inline-block;padding:2px 7px;border-radius:4px;font-size:0.68rem;font-weight:600;white-space:nowrap}
.tag-green{background:#14532d;color:#4ade80;border:1px solid #166534}
.tag-red{background:#450a0a;color:#f87171;border:1px solid #7f1d1d}
.tag-yellow{background:#422006;color:#fbbf24;border:1px solid #92400e}
.tag-gray{background:#1e293b;color:#94a3b8;border:1px solid #334155}
.status-badge{display:inline-flex;align-items:center;gap:6px;padding:4px 12px;border-radius:999px;font-size:0.78rem;font-weight:600}
.status-badge.running{background:#14532d;color:#4ade80;border:1px solid #166534}
.status-badge.stopped{background:#1e293b;color:#64748b;border:1px solid #334155}
.dot{width:8px;height:8px;border-radius:50%;background:currentColor}
.dot.pulse{animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.header-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;flex-wrap:wrap;gap:12px}
.toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
.toolbar input[type=text]{background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:6px 12px;color:#e2e8f0;font-size:0.82rem;width:200px}
.toolbar input[type=text]:focus{outline:none;border-color:#3b82f6}
.btn{display:inline-flex;align-items:center;gap:4px;padding:6px 14px;border-radius:6px;font-size:0.78rem;font-weight:600;border:1px solid #2d3148;background:#1e2130;color:#e2e8f0;cursor:pointer;white-space:nowrap}
.btn:hover{background:#252a3a;border-color:#3b82f6}
.btn-primary{background:#1e40af;border-color:#3b82f6;color:#fff}.btn-primary:hover{background:#1d4ed8}
.btn-danger{background:#7f1d1d;border-color:#991b1b;color:#fca5a5}.btn-danger:hover{background:#991b1b}
.btn-green{background:#14532d;border-color:#166534;color:#4ade80}.btn-green:hover{background:#166534}
.filter-pills{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:16px}
.pill{padding:4px 10px;border-radius:999px;font-size:0.72rem;font-weight:600;border:1px solid #2d3148;background:#1e2130;color:#94a3b8;cursor:pointer;text-decoration:none}
.pill:hover{border-color:#3b82f6;color:#e2e8f0;text-decoration:none}
.pill.active{background:#1e40af;border-color:#3b82f6;color:#fff}
.pagination{display:flex;gap:4px;align-items:center;justify-content:center;margin-top:16px}
.pagination a,.pagination span{padding:4px 10px;border-radius:4px;font-size:0.78rem;border:1px solid #2d3148;color:#94a3b8}
.pagination a:hover{background:#252a3a;text-decoration:none}
.pagination .current{background:#1e40af;border-color:#3b82f6;color:#fff}
.log-box{background:#0d1117;border:1px solid #2d3148;border-radius:8px;padding:12px;font-family:'Courier New',monospace;font-size:0.72rem;line-height:1.5;color:#94a3b8;max-height:280px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.form-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.form-row label{font-size:0.78rem;color:#94a3b8;min-width:80px}
.form-row input[type=number]{background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:5px 10px;color:#e2e8f0;font-size:0.82rem;width:80px}
.form-row input[type=text]{background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:5px 10px;color:#e2e8f0;font-size:0.82rem;width:120px}
.form-row input:focus{outline:none;border-color:#3b82f6}
.breakdown{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.breakdown-item{text-align:center;padding:8px 12px;background:#252a3a;border-radius:6px;min-width:60px}
.breakdown-item .b-val{font-size:1.1rem;font-weight:700;color:#fff}
.breakdown-item .b-label{font-size:0.65rem;color:#64748b;margin-top:2px}
.trunc{max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.reason-cell{font-size:0.7rem;color:#94a3b8;white-space:normal;line-height:1.4;min-width:250px}
.match-cell{text-align:center}
</style>
</head>
<body>

<div class="header-row">
    <div>
        <h1>Validation Dashboard</h1>
        <div class="subtitle">Проверка принадлежности контактных данных через Claude Haiku &nbsp;&middot;&nbsp; <?= date('d.m.Y H:i:s') ?> &nbsp;&middot;&nbsp; <?= DB_NAME ?> &rsaquo; <?= DB_TABLE ?></div>
    </div>
    <div style="display:flex;gap:8px;align-items:center">
        <?php if ($isRunning): ?>
            <span class="status-badge running"><span class="dot pulse"></span> Валидация работает</span>
        <?php else: ?>
            <span class="status-badge stopped"><span class="dot"></span> Остановлена</span>
        <?php endif; ?>
        <a href="xpoz_dashboard.php" class="btn" style="text-decoration:none">Xpoz Dashboard</a>
    </div>
</div>

<?php if ($flash): ?>
<div class="flash"><?= $h($flash) ?></div>
<?php endif; ?>

<!-- Progress -->
<div class="progress-wrap">
    <div class="progress-header">
        <span class="progress-title">Прогресс валидации контактных данных</span>
        <span class="progress-pct"><?= $progress ?>%</span>
    </div>
    <div class="progress-bar-bg">
        <div class="progress-bar-fill" style="width:<?= $progress ?>%"></div>
    </div>
    <div class="progress-nums">
        <span>Проверено: <?= $nf($stats['validated']) ?></span>
        <span>Осталось: <?= $nf(max(0, $eligible - $stats['validated'])) ?></span>
        <span>Подходящих: <?= $nf($eligible) ?></span>
    </div>
</div>

<!-- Stats cards -->
<div class="grid">
    <a href="<?= filterUrl('all') ?>" style="text-decoration:none">
    <div class="card">
        <div class="card-label">Проверено</div>
        <div class="card-value blue"><?= $nf($stats['validated']) ?></div>
        <div class="card-sub"><?= $progress ?>% от подходящих</div>
    </div></a>
    <a href="<?= filterUrl('valid') ?>" style="text-decoration:none">
    <div class="card">
        <div class="card-label">Valid</div>
        <div class="card-value green"><?= $nf($stats['valid']) ?></div>
        <div class="card-sub">данные совпадают</div>
    </div></a>
    <a href="<?= filterUrl('suspicious') ?>" style="text-decoration:none">
    <div class="card">
        <div class="card-label">Suspicious</div>
        <div class="card-value yellow"><?= $nf($stats['suspicious']) ?></div>
        <div class="card-sub">частичное совпадение</div>
    </div></a>
    <a href="<?= filterUrl('mismatch') ?>" style="text-decoration:none">
    <div class="card">
        <div class="card-label">Mismatch</div>
        <div class="card-value red"><?= $nf($stats['mismatch']) ?></div>
        <div class="card-sub">данные не совпадают</div>
    </div></a>
    <a href="<?= filterUrl('insufficient') ?>" style="text-decoration:none">
    <div class="card">
        <div class="card-label">Insufficient</div>
        <div class="card-value" style="color:#64748b"><?= $nf($stats['insufficient']) ?></div>
        <div class="card-sub">мало данных</div>
    </div></a>
    <div class="card">
        <div class="card-label">Avg Confidence</div>
        <div class="card-value cyan"><?= $stats['avg_conf'] ? number_format((float)$stats['avg_conf'], 2) : '—' ?></div>
        <div class="card-sub">средняя уверенность</div>
    </div>
</div>

<!-- Match breakdowns per channel -->
<div class="four-col">
    <div class="panel">
        <div class="section-title">Email</div>
        <div class="breakdown">
            <a href="<?= filterUrl('email_yes') ?>" style="text-decoration:none"><div class="breakdown-item"><div class="b-val" style="color:#4ade80"><?= $nf($stats['email_yes']) ?></div><div class="b-label">yes</div></div></a>
            <div class="breakdown-item"><div class="b-val" style="color:#fbbf24"><?= $nf($stats['email_likely']) ?></div><div class="b-label">likely</div></div>
            <a href="<?= filterUrl('email_no') ?>" style="text-decoration:none"><div class="breakdown-item"><div class="b-val" style="color:#f87171"><?= $nf($stats['email_no']) ?></div><div class="b-label">no</div></div></a>
        </div>
    </div>
    <div class="panel">
        <div class="section-title">Website</div>
        <div class="breakdown">
            <a href="<?= filterUrl('web_yes') ?>" style="text-decoration:none"><div class="breakdown-item"><div class="b-val" style="color:#4ade80"><?= $nf($stats['web_yes']) ?></div><div class="b-label">yes</div></div></a>
            <div class="breakdown-item"><div class="b-val" style="color:#fbbf24"><?= $nf($stats['web_likely']) ?></div><div class="b-label">likely</div></div>
            <a href="<?= filterUrl('web_no') ?>" style="text-decoration:none"><div class="breakdown-item"><div class="b-val" style="color:#f87171"><?= $nf($stats['web_no']) ?></div><div class="b-label">no</div></div></a>
        </div>
    </div>
    <div class="panel">
        <div class="section-title">Instagram</div>
        <div class="breakdown">
            <a href="<?= filterUrl('ig_yes') ?>" style="text-decoration:none"><div class="breakdown-item"><div class="b-val" style="color:#4ade80"><?= $nf($stats['ig_yes']) ?></div><div class="b-label">yes</div></div></a>
            <div class="breakdown-item"><div class="b-val" style="color:#fbbf24"><?= $nf($stats['ig_likely']) ?></div><div class="b-label">likely</div></div>
            <a href="<?= filterUrl('ig_no') ?>" style="text-decoration:none"><div class="breakdown-item"><div class="b-val" style="color:#f87171"><?= $nf($stats['ig_no']) ?></div><div class="b-label">no</div></div></a>
        </div>
    </div>
    <div class="panel">
        <div class="section-title">LinkedIn</div>
        <div class="breakdown">
            <div class="breakdown-item"><div class="b-val" style="color:#4ade80"><?= $nf($stats['li_yes']) ?></div><div class="b-label">yes</div></div>
            <div class="breakdown-item"><div class="b-val" style="color:#fbbf24"><?= $nf($stats['li_likely']) ?></div><div class="b-label">likely</div></div>
            <div class="breakdown-item"><div class="b-val" style="color:#f87171"><?= $nf($stats['li_no']) ?></div><div class="b-label">no</div></div>
        </div>
    </div>
</div>

<!-- Controls -->
<div class="two-col">
    <div class="panel">
        <div class="section-title">Валидация одного лида</div>
        <form method="POST">
            <input type="hidden" name="action" value="validate_single">
            <div class="form-row">
                <label>Row ID</label>
                <input type="number" name="rowid" placeholder="12345" required min="1" style="width:100px">
                <button type="submit" class="btn btn-primary">Проверить</button>
            </div>
        </form>

        <div class="section-title" style="margin-top:20px">Запустить все</div>
        <?php if ($isRunning): ?>
            <form method="POST" style="margin-bottom:8px">
                <input type="hidden" name="action" value="stop_batch">
                <button type="submit" class="btn btn-danger">Остановить</button>
            </form>
        <?php else: ?>
            <form method="POST" style="margin-bottom:12px" onsubmit="return confirm('Запустить валидацию всех <?= $nf($eligible) ?> строк с Instagram с нуля?')">
                <input type="hidden" name="action" value="run_all">
                <button type="submit" class="btn btn-primary" style="width:100%;padding:10px 14px;font-size:0.88rem">Запустить все (<?= $nf($eligible) ?> строк)</button>
            </form>

        <div class="section-title" style="margin-top:16px">Batch валидация (выборочно)</div>
            <form method="POST">
                <input type="hidden" name="action" value="start_batch">
                <div class="form-row">
                    <label>Лимит</label>
                    <input type="number" name="limit" value="100" min="0" title="0 = все">
                </div>
                <div class="form-row">
                    <label>С rowid</label>
                    <input type="number" name="batch_from" value="0" min="0" style="width:90px">
                    <label>По rowid</label>
                    <input type="number" name="batch_to" value="0" min="0" style="width:90px">
                </div>
                <div class="form-row">
                    <label><input type="checkbox" name="revalidate" style="margin-right:4px">Перепроверить</label>
                    <button type="submit" class="btn btn-green">Запустить</button>
                </div>
            </form>
        <?php endif; ?>

        <div class="section-title" style="margin-top:20px">Экспорт CSV</div>
        <form method="POST" style="display:flex;gap:6px">
            <input type="hidden" name="action" value="export_csv">
            <select name="export_filter" style="background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:5px 10px;color:#e2e8f0;font-size:0.82rem">
                <option value="all">Все проверенные</option>
                <option value="valid">Только valid</option>
                <option value="suspicious">Только suspicious</option>
                <option value="mismatch">Только mismatch</option>
            </select>
            <button type="submit" class="btn">Скачать CSV</button>
        </form>
    </div>

    <div class="panel">
        <div class="section-title">Лог (последние строки)</div>
        <div class="log-box" id="logbox"><?php
            $logText = $h($logTail);
            $logText = preg_replace('/✅/', '<span style="color:#4ade80">✅</span>', $logText);
            $logText = preg_replace('/❌/', '<span style="color:#f87171">❌</span>', $logText);
            $logText = preg_replace('/🟡/', '<span style="color:#fbbf24">🟡</span>', $logText);
            echo $logText;
        ?></div>
    </div>
</div>

<!-- Results table -->
<div class="panel">
    <div class="section-title">Результаты валидации (<?= $nf($totalRows) ?>)</div>

    <div class="toolbar">
        <form method="GET" style="display:flex;gap:6px;align-items:center">
            <?php foreach ($_GET as $k => $v): if ($k === 'search' || $k === 'page') continue; ?>
            <input type="hidden" name="<?= $h($k) ?>" value="<?= $h($v) ?>">
            <?php endforeach; ?>
            <input type="text" name="search" value="<?= $h($search) ?>" placeholder="Поиск...">
            <button type="submit" class="btn">Найти</button>
            <?php if ($search): ?>
                <a href="<?= filterUrl($filter) ?>" class="btn">Сбросить</a>
            <?php endif; ?>
        </form>
    </div>

    <div class="filter-pills">
        <a href="<?= filterUrl('all') ?>" class="pill <?= $filter==='all'?'active':'' ?>">Все (<?= $nf($stats['validated']) ?>)</a>
        <a href="<?= filterUrl('valid') ?>" class="pill <?= $filter==='valid'?'active':'' ?>">Valid (<?= $nf($stats['valid']) ?>)</a>
        <a href="<?= filterUrl('suspicious') ?>" class="pill <?= $filter==='suspicious'?'active':'' ?>">Suspicious (<?= $nf($stats['suspicious']) ?>)</a>
        <a href="<?= filterUrl('mismatch') ?>" class="pill <?= $filter==='mismatch'?'active':'' ?>">Mismatch (<?= $nf($stats['mismatch']) ?>)</a>
        <a href="<?= filterUrl('insufficient') ?>" class="pill <?= $filter==='insufficient'?'active':'' ?>">Insufficient (<?= $nf($stats['insufficient']) ?>)</a>
        <a href="<?= filterUrl('pending') ?>" class="pill <?= $filter==='pending'?'active':'' ?>">Не проверены</a>
        <span style="color:#2d3148">|</span>
        <a href="<?= filterUrl('email_yes') ?>" class="pill <?= $filter==='email_yes'?'active':'' ?>">Email ✅</a>
        <a href="<?= filterUrl('email_no') ?>" class="pill <?= $filter==='email_no'?'active':'' ?>">Email ❌</a>
        <a href="<?= filterUrl('web_yes') ?>" class="pill <?= $filter==='web_yes'?'active':'' ?>">Web ✅</a>
        <a href="<?= filterUrl('web_no') ?>" class="pill <?= $filter==='web_no'?'active':'' ?>">Web ❌</a>
        <a href="<?= filterUrl('ig_yes') ?>" class="pill <?= $filter==='ig_yes'?'active':'' ?>">IG ✅</a>
        <a href="<?= filterUrl('ig_no') ?>" class="pill <?= $filter==='ig_no'?'active':'' ?>">IG ❌</a>
    </div>

    <div style="overflow-x:auto">
    <table>
        <thead>
        <tr>
            <th>ID</th>
            <th><a href="<?= sortUrl('instructor',$sort,$dir) ?>">Инструктор<?= sortIcon('instructor',$sort,$dir) ?></a></th>
            <th>Email</th>
            <th>Сайт</th>
            <th>Instagram</th>
            <th>LinkedIn</th>
            <th class="match-cell"><a href="<?= sortUrl('validate_email_match',$sort,$dir) ?>">Em<?= sortIcon('validate_email_match',$sort,$dir) ?></a></th>
            <th class="match-cell"><a href="<?= sortUrl('validate_website_match',$sort,$dir) ?>">Web<?= sortIcon('validate_website_match',$sort,$dir) ?></a></th>
            <th class="match-cell"><a href="<?= sortUrl('validate_instagram_match',$sort,$dir) ?>">IG<?= sortIcon('validate_instagram_match',$sort,$dir) ?></a></th>
            <th class="match-cell"><a href="<?= sortUrl('validate_linkedin_match',$sort,$dir) ?>">LI<?= sortIcon('validate_linkedin_match',$sort,$dir) ?></a></th>
            <th><a href="<?= sortUrl('validate_confidence',$sort,$dir) ?>">Conf<?= sortIcon('validate_confidence',$sort,$dir) ?></a></th>
            <th><a href="<?= sortUrl('validate_verdict',$sort,$dir) ?>">Verdict<?= sortIcon('validate_verdict',$sort,$dir) ?></a></th>
            <th>Summary</th>
        </tr>
        </thead>
        <tbody>
        <?php foreach ($rows as $r):
            $emailShort = mb_strimwidth(str_replace(';', ' ', $r['email_parcing'] ?? ''), 0, 24, '…');
            $webShort   = preg_replace('#https?://(www\.)?#', '', $r['website_parcing'] ?? '');
            $webShort   = mb_strimwidth(rtrim($webShort, '/'), 0, 20, '…');
            $igShort    = preg_replace('#https?://(www\.)?instagram\.com/#', '@', $r['instagram_parcing'] ?? '');
            $igShort    = mb_strimwidth(rtrim($igShort, '/'), 0, 18, '…');
            $liShort    = preg_replace('#https?://(www\.)?linkedin\.com/in/#', '', $r['linkedin_parcing'] ?? '');
            $liShort    = mb_strimwidth(rtrim(explode(',', $liShort)[0], '/'), 0, 18, '…');
        ?>
        <tr>
            <td style="color:#475569;font-size:0.72rem"><?= $r['_rowid'] ?></td>
            <td class="trunc" title="<?= $h($r['instructor']) ?>"><?= $h(mb_strimwidth($r['instructor'] ?? '', 0, 22, '…')) ?></td>
            <td class="mono" style="color:#4ade80;font-size:0.72rem" title="<?= $h($r['email_parcing']) ?>"><?= $h($emailShort) ?></td>
            <td style="font-size:0.72rem" title="<?= $h($r['website_parcing']) ?>">
                <?php if ($r['website_parcing']): ?><a href="<?= $h($r['website_parcing']) ?>" target="_blank" style="color:#fbbf24"><?= $h($webShort) ?></a><?php else: ?><span style="color:#334155">&mdash;</span><?php endif; ?>
            </td>
            <td style="font-size:0.72rem" title="<?= $h($r['instagram_parcing']) ?>">
                <?php if ($r['instagram_parcing']): ?><span style="color:#f472b6"><?= $h($igShort) ?></span><?php else: ?><span style="color:#334155">&mdash;</span><?php endif; ?>
            </td>
            <td style="font-size:0.72rem" title="<?= $h($r['linkedin_parcing']) ?>">
                <?php if ($r['linkedin_parcing']): ?><span style="color:#60a5fa"><?= $h($liShort) ?></span><?php else: ?><span style="color:#334155">&mdash;</span><?php endif; ?>
            </td>
            <td class="match-cell" title="<?= $h($r['validate_email_reason']) ?>"><?= matchIcon($r['validate_email_match'] ?? '') ?></td>
            <td class="match-cell" title="<?= $h($r['validate_website_reason']) ?>"><?= matchIcon($r['validate_website_match'] ?? '') ?></td>
            <td class="match-cell" title="<?= $h($r['validate_instagram_reason']) ?>"><?= matchIcon($r['validate_instagram_match'] ?? '') ?></td>
            <td class="match-cell" title="<?= $h($r['validate_linkedin_reason']) ?>"><?= matchIcon($r['validate_linkedin_match'] ?? '') ?></td>
            <td><?= $r['validate_confidence'] !== null ? confBar((float)$r['validate_confidence']) : '<span style="color:#334155">&mdash;</span>' ?></td>
            <td><?= $r['validate_verdict'] ? verdictTag($r['validate_verdict']) : '<span style="color:#334155">&mdash;</span>' ?></td>
            <td class="reason-cell"><?= $h($r['validate_summary'] ?? '') ?></td>
        </tr>
        <?php endforeach; ?>
        <?php if (!$rows): ?>
        <tr><td colspan="13" style="text-align:center;color:#64748b;padding:24px">Нет данных</td></tr>
        <?php endif; ?>
        </tbody>
    </table>
    </div>

    <?php if ($totalPages > 1): ?>
    <div class="pagination">
        <?php if ($page > 1): ?>
            <a href="?<?= http_build_query(array_merge($_GET, ['page' => $page - 1])) ?>">&larr;</a>
        <?php endif; ?>
        <?php
        $range = 3;
        $startP = max(1, $page - $range);
        $endP = min($totalPages, $page + $range);
        if ($startP > 1) echo '<span>...</span>';
        for ($p = $startP; $p <= $endP; $p++):
        ?>
            <?php if ($p === $page): ?>
                <span class="current"><?= $p ?></span>
            <?php else: ?>
                <a href="?<?= http_build_query(array_merge($_GET, ['page' => $p])) ?>"><?= $p ?></a>
            <?php endif; ?>
        <?php endfor; ?>
        <?php if ($endP < $totalPages) echo '<span>...</span>'; ?>
        <?php if ($page < $totalPages): ?>
            <a href="?<?= http_build_query(array_merge($_GET, ['page' => $page + 1])) ?>">&rarr;</a>
        <?php endif; ?>
    </div>
    <?php endif; ?>
</div>

<div style="text-align:right;font-size:0.72rem;color:#334155;margin-top:16px">
    <a href="?" style="color:#3b82f6">Обновить</a> &nbsp;&middot;&nbsp; validate_dashboard.php
</div>

<script>
const lb = document.getElementById('logbox');
if (lb) lb.scrollTop = lb.scrollHeight;
</script>
</body>
</html>
