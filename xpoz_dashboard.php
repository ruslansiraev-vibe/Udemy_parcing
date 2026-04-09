<?php
/**
 * xpoz_dashboard.php — веб-интерфейс для xpoz парсера.
 *
 * Статистика, таблица результатов, фильтры, запуск batch/single, CSV-экспорт.
 *
 * Usage:
 *   php -S localhost:8088 xpoz_dashboard.php
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

require_once __DIR__ . '/xpoz_migrate.php';
xpoz_ensure_columns(getDb(), DB_TABLE);

/** Путь к PHP CLI (у FPM часто нет «php» в PATH — иначе анализ из веба молча не запускается). */
function xpoz_dashboard_php_cli(): string
{
    $env = getenv('PHP_CLI');
    if ($env !== false && $env !== '' && is_executable($env)) {
        return $env;
    }
    if (defined('PHP_BINDIR') && PHP_BINDIR !== '') {
        $p = PHP_BINDIR . DIRECTORY_SEPARATOR . 'php';
        if (is_executable($p)) {
            return $p;
        }
    }
    foreach (['/usr/bin/php', '/usr/local/bin/php', '/bin/php'] as $p) {
        if (is_executable($p)) {
            return $p;
        }
    }

    return 'php';
}

function xpoz_dashboard_log_file(): string
{
    return __DIR__ . DIRECTORY_SEPARATOR . 'xpoz_parser.log';
}

/** Можно ли создать/дописать лог (иначе >> из shell тоже молча не сработает). */
function xpoz_dashboard_log_is_writable(string $path): bool
{
    if (is_file($path)) {
        return is_writable($path);
    }

    return is_writable(dirname($path));
}

// ── Actions (POST) ───────────────────────────────────────────────────────────

$flash = '';

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') === 'POST') {
    $action = $_POST['action'] ?? '';

    if ($action === 'analyze_single') {
        $username = trim($_POST['username'] ?? '');
        if ($username !== '') {
            $php    = xpoz_dashboard_php_cli();
            $script = __DIR__ . '/xpoz_parser.php';
            $cmd    = escapeshellarg($php) . ' ' . escapeshellarg($script)
                    . ' --username=' . escapeshellarg($username) . ' 2>&1';
            $disabled = array_filter(
                array_map('trim', explode(',', strtolower((string)ini_get('disable_functions')))),
                static fn ($x) => $x !== ''
            );
            $execOk       = function_exists('exec') && !in_array('exec', $disabled, true);
            $shellExecOk  = function_exists('shell_exec') && !in_array('shell_exec', $disabled, true);

            $output = '';
            $code   = -1;
            if ($execOk) {
                exec($cmd, $lines, $code);
                $output = implode("\n", $lines);
            } elseif ($shellExecOk) {
                $output = (string)shell_exec($cmd);
                $code   = 0;
            }
            @file_put_contents(__DIR__ . '/xpoz_parser.log', date('[Y-m-d H:i:s] ') . "single @{$username} code={$code}\n{$output}\n\n", FILE_APPEND);

            if ($output === '' && $code === -1) {
                $flash = 'Не удалось запустить парсер: exec/shell_exec отключены в php.ini.';
            } elseif ($output === '' && $code !== 0) {
                $flash = "Парсер не вернул вывод (код {$code}). Проверьте путь к PHP CLI (на сервере задайте переменную окружения PHP_CLI, например /usr/bin/php8.4).";
            } elseif (str_contains($output, 'не найден в') || str_contains($output, 'результат не сохран')) {
                $flash = "Аккаунт @{$username} не найден в таблице " . DB_TABLE . " (или не совпал с полем instagram_parcing). Добавьте строку в базу или введите username как в таблице. См. xpoz_parser.log.";
            } elseif ($code !== 0) {
                $flash = "Анализ @{$username} завершился с ошибкой (код {$code}). Полный вывод в xpoz_parser.log.";
            } else {
                $flash = "Анализ @{$username} завершён, результат записан в " . DB_TABLE . ".";
            }
        }
    }

    if ($action === 'start_batch') {
        $workers   = max(1, (int)($_POST['workers'] ?? 5));
        $limit     = max(0, (int)($_POST['limit'] ?? 0));
        $batchFrom = max(0, (int)($_POST['batch_from'] ?? 0));
        $batchTo   = max(0, (int)($_POST['batch_to'] ?? 0));
        $reanalyze = !empty($_POST['reanalyze']);
        $requireEmail = !empty($_POST['require_email']);
        $logFile      = xpoz_dashboard_log_file();
        $phpBin       = xpoz_dashboard_php_cli();

        if (!xpoz_dashboard_log_is_writable($logFile)) {
            $flash = 'Лог недоступен для записи: ' . $logFile
                . ' — воркеры не запущены. На сервере: sudo touch … && sudo chown www-data:www-data … && sudo chmod 664 …'
                . ' (или chown каталога проекта под пользователя веб-сервера).';
        } else {
            $marker = "\n" . date('c') . " [dashboard] batch: workers={$workers} limit={$limit} from={$batchFrom} to={$batchTo} reanalyze="
                . ($reanalyze ? '1' : '0') . ' require_email=' . ($requireEmail ? '1' : '0') . "\n";
            @file_put_contents($logFile, $marker, FILE_APPEND | LOCK_EX);

            $script = __DIR__ . '/xpoz_parser.php';
            $cmds   = [];
            for ($i = 0; $i < $workers; $i++) {
                $args = ' --worker=' . (int)$i . ' --total-workers=' . (int)$workers;
                if ($limit > 0) {
                    $args .= ' --limit=' . (int)$limit;
                }
                if ($batchFrom > 0) {
                    $args .= ' --from=' . (int)$batchFrom;
                }
                if ($batchTo > 0) {
                    $args .= ' --to=' . (int)$batchTo;
                }
                if ($reanalyze) {
                    $args .= ' --reanalyze';
                }
                if ($requireEmail) {
                    $args .= ' --require-email';
                }
                $cmds[] = 'cd ' . escapeshellarg(__DIR__)
                    . ' && nohup ' . escapeshellarg($phpBin) . ' ' . escapeshellarg($script) . $args
                    . ' >> ' . escapeshellarg($logFile) . ' 2>&1 &';
            }
            foreach ($cmds as $c) {
                exec($c);
            }
            $rangeInfo = ($batchFrom > 0 || $batchTo > 0) ? " (#{$batchFrom}–#{$batchTo})" : '';
            $emailNote = $requireEmail ? ' Только Instagram со строкой email.' : '';
            $flash = "Запущено {$workers} воркеров{$rangeInfo}.{$emailNote} Лог: " . basename($logFile);
        }
    }

    if ($action === 'stop_batch') {
        exec("pkill -f 'xpoz_parser.php' 2>/dev/null");
        $flash = 'Все воркеры остановлены.';
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

    $where = "WHERE `xpoz_scraped` = 1";
    if ($filter === 'qualified')    $where .= " AND `xpoz_qualified` = 1";
    if ($filter === 'monetization') $where .= " AND `xpoz_monetization` = 1";

    $rows = $pdo->query("
        SELECT `instructor`, `instagram_parcing`, `email_parcing`,
               `xpoz_follower_count`, `xpoz_reels_performance`, `xpoz_low_performing_reels`,
               `xpoz_post_engagement`, `xpoz_monetization`, `xpoz_engagement_rate`,
               `xpoz_icp`, `xpoz_offer_type`, `xpoz_funnel_type`, `xpoz_business_model`,
               `xpoz_audience_type`, `xpoz_monetization_strength`, `xpoz_monetization_reason`,
               `xpoz_platform_mix`, `xpoz_primary_domain`, `xpoz_language`, `xpoz_geo_hint`,
               `xpoz_qualified`, `xpoz_error`, `xpoz_analyzed_at`, `validate_verdict`
        FROM `" . DB_TABLE . "` {$where}
        ORDER BY `xpoz_qualified` DESC, `xpoz_follower_count` DESC
    ")->fetchAll();

    $filename = "xpoz_results_{$filter}_" . date('Ymd_His') . ".csv";
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

$page     = max(1, (int)($_GET['page'] ?? 1));
$perPage  = 50;
$search   = trim($_GET['search'] ?? '');
$filter   = $_GET['filter'] ?? 'all';
$sort     = $_GET['sort'] ?? 'xpoz_follower_count';
$dir      = strtolower($_GET['dir'] ?? 'desc') === 'asc' ? 'ASC' : 'DESC';

$allowedSorts = ['xpoz_follower_count','xpoz_engagement_rate','xpoz_icp','xpoz_offer_type',
                 'xpoz_monetization_strength','xpoz_qualified','xpoz_analyzed_at','instructor',
                 'validate_verdict'];
if (!in_array($sort, $allowedSorts)) $sort = 'xpoz_follower_count';

// Stats
$stats = $pdo->query("
    SELECT
        COUNT(CASE WHEN `instagram_parcing` IS NOT NULL AND `instagram_parcing` != '' THEN 1 END) AS total_ig,
        COUNT(CASE WHEN `xpoz_scraped` = 1 THEN 1 END) AS scraped,
        COUNT(CASE WHEN `xpoz_qualified` = 1 THEN 1 END) AS qualified,
        COUNT(CASE WHEN `xpoz_monetization` = 1 THEN 1 END) AS monetized,
        COUNT(CASE WHEN `xpoz_reels_performance` = 1 THEN 1 END) AS c1_pass,
        COUNT(CASE WHEN `xpoz_low_performing_reels` = 1 THEN 1 END) AS c2_pass,
        COUNT(CASE WHEN `xpoz_post_engagement` = 1 THEN 1 END) AS c3_pass,
        COUNT(CASE WHEN `xpoz_scraped` = 1 AND `xpoz_error` IS NOT NULL AND TRIM(`xpoz_error`) != '' THEN 1 END) AS errors,
        AVG(CASE WHEN `xpoz_scraped` = 1 AND `xpoz_engagement_rate` > 0 THEN `xpoz_engagement_rate` END) AS avg_eng
    FROM `" . DB_TABLE . "`
")->fetch();

$icpData = $pdo->query("
    SELECT COALESCE(`xpoz_icp`, 'unknown') AS icp, COUNT(*) AS cnt
    FROM `" . DB_TABLE . "` WHERE `xpoz_scraped` = 1
    GROUP BY `xpoz_icp` ORDER BY cnt DESC
")->fetchAll();

$offerData = $pdo->query("
    SELECT COALESCE(`xpoz_offer_type`, 'unknown') AS ot, COUNT(*) AS cnt
    FROM `" . DB_TABLE . "` WHERE `xpoz_monetization` = 1
    GROUP BY `xpoz_offer_type` ORDER BY cnt DESC
")->fetchAll();

$strengthData = $pdo->query("
    SELECT COALESCE(`xpoz_monetization_strength`, 'none') AS ms, COUNT(*) AS cnt
    FROM `" . DB_TABLE . "` WHERE `xpoz_scraped` = 1
    GROUP BY `xpoz_monetization_strength` ORDER BY FIELD(ms, 'strong','moderate','weak','none')
")->fetchAll();

// Process status
$isRunning = false;
$output = shell_exec("ps aux | grep 'xpoz_parser.php' | grep -v grep 2>/dev/null");
if ($output && trim($output) !== '') $isRunning = true;
$activeWorkers = $isRunning ? max(1, substr_count($output, 'xpoz_parser.php')) : 0;

// Results table
$where = "`xpoz_scraped` = 1";
if ($filter === 'qualified')       $where .= " AND `xpoz_qualified` = 1";
elseif ($filter === 'monetization') $where .= " AND `xpoz_monetization` = 1";
elseif ($filter === 'errors')       $where .= " AND `xpoz_error` IS NOT NULL AND TRIM(`xpoz_error`) != ''";
elseif ($filter === 'c1')           $where .= " AND `xpoz_reels_performance` = 1";
elseif ($filter === 'c2')           $where .= " AND `xpoz_low_performing_reels` = 1";
elseif ($filter === 'c3')           $where .= " AND `xpoz_post_engagement` = 1";
elseif (str_starts_with($filter, 'icp_')) {
    $icpVal = strtoupper(substr($filter, 4));
    $where .= " AND `xpoz_icp` = " . $pdo->quote($icpVal);
}

$params = [];
if ($search !== '') {
    $where .= " AND (`instructor` LIKE :s OR `instagram_parcing` LIKE :s2 OR `xpoz_offer_type` LIKE :s3 OR `xpoz_icp` LIKE :s4)";
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

$dataStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `instagram_parcing`, `email_parcing`,
           `xpoz_follower_count`, `xpoz_reels_performance`, `xpoz_low_performing_reels`,
           `xpoz_post_engagement`, `xpoz_monetization`, `xpoz_engagement_rate`,
           `xpoz_icp`, `xpoz_offer_type`, `xpoz_funnel_type`, `xpoz_business_model`,
           `xpoz_audience_type`, `xpoz_monetization_strength`, `xpoz_monetization_reason`,
           `xpoz_c4_reason`, `xpoz_platform_mix`, `xpoz_primary_domain`, `xpoz_language`, `xpoz_geo_hint`,
           `xpoz_qualified`, `xpoz_error`, `xpoz_analyzed_at`, `validate_verdict`
    FROM `" . DB_TABLE . "` WHERE {$where}
    ORDER BY `{$sort}` {$dir}
    LIMIT {$perPage} OFFSET {$offset}
");
$dataStmt->execute($params);
$rows = $dataStmt->fetchAll();

// Log tail
$logFile      = xpoz_dashboard_log_file();
$logTail      = '';
$logWritable  = xpoz_dashboard_log_is_writable($logFile);
if (file_exists($logFile) && filesize($logFile) > 0) {
    $lines = file($logFile);
    if ($lines !== false) {
        $logTail = implode('', array_slice($lines, -40));
    }
}
if ($logTail === '') {
    if (!$logWritable) {
        $logTail = "Лог пустой или недоступен для записи.\n\n"
            . "Частая причина: PHP-FPM работает как www-data, а каталог проекта принадлежит root без прав на запись.\n\n"
            . "На сервере (подставьте свой путь):\n"
            . "  sudo touch " . $logFile . "\n"
            . "  sudo chown www-data:www-data " . $logFile . "\n"
            . "  sudo chmod 664 " . $logFile . "\n"
            . "или: sudo chown -R www-data:www-data " . dirname($logFile) . "\n";
    } else {
        $logTail = "Пока нет строк в логе. Запустите batch или одиночный анализ — вывод пишется в:\n" . $logFile;
    }
}

$progress = $stats['total_ig'] > 0 ? round($stats['scraped'] / $stats['total_ig'] * 100, 1) : 0;

// ── URL helpers ──────────────────────────────────────────────────────────────
function sortUrl(string $col, string $currentSort, string $currentDir): string {
    $newDir = ($col === $currentSort && $currentDir === 'DESC') ? 'asc' : 'desc';
    $params = $_GET;
    $params['sort'] = $col;
    $params['dir'] = $newDir;
    $params['page'] = 1;
    return '?' . http_build_query($params);
}
function sortIcon(string $col, string $currentSort, string $currentDir): string {
    if ($col !== $currentSort) return '';
    return $currentDir === 'DESC' ? ' ▼' : ' ▲';
}
function filterUrl(string $f): string {
    $params = $_GET;
    $params['filter'] = $f;
    $params['page'] = 1;
    return '?' . http_build_query($params);
}

$h = fn($s) => htmlspecialchars((string)$s, ENT_QUOTES, 'UTF-8');
$nf = fn($n) => number_format((int)$n, 0, '.', ' ');
$tf = fn($v) => $v ? '✅' : '❌';
?>
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Xpoz Dashboard — Instagram Qualifier</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh;padding:24px}
a{color:#60a5fa;text-decoration:none}
a:hover{text-decoration:underline}
h1{font-size:1.5rem;font-weight:700;color:#fff}
.subtitle{color:#64748b;font-size:0.82rem;margin-bottom:24px}
.flash{background:#1e3a5f;border:1px solid #3b82f6;border-radius:8px;padding:12px 16px;margin-bottom:20px;color:#93c5fd;font-size:0.85rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px;margin-bottom:24px}
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
.three-col{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:24px}
@media(max-width:900px){.three-col{grid-template-columns:1fr}}
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
.tag-blue{background:#1e3a5f;color:#60a5fa;border:1px solid #1e40af}
.tag-yellow{background:#422006;color:#fbbf24;border:1px solid #92400e}
.tag-purple{background:#3b0764;color:#c084fc;border:1px solid #581c87}
.tag-cyan{background:#083344;color:#22d3ee;border:1px solid #155e75}
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
.btn-primary{background:#1e40af;border-color:#3b82f6;color:#fff}
.btn-primary:hover{background:#1d4ed8}
.btn-danger{background:#7f1d1d;border-color:#991b1b;color:#fca5a5}
.btn-danger:hover{background:#991b1b}
.btn-green{background:#14532d;border-color:#166534;color:#4ade80}
.btn-green:hover{background:#166534}
.filter-pills{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:16px}
.pill{padding:4px 10px;border-radius:999px;font-size:0.72rem;font-weight:600;border:1px solid #2d3148;background:#1e2130;color:#94a3b8;cursor:pointer;text-decoration:none}
.pill:hover{border-color:#3b82f6;color:#e2e8f0;text-decoration:none}
.pill.active{background:#1e40af;border-color:#3b82f6;color:#fff}
.mini-bar{display:flex;height:6px;border-radius:3px;overflow:hidden;background:#2d3148;margin-top:4px}
.mini-bar span{display:block}
.pagination{display:flex;gap:4px;align-items:center;justify-content:center;margin-top:16px}
.pagination a,.pagination span{padding:4px 10px;border-radius:4px;font-size:0.78rem;border:1px solid #2d3148;color:#94a3b8}
.pagination a:hover{background:#252a3a;text-decoration:none}
.pagination .current{background:#1e40af;border-color:#3b82f6;color:#fff}
.log-box{background:#0d1117;border:1px solid #2d3148;border-radius:8px;padding:12px;font-family:'Courier New',monospace;font-size:0.72rem;line-height:1.5;color:#94a3b8;max-height:280px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.form-row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
.form-row label{font-size:0.78rem;color:#94a3b8;min-width:80px}
.form-row input[type=number]{background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:5px 10px;color:#e2e8f0;font-size:0.82rem;width:80px}
.form-row input[type=text]{background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:5px 10px;color:#e2e8f0;font-size:0.82rem;width:180px}
.form-row input:focus{outline:none;border-color:#3b82f6}
.breakdown{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.breakdown-item{text-align:center;padding:8px 12px;background:#252a3a;border-radius:6px;min-width:70px}
.breakdown-item .b-val{font-size:1.1rem;font-weight:700;color:#fff}
.breakdown-item .b-label{font-size:0.65rem;color:#64748b;margin-top:2px}
.trunc{max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
</style>
</head>
<body>

<div class="header-row">
    <div>
        <h1>Xpoz Dashboard</h1>
        <div class="subtitle">Instagram Qualifier &nbsp;·&nbsp; <?= date('d.m.Y H:i:s') ?> &nbsp;·&nbsp; <?= DB_NAME ?> › <?= DB_TABLE ?></div>
    </div>
    <div>
        <?php if ($isRunning): ?>
            <span class="status-badge running"><span class="dot pulse"></span> <?= $activeWorkers ?> воркер(ов) работает</span>
        <?php else: ?>
            <span class="status-badge stopped"><span class="dot"></span> Парсинг остановлен</span>
        <?php endif; ?>
    </div>
</div>

<?php if ($flash): ?>
<div class="flash"><?= $h($flash) ?></div>
<?php endif; ?>

<!-- Progress -->
<div class="progress-wrap">
    <div class="progress-header">
        <span class="progress-title">Прогресс анализа Instagram аккаунтов</span>
        <span class="progress-pct"><?= $progress ?>%</span>
    </div>
    <div class="progress-bar-bg">
        <div class="progress-bar-fill" style="width:<?= $progress ?>%"></div>
    </div>
    <div class="progress-nums">
        <span>Обработано: <?= $nf($stats['scraped']) ?></span>
        <span>Осталось: <?= $nf(max(0, $stats['total_ig'] - $stats['scraped'])) ?></span>
        <span>Всего с Instagram: <?= $nf($stats['total_ig']) ?></span>
    </div>
</div>

<!-- Stats cards -->
<div class="grid">
    <div class="card">
        <div class="card-label">Проанализировано</div>
        <div class="card-value blue"><?= $nf($stats['scraped']) ?></div>
        <div class="card-sub"><?= $progress ?>% от всех</div>
    </div>
    <div class="card">
        <div class="card-label">Квалифицированных</div>
        <div class="card-value green"><?= $nf($stats['qualified']) ?></div>
        <div class="card-sub">C1+C2+C3+C4</div>
    </div>
    <div class="card">
        <div class="card-label">С монетизацией</div>
        <div class="card-value purple"><?= $nf($stats['monetized']) ?></div>
        <div class="card-sub">C4=true</div>
    </div>
    <div class="card">
        <div class="card-label">C1: Reels охват</div>
        <div class="card-value cyan"><?= $nf($stats['c1_pass']) ?></div>
        <div class="card-sub">≥5 Reels >150% fol</div>
    </div>
    <div class="card">
        <div class="card-label">C2: Стабильность</div>
        <div class="card-value cyan"><?= $nf($stats['c2_pass']) ?></div>
        <div class="card-sub">bottom-10 avg >15%</div>
    </div>
    <div class="card">
        <div class="card-label">C3: Вовлечённость</div>
        <div class="card-value cyan"><?= $nf($stats['c3_pass']) ?></div>
        <div class="card-sub">eng rate ≥1.5%</div>
    </div>
    <div class="card">
        <div class="card-label">Avg engagement</div>
        <div class="card-value yellow"><?= $stats['avg_eng'] ? number_format((float)$stats['avg_eng'], 2) . '%' : '—' ?></div>
        <div class="card-sub">среднее по обработанным</div>
    </div>
    <div class="card">
        <div class="card-label">С ошибками</div>
        <div class="card-value red"><?= $nf($stats['errors']) ?></div>
        <div class="card-sub">xpoz_error</div>
    </div>
</div>

<!-- ICP + Offer type + Strength breakdowns -->
<div class="three-col">
    <div class="panel">
        <div class="section-title">ICP-сегменты</div>
        <div class="breakdown">
            <?php foreach ($icpData as $r):
                $icpColors = ['ICP1'=>'#4ade80','ICP2'=>'#60a5fa','ICP3'=>'#c084fc','ICP4'=>'#fbbf24','ICP5'=>'#f87171'];
                $color = $icpColors[$r['icp']] ?? '#94a3b8';
            ?>
            <a href="<?= filterUrl('icp_' . strtolower($r['icp'])) ?>" style="text-decoration:none">
                <div class="breakdown-item">
                    <div class="b-val" style="color:<?= $color ?>"><?= $nf($r['cnt']) ?></div>
                    <div class="b-label"><?= $h($r['icp']) ?></div>
                </div>
            </a>
            <?php endforeach; ?>
        </div>
    </div>
    <div class="panel">
        <div class="section-title">Типы офферов</div>
        <div class="breakdown">
            <?php foreach (array_slice($offerData, 0, 6) as $r): ?>
            <div class="breakdown-item">
                <div class="b-val" style="color:#c084fc"><?= $nf($r['cnt']) ?></div>
                <div class="b-label"><?= $h($r['ot']) ?></div>
            </div>
            <?php endforeach; ?>
        </div>
    </div>
    <div class="panel">
        <div class="section-title">Сила монетизации</div>
        <div class="breakdown">
            <?php
            $strColors = ['strong'=>'#4ade80','moderate'=>'#fbbf24','weak'=>'#f87171','none'=>'#64748b'];
            foreach ($strengthData as $r):
                $sc = $strColors[$r['ms']] ?? '#94a3b8';
            ?>
            <div class="breakdown-item">
                <div class="b-val" style="color:<?= $sc ?>"><?= $nf($r['cnt']) ?></div>
                <div class="b-label"><?= $h($r['ms']) ?></div>
            </div>
            <?php endforeach; ?>
        </div>
    </div>
</div>

<!-- Controls: single + batch + export -->
<div class="two-col">
    <div class="panel">
        <div class="section-title">Анализ одного аккаунта</div>
        <form method="POST">
            <input type="hidden" name="action" value="analyze_single">
            <div class="form-row">
                <input type="text" name="username" placeholder="username (без @)" required>
                <button type="submit" class="btn btn-primary">Анализировать</button>
            </div>
        </form>

        <div class="section-title" style="margin-top:20px">Batch запуск</div>
        <?php if ($isRunning): ?>
            <form method="POST" style="margin-bottom:8px">
                <input type="hidden" name="action" value="stop_batch">
                <button type="submit" class="btn btn-danger">Остановить все воркеры</button>
            </form>
        <?php else: ?>
            <form method="POST">
                <input type="hidden" name="action" value="start_batch">
                <div class="form-row">
                    <label>Воркеры</label>
                    <input type="number" name="workers" value="5" min="1" max="20">
                    <label>Лимит</label>
                    <input type="number" name="limit" value="0" min="0" title="0 = все">
                </div>
                <div class="form-row">
                    <label>С</label>
                    <input type="number" name="batch_from" value="0" min="0" title="Начать с аккаунта # (1-based, 0 = с начала)" style="width:90px">
                    <label>По</label>
                    <input type="number" name="batch_to" value="0" min="0" title="Закончить на аккаунте # (0 = до конца)" style="width:90px">
                </div>
                <div class="form-row">
                    <label title="Сайт, YouTube About, Twitter bio или Instagram bio (не NOT_FOUND/ошибки)"><input type="checkbox" name="require_email" value="1" checked style="margin-right:4px">Только со строкой email</label>
                </div>
                <div class="form-row">
                    <label><input type="checkbox" name="reanalyze" style="margin-right:4px">Перезапуск</label>
                    <button type="submit" class="btn btn-green">Запустить batch</button>
                </div>
            </form>
            <div style="margin-top:10px;padding:10px 12px;background:#0f1117;border:1px solid #2d3148;border-radius:6px;font-size:0.72rem;color:#64748b;line-height:1.6">
                <b style="color:#94a3b8">Подсказка:</b>
                <b>Лимит</b> — макс. аккаунтов <u>на каждый воркер</u> (0 = без лимита).<br>
                Пример: Воркеры=1, Лимит=100 → обработает 100 аккаунтов.<br>
                Пример: Воркеры=5, Лимит=20 → обработает до 100 (5×20).<br>
                <b>С / По</b> — порядковый номер в очереди (1-based, 0 = без ограничения).<br>
                <b>Перезапуск</b> — повторно анализировать уже обработанные аккаунты.
            </div>
        <?php endif; ?>

        <div class="section-title" style="margin-top:20px">Экспорт CSV</div>
        <form method="POST" style="display:flex;gap:6px">
            <input type="hidden" name="action" value="export_csv">
            <select name="export_filter" style="background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:5px 10px;color:#e2e8f0;font-size:0.82rem">
                <option value="all">Все обработанные</option>
                <option value="qualified">Только квалифицированные</option>
                <option value="monetization">С монетизацией</option>
            </select>
            <button type="submit" class="btn">Скачать CSV</button>
        </form>
    </div>

    <div class="panel">
        <div class="section-title">Лог (последние строки)</div>
        <div class="log-box" id="logbox"><?php
            $logText = $h($logTail);
            $logText = preg_replace('/✅/', '<span style="color:#4ade80">✅</span>', $logText);
            $logText = preg_replace('/⚠/', '<span style="color:#fbbf24">⚠</span>', $logText);
            $logText = preg_replace('/❌/', '<span style="color:#f87171">❌</span>', $logText);
            $logText = preg_replace('/QUALIFIED/', '<span style="color:#4ade80;font-weight:700">QUALIFIED</span>', $logText);
            echo $logText;
        ?></div>
    </div>
</div>

<!-- Results table -->
<div class="panel">
    <div class="section-title">Результаты анализа (<?= $nf($totalRows) ?>)</div>

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
        <a href="<?= filterUrl('all') ?>" class="pill <?= $filter==='all'?'active':'' ?>">Все (<?= $nf($stats['scraped']) ?>)</a>
        <a href="<?= filterUrl('qualified') ?>" class="pill <?= $filter==='qualified'?'active':'' ?>">Квалифицированные (<?= $nf($stats['qualified']) ?>)</a>
        <a href="<?= filterUrl('monetization') ?>" class="pill <?= $filter==='monetization'?'active':'' ?>">С монетизацией (<?= $nf($stats['monetized']) ?>)</a>
        <a href="<?= filterUrl('c1') ?>" class="pill <?= $filter==='c1'?'active':'' ?>">C1 Reels</a>
        <a href="<?= filterUrl('c2') ?>" class="pill <?= $filter==='c2'?'active':'' ?>">C2 Стаб.</a>
        <a href="<?= filterUrl('c3') ?>" class="pill <?= $filter==='c3'?'active':'' ?>">C3 Вовл.</a>
        <a href="<?= filterUrl('errors') ?>" class="pill <?= $filter==='errors'?'active':'' ?>">Ошибки (<?= $nf($stats['errors']) ?>)</a>
        <?php foreach (['ICP1','ICP2','ICP3','ICP4','ICP5'] as $icp): ?>
            <a href="<?= filterUrl('icp_' . strtolower($icp)) ?>" class="pill <?= $filter==='icp_'.strtolower($icp)?'active':'' ?>"><?= $icp ?></a>
        <?php endforeach; ?>
    </div>

    <div style="overflow-x:auto">
    <table>
        <thead>
        <tr>
            <th><a href="<?= sortUrl('instructor',$sort,$dir) ?>">Инструктор<?= sortIcon('instructor',$sort,$dir) ?></a></th>
            <th>Instagram</th>
            <th>Email</th>
            <th><a href="<?= sortUrl('xpoz_follower_count',$sort,$dir) ?>">Followers<?= sortIcon('xpoz_follower_count',$sort,$dir) ?></a></th>
            <th>C1</th><th>C2</th><th>C3</th><th>C4</th>
            <th><a href="<?= sortUrl('xpoz_engagement_rate',$sort,$dir) ?>">Eng%<?= sortIcon('xpoz_engagement_rate',$sort,$dir) ?></a></th>
            <th><a href="<?= sortUrl('xpoz_icp',$sort,$dir) ?>">ICP<?= sortIcon('xpoz_icp',$sort,$dir) ?></a></th>
            <th><a href="<?= sortUrl('xpoz_offer_type',$sort,$dir) ?>">Offer<?= sortIcon('xpoz_offer_type',$sort,$dir) ?></a></th>
            <th><a href="<?= sortUrl('xpoz_monetization_strength',$sort,$dir) ?>">Strength<?= sortIcon('xpoz_monetization_strength',$sort,$dir) ?></a></th>
            <th>Platform</th>
            <th>Domain</th>
            <th>Reason</th>
            <th>C4 Reason</th>
            <th><a href="<?= sortUrl('validate_verdict',$sort,$dir) ?>">Verdict<?= sortIcon('validate_verdict',$sort,$dir) ?></a></th>
            <th><a href="<?= sortUrl('xpoz_qualified',$sort,$dir) ?>">Qual<?= sortIcon('xpoz_qualified',$sort,$dir) ?></a></th>
        </tr>
        </thead>
        <tbody>
        <?php foreach ($rows as $r):
            $ig = preg_replace('#https?://(www\.)?instagram\.com/#', '', $r['instagram_parcing'] ?? '');
            $ig = rtrim(explode(',', $ig)[0], '/');
            $igLink = 'https://instagram.com/' . urlencode(ltrim($ig, '@'));

            $icpTag = match($r['xpoz_icp']) {
                'ICP1' => 'tag-green', 'ICP2' => 'tag-blue', 'ICP3' => 'tag-purple',
                'ICP4' => 'tag-yellow', 'ICP5' => 'tag-red', default => 'tag-gray',
            };
            $strTag = match($r['xpoz_monetization_strength']) {
                'strong' => 'tag-green', 'moderate' => 'tag-yellow', 'weak' => 'tag-red', default => 'tag-gray',
            };
        ?>
        <tr>
            <td class="trunc" title="<?= $h($r['instructor']) ?>"><?= $h(mb_strimwidth($r['instructor'] ?? '', 0, 22, '…')) ?></td>
            <td><a href="<?= $h($igLink) ?>" target="_blank" style="color:#f472b6">@<?= $h($ig) ?></a></td>
            <td class="mono trunc" style="color:#4ade80;max-width:160px" title="<?= $h($r['email_parcing']) ?>"><?= $h(mb_strimwidth(str_replace(';', ' ', $r['email_parcing'] ?? ''), 0, 28, '…')) ?></td>
            <td style="text-align:right;color:#e2e8f0"><?= $nf($r['xpoz_follower_count']) ?></td>
            <td><?= $tf($r['xpoz_reels_performance']) ?></td>
            <td><?= $tf($r['xpoz_low_performing_reels']) ?></td>
            <td><?= $tf($r['xpoz_post_engagement']) ?></td>
            <td><?= $tf($r['xpoz_monetization']) ?></td>
            <td style="text-align:right;color:#fbbf24"><?= $r['xpoz_engagement_rate'] ? number_format((float)$r['xpoz_engagement_rate'], 2) : '—' ?></td>
            <td><span class="tag <?= $icpTag ?>"><?= $h($r['xpoz_icp'] ?: '—') ?></span></td>
            <td style="font-size:0.72rem;color:#c084fc"><?= $h($r['xpoz_offer_type'] ?: '—') ?></td>
            <td><span class="tag <?= $strTag ?>"><?= $h($r['xpoz_monetization_strength'] ?: '—') ?></span></td>
            <td style="font-size:0.7rem;color:#64748b"><?= $h($r['xpoz_platform_mix'] ?: '') ?></td>
            <td style="font-size:0.72rem;color:#60a5fa"><?= $h($r['xpoz_primary_domain'] ?: '') ?></td>
            <td style="font-size:0.72rem;color:#94a3b8;max-width:220px;white-space:normal;line-height:1.4"><?= $h($r['xpoz_monetization_reason'] ?: '') ?></td>
            <td style="font-size:0.72rem;color:#c084fc;max-width:220px;white-space:normal;line-height:1.4"><?= $h($r['xpoz_c4_reason'] ?: '') ?></td>
            <td><?php
                $vv = $r['validate_verdict'] ?? '';
                $vvTag = match($vv) {
                    'valid' => 'tag-green', 'suspicious' => 'tag-yellow',
                    'mismatch' => 'tag-red', 'insufficient_data' => 'tag-gray',
                    default => '',
                };
                if ($vv): ?><span class="tag <?= $vvTag ?>"><?= $h($vv) ?></span><?php else: ?>—<?php endif;
            ?></td>
            <td><?php if ($r['xpoz_qualified']): ?><span class="tag tag-green">YES</span><?php elseif ($r['xpoz_error']): ?><span class="tag tag-red" title="<?= $h($r['xpoz_error']) ?>">ERR</span><?php else: ?><span class="tag tag-gray">NO</span><?php endif; ?></td>
        </tr>
        <?php endforeach; ?>
        <?php if (!$rows): ?>
        <tr><td colspan="18" style="text-align:center;color:#64748b;padding:24px">Нет данных</td></tr>
        <?php endif; ?>
        </tbody>
    </table>
    </div>

    <?php if ($totalPages > 1): ?>
    <div class="pagination">
        <?php if ($page > 1): ?>
            <a href="?<?= http_build_query(array_merge($_GET, ['page' => $page - 1])) ?>">←</a>
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
            <a href="?<?= http_build_query(array_merge($_GET, ['page' => $page + 1])) ?>">→</a>
        <?php endif; ?>
    </div>
    <?php endif; ?>
</div>

<div style="text-align:right;font-size:0.72rem;color:#334155;margin-top:16px">
    <a href="?" style="color:#3b82f6">Обновить</a> &nbsp;·&nbsp; xpoz_dashboard.php
</div>

<script>
const lb = document.getElementById('logbox');
if (lb) lb.scrollTop = lb.scrollHeight;
</script>
</body>
</html>
