<?php

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
        ]);
    }
    return $pdo;
}

$pdo = getDb();

// ── Основная статистика ────────────────────────────────────────────────────
$stats = $pdo->query("
    SELECT
        COUNT(*) AS total,
        COUNT(CASE WHEN website_parcing IS NOT NULL AND website_parcing != '' THEN 1 END) AS has_website,
        COUNT(CASE WHEN email_scraped = 1 THEN 1 END) AS processed,
        COUNT(CASE WHEN email_scraped IS NULL AND website_parcing IS NOT NULL AND website_parcing != '' THEN 1 END) AS remaining,
        COUNT(CASE WHEN email_parcing IS NOT NULL
                    AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL')
                    AND email_parcing NOT LIKE 'HTTP_%'
                    AND email_parcing NOT LIKE 'ERROR:%' THEN 1 END) AS found_emails,
        COUNT(CASE WHEN email_parcing = 'NOT_FOUND' THEN 1 END) AS not_found,
        COUNT(CASE WHEN email_parcing = 'JS_REQUIRED' THEN 1 END) AS js_required,
        COUNT(CASE WHEN email_parcing = 'SOCIAL_URL' THEN 1 END) AS social_url,
        COUNT(CASE WHEN email_parcing LIKE 'HTTP_%' THEN 1 END) AS http_errors,
        COUNT(CASE WHEN email_parcing = 'UNAVAILABLE' THEN 1 END) AS unavailable,
        COUNT(CASE WHEN email_parcing REGEXP '^RETRY:[0-9]+$' THEN 1 END) AS in_retry,
        COUNT(CASE WHEN linkedin_parcing IS NOT NULL AND linkedin_parcing != '' THEN 1 END) AS has_linkedin,
        COUNT(CASE WHEN instagram_parcing IS NOT NULL AND instagram_parcing != '' THEN 1 END) AS has_instagram,
        COUNT(CASE WHEN youtube_parcing IS NOT NULL AND youtube_parcing != '' THEN 1 END) AS has_youtube,
        COUNT(CASE WHEN twitter_parcing IS NOT NULL AND twitter_parcing != '' THEN 1 END) AS has_twitter,
        COUNT(CASE WHEN facebook_parcing IS NOT NULL AND facebook_parcing != '' THEN 1 END) AS has_facebook,
        COUNT(CASE WHEN tiktok_parcing IS NOT NULL AND tiktok_parcing != '' THEN 1 END) AS has_tiktok
    FROM `" . DB_TABLE . "`
")->fetch();

$processed  = (int) $stats['processed'];
$hasWebsite = (int) $stats['has_website'];
$progress   = $hasWebsite > 0 ? round($processed / $hasWebsite * 100, 1) : 0;

// ── Последние найденные email-ы ────────────────────────────────────────────
$recentEmails = $pdo->query("
    SELECT instructor, website_parcing, email_parcing, linkedin_parcing, instagram_parcing, email_scraped_at
    FROM `" . DB_TABLE . "`
    WHERE email_scraped = 1
      AND email_parcing IS NOT NULL
      AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL')
      AND email_parcing NOT LIKE 'HTTP_%'
      AND email_parcing NOT LIKE 'ERROR:%'
    ORDER BY CASE WHEN email_scraped_at IS NOT NULL THEN 0 ELSE 1 END ASC,
             email_scraped_at DESC,
             _rowid DESC
    LIMIT 15
")->fetchAll();

// ── Instagram + Email статистика ──────────────────────────────────────────
$instaStats = $pdo->query("
    SELECT
        COUNT(CASE WHEN instagram_parcing IS NOT NULL AND instagram_parcing != '' THEN 1 END) AS total_insta,
        COUNT(CASE WHEN (instagram_parcing IS NOT NULL AND instagram_parcing != '')
                    AND email_parcing IS NOT NULL
                    AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL')
                    AND email_parcing NOT LIKE 'HTTP_%' THEN 1 END) AS insta_with_email,
        COUNT(CASE WHEN (instagram_parcing IS NOT NULL AND instagram_parcing != '')
                    AND (email_parcing IS NULL
                         OR email_parcing IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL')
                         OR email_parcing LIKE 'HTTP_%') THEN 1 END) AS insta_no_email
    FROM `" . DB_TABLE . "`
")->fetch();

// ── Последние Instagram + Email ────────────────────────────────────────────
$recentInstaEmail = $pdo->query("
    SELECT instructor, instagram_parcing, email_parcing, email_scraped_at
    FROM `" . DB_TABLE . "`
    WHERE instagram_parcing IS NOT NULL AND instagram_parcing != ''
      AND email_parcing IS NOT NULL
      AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL')
      AND email_parcing NOT LIKE 'HTTP_%'
    ORDER BY CASE WHEN email_scraped_at IS NOT NULL THEN 0 ELSE 1 END ASC,
             email_scraped_at DESC,
             _rowid DESC
    LIMIT 20
")->fetchAll();

// ── YouTube Email статистика ─────────────────────────────────────────────────
$ytStats = $pdo->query("
    SELECT
        COUNT(CASE WHEN youtube_parcing IS NOT NULL AND youtube_parcing != '' THEN 1 END) AS total_yt,
        COUNT(CASE WHEN youtube_parcing_email IS NOT NULL
                    AND youtube_parcing_email NOT IN ('NOT_FOUND','INVALID_URL') THEN 1 END) AS yt_with_email,
        COUNT(CASE WHEN (youtube_parcing IS NOT NULL AND youtube_parcing != '')
                    AND youtube_parcing_email = 'NOT_FOUND' THEN 1 END) AS yt_not_found,
        COUNT(CASE WHEN (youtube_parcing IS NOT NULL AND youtube_parcing != '')
                    AND youtube_parcing_email IS NULL THEN 1 END) AS yt_pending
    FROM `" . DB_TABLE . "`
")->fetch();

$recentYtEmail = $pdo->query("
    SELECT instructor, youtube_parcing, youtube_parcing_email
    FROM `" . DB_TABLE . "`
    WHERE youtube_parcing_email IS NOT NULL
      AND youtube_parcing_email NOT IN ('NOT_FOUND','INVALID_URL')
    ORDER BY _rowid DESC
    LIMIT 10
")->fetchAll();

// ── Twitter Email статистика ─────────────────────────────────────────────────
$twStats = $pdo->query("
    SELECT
        COUNT(CASE WHEN twitter_parcing IS NOT NULL AND twitter_parcing != '' THEN 1 END) AS total_tw,
        COUNT(CASE WHEN twitter_parcing_email IS NOT NULL
                    AND twitter_parcing_email NOT IN ('NOT_FOUND','INVALID_URL') THEN 1 END) AS tw_with_email,
        COUNT(CASE WHEN (twitter_parcing IS NOT NULL AND twitter_parcing != '')
                    AND twitter_parcing_email = 'NOT_FOUND' THEN 1 END) AS tw_not_found,
        COUNT(CASE WHEN (twitter_parcing IS NOT NULL AND twitter_parcing != '')
                    AND twitter_parcing_email IS NULL THEN 1 END) AS tw_pending
    FROM `" . DB_TABLE . "`
")->fetch();

$recentTwEmail = $pdo->query("
    SELECT instructor, twitter_parcing, twitter_parcing_email
    FROM `" . DB_TABLE . "`
    WHERE twitter_parcing_email IS NOT NULL
      AND twitter_parcing_email NOT IN ('NOT_FOUND','INVALID_URL')
    ORDER BY _rowid DESC
    LIMIT 10
")->fetchAll();

// ── HTTP коды ошибок ───────────────────────────────────────────────────────
$httpErrors = $pdo->query("
    SELECT email_parcing AS code, COUNT(*) AS cnt
    FROM `" . DB_TABLE . "`
    WHERE email_parcing LIKE 'HTTP_%'
    GROUP BY email_parcing
    ORDER BY cnt DESC
    LIMIT 10
")->fetchAll();

// ── Статус процесса парсинга ───────────────────────────────────────────────
$isRunning = false;
$processInfo = '';
$output = shell_exec('ps aux | grep scrape_emails | grep -v grep 2>/dev/null');
if ($output && trim($output) !== '') {
    $isRunning = true;
    $processInfo = trim($output);
}

// ── Последние строки лога ──────────────────────────────────────────────────
$logFile = __DIR__ . '/scrape_emails.log';
$logLines = [];
if (file_exists($logFile)) {
    $lines = file($logFile);
    $logLines = array_slice($lines, -30);
}

$refreshSec = 30;
?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="<?= $refreshSec ?>">
    <title>Udemy Parcing Dashboard</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0f1117;
            color: #e2e8f0;
            min-height: 100vh;
            padding: 24px;
        }

        h1 {
            font-size: 1.6rem;
            font-weight: 700;
            color: #fff;
            margin-bottom: 4px;
        }

        .subtitle {
            color: #64748b;
            font-size: 0.85rem;
            margin-bottom: 28px;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 28px;
        }

        .card {
            background: #1e2130;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 20px;
        }

        .card-label {
            font-size: 0.75rem;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 8px;
        }

        .card-value {
            font-size: 2rem;
            font-weight: 700;
            color: #fff;
            line-height: 1;
        }

        .card-value.green  { color: #4ade80; }
        .card-value.blue   { color: #60a5fa; }
        .card-value.yellow { color: #fbbf24; }
        .card-value.red    { color: #f87171; }
        .card-value.purple { color: #c084fc; }

        .card-sub {
            font-size: 0.78rem;
            color: #64748b;
            margin-top: 6px;
        }

        .progress-wrap {
            background: #1e2130;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 28px;
        }

        .progress-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }

        .progress-title {
            font-size: 0.9rem;
            font-weight: 600;
            color: #cbd5e1;
        }

        .progress-pct {
            font-size: 1.4rem;
            font-weight: 700;
            color: #60a5fa;
        }

        .progress-bar-bg {
            background: #2d3148;
            border-radius: 999px;
            height: 12px;
            overflow: hidden;
        }

        .progress-bar-fill {
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
            transition: width 0.5s ease;
        }

        .progress-nums {
            display: flex;
            justify-content: space-between;
            font-size: 0.78rem;
            color: #64748b;
            margin-top: 8px;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 12px;
            border-radius: 999px;
            font-size: 0.8rem;
            font-weight: 600;
        }

        .status-badge.running {
            background: #14532d;
            color: #4ade80;
            border: 1px solid #166534;
        }

        .status-badge.stopped {
            background: #450a0a;
            color: #f87171;
            border: 1px solid #7f1d1d;
        }

        .dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: currentColor;
        }

        .dot.pulse {
            animation: pulse 1.5s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        .section-title {
            font-size: 0.9rem;
            font-weight: 600;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 12px;
        }

        .two-col {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 28px;
        }

        @media (max-width: 768px) {
            .two-col { grid-template-columns: 1fr; }
        }

        .panel {
            background: #1e2130;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 20px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.82rem;
        }

        th {
            text-align: left;
            padding: 6px 10px;
            color: #64748b;
            font-weight: 500;
            border-bottom: 1px solid #2d3148;
        }

        td {
            padding: 8px 10px;
            border-bottom: 1px solid #1a1f2e;
            vertical-align: top;
        }

        tr:last-child td { border-bottom: none; }

        tr:hover td { background: #252a3a; }

        .email-cell {
            color: #4ade80;
            font-family: monospace;
            font-size: 0.8rem;
        }

        .site-cell {
            color: #60a5fa;
            font-size: 0.78rem;
            max-width: 180px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .instructor-cell {
            color: #e2e8f0;
            max-width: 160px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .social-icons {
            display: flex;
            gap: 4px;
            flex-wrap: wrap;
        }

        .social-tag {
            font-size: 0.7rem;
            padding: 2px 6px;
            border-radius: 4px;
            background: #2d3148;
            color: #94a3b8;
        }

        .log-box {
            background: #0d1117;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 16px;
            font-family: 'Courier New', monospace;
            font-size: 0.75rem;
            line-height: 1.6;
            color: #94a3b8;
            max-height: 340px;
            overflow-y: auto;
            white-space: pre-wrap;
            word-break: break-all;
        }

        .log-box .log-email  { color: #4ade80; }
        .log-box .log-error  { color: #f87171; }
        .log-box .log-http   { color: #60a5fa; }
        .log-box .log-site   { color: #fbbf24; }
        .log-box .log-social { color: #c084fc; }

        .social-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
        }

        .social-card {
            background: #252a3a;
            border-radius: 8px;
            padding: 14px;
            text-align: center;
        }

        .social-card .s-name {
            font-size: 0.72rem;
            color: #64748b;
            margin-bottom: 4px;
            text-transform: uppercase;
        }

        .social-card .s-val {
            font-size: 1.4rem;
            font-weight: 700;
            color: #fff;
        }

        .refresh-note {
            text-align: right;
            font-size: 0.75rem;
            color: #334155;
            margin-top: 24px;
        }

        .header-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 28px;
            flex-wrap: wrap;
            gap: 12px;
        }
    </style>
</head>
<body>

<div class="header-row">
    <div>
        <h1>Udemy Parcing Dashboard</h1>
        <div class="subtitle">Обновляется каждые <?= $refreshSec ?> сек &nbsp;·&nbsp; <?= date('d.m.Y H:i:s') ?></div>
    </div>
    <div>
        <?php if ($isRunning): ?>
            <span class="status-badge running"><span class="dot pulse"></span> Парсинг запущен</span>
        <?php else: ?>
            <span class="status-badge stopped"><span class="dot"></span> Парсинг остановлен</span>
        <?php endif; ?>
    </div>
</div>

<!-- Прогресс-бар -->
<div class="progress-wrap">
    <div class="progress-header">
        <span class="progress-title">Прогресс обработки сайтов</span>
        <span class="progress-pct"><?= $progress ?>%</span>
    </div>
    <div class="progress-bar-bg">
        <div class="progress-bar-fill" style="width: <?= $progress ?>%"></div>
    </div>
    <div class="progress-nums">
        <span>Обработано: <?= number_format($processed, 0, '.', ' ') ?></span>
        <span>Осталось: <?= number_format((int)$stats['remaining'], 0, '.', ' ') ?></span>
        <span>Всего сайтов: <?= number_format($hasWebsite, 0, '.', ' ') ?></span>
    </div>
</div>

<!-- Карточки -->
<div class="grid">
    <div class="card">
        <div class="card-label">Всего записей</div>
        <div class="card-value blue"><?= number_format((int)$stats['total'], 0, '.', ' ') ?></div>
        <div class="card-sub">в базе данных</div>
    </div>
    <div class="card">
        <div class="card-label">Найдено email</div>
        <div class="card-value green"><?= number_format((int)$stats['found_emails'], 0, '.', ' ') ?></div>
        <div class="card-sub">
            <?= $processed > 0 ? round($stats['found_emails'] / $processed * 100, 1) : 0 ?>% от обработанных
        </div>
    </div>
    <div class="card">
        <div class="card-label">Обработано</div>
        <div class="card-value blue"><?= number_format($processed, 0, '.', ' ') ?></div>
        <div class="card-sub"><?= $progress ?>% от сайтов</div>
    </div>
    <div class="card">
        <div class="card-label">Осталось</div>
        <div class="card-value yellow"><?= number_format((int)$stats['remaining'], 0, '.', ' ') ?></div>
        <div class="card-sub">сайтов в очереди</div>
    </div>
    <div class="card">
        <div class="card-label">Нет email</div>
        <div class="card-value red"><?= number_format((int)$stats['not_found'], 0, '.', ' ') ?></div>
        <div class="card-sub">NOT_FOUND</div>
    </div>
    <div class="card">
        <div class="card-label">JS-сайты</div>
        <div class="card-value purple"><?= number_format((int)$stats['js_required'], 0, '.', ' ') ?></div>
        <div class="card-sub">требуют браузер</div>
    </div>
    <div class="card">
        <div class="card-label">Соцсети</div>
        <div class="card-value"><?= number_format((int)$stats['social_url'], 0, '.', ' ') ?></div>
        <div class="card-sub">пропущено (SOCIAL_URL)</div>
    </div>
    <div class="card">
        <div class="card-label">HTTP ошибки</div>
        <div class="card-value red"><?= number_format((int)$stats['http_errors'], 0, '.', ' ') ?></div>
        <div class="card-sub">4xx / 5xx</div>
    </div>
    <div class="card">
        <div class="card-label">Недоступен (5 попыток)</div>
        <div class="card-value red"><?= number_format((int)$stats['unavailable'], 0, '.', ' ') ?></div>
        <div class="card-sub">UNAVAILABLE</div>
    </div>
    <div class="card">
        <div class="card-label">В очереди повторов</div>
        <div class="card-value yellow"><?= number_format((int)$stats['in_retry'], 0, '.', ' ') ?></div>
        <div class="card-sub">RETRY:1..4</div>
    </div>
</div>

<!-- Соцсети -->
<div class="panel" style="margin-bottom: 28px;">
    <div class="section-title">Найдено соцсетей с сайтов</div>
    <div class="social-grid">
        <div class="social-card">
            <div class="s-name">LinkedIn</div>
            <div class="s-val" style="color:#60a5fa"><?= number_format((int)$stats['has_linkedin'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">Instagram</div>
            <div class="s-val" style="color:#f472b6"><?= number_format((int)$stats['has_instagram'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">YouTube</div>
            <div class="s-val" style="color:#f87171"><?= number_format((int)$stats['has_youtube'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">Twitter / X</div>
            <div class="s-val" style="color:#94a3b8"><?= number_format((int)$stats['has_twitter'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">Facebook</div>
            <div class="s-val" style="color:#818cf8"><?= number_format((int)$stats['has_facebook'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">TikTok</div>
            <div class="s-val" style="color:#2dd4bf"><?= number_format((int)$stats['has_tiktok'], 0, '.', ' ') ?></div>
        </div>
    </div>
</div>

<!-- Instagram + Email блок -->
<div class="panel" style="margin-bottom: 28px;">
    <div class="section-title">Instagram аккаунты + Email</div>
    <div style="display:grid; grid-template-columns: repeat(3,1fr); gap:12px; margin-bottom:20px;">
        <div class="social-card">
            <div class="s-name">Всего Instagram</div>
            <div class="s-val" style="color:#f472b6"><?= number_format((int)$instaStats['total_insta'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">Instagram + Email</div>
            <div class="s-val" style="color:#4ade80"><?= number_format((int)$instaStats['insta_with_email'], 0, '.', ' ') ?></div>
            <div style="font-size:0.72rem; color:#64748b; margin-top:4px;">
                <?= $instaStats['total_insta'] > 0 ? round($instaStats['insta_with_email'] / $instaStats['total_insta'] * 100, 1) : 0 ?>% от всех
            </div>
        </div>
        <div class="social-card">
            <div class="s-name">Instagram без Email</div>
            <div class="s-val" style="color:#fbbf24"><?= number_format((int)$instaStats['insta_no_email'], 0, '.', ' ') ?></div>
        </div>
    </div>
    <table>
        <thead>
            <tr>
                <th>Инструктор</th>
                <th>Instagram</th>
                <th>Email</th>
                <th>Время</th>
            </tr>
        </thead>
        <tbody>
            <?php foreach ($recentInstaEmail as $row): ?>
            <tr>
                <td class="instructor-cell" title="<?= htmlspecialchars($row['instructor'] ?? '') ?>">
                    <?= htmlspecialchars(mb_strimwidth($row['instructor'] ?? '', 0, 28, '…')) ?>
                </td>
                <td style="font-size:0.78rem;">
                    <?php
                        $instaUrl = $row['instagram_parcing'] ?? '';
                        $instaHandle = preg_replace('#https?://(www\.)?instagram\.com/#', '@', $instaUrl);
                        $instaHandle = rtrim($instaHandle, '/');
                    ?>
                    <a href="<?= htmlspecialchars($instaUrl) ?>" target="_blank"
                       style="color:#f472b6; text-decoration:none;">
                        <?= htmlspecialchars($instaHandle) ?>
                    </a>
                </td>
                <td class="email-cell" style="font-size:0.78rem;">
                    <?= htmlspecialchars(str_replace(';', ' ', $row['email_parcing'] ?? '')) ?>
                </td>
                <td style="color:#64748b; font-size:0.75rem; white-space:nowrap;">
                    <?= !empty($row['email_scraped_at']) ? date('H:i:s', strtotime($row['email_scraped_at'])) : '—' ?>
                </td>
            </tr>
            <?php endforeach; ?>
        </tbody>
    </table>
</div>

<!-- YouTube Email блок -->
<div class="panel" style="margin-bottom: 28px;">
    <div class="section-title">YouTube каналы → Email</div>
    <div style="display:grid; grid-template-columns: repeat(4,1fr); gap:12px; margin-bottom:20px;">
        <div class="social-card">
            <div class="s-name">Всего YouTube</div>
            <div class="s-val" style="color:#f87171"><?= number_format((int)$ytStats['total_yt'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">Найдено Email</div>
            <div class="s-val" style="color:#4ade80"><?= number_format((int)$ytStats['yt_with_email'], 0, '.', ' ') ?></div>
            <div style="font-size:0.72rem; color:#64748b; margin-top:4px;">
                <?= $ytStats['total_yt'] > 0 ? round($ytStats['yt_with_email'] / $ytStats['total_yt'] * 100, 1) : 0 ?>%
            </div>
        </div>
        <div class="social-card">
            <div class="s-name">Не найдено</div>
            <div class="s-val" style="color:#fbbf24"><?= number_format((int)$ytStats['yt_not_found'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">В очереди</div>
            <div class="s-val" style="color:#60a5fa"><?= number_format((int)$ytStats['yt_pending'], 0, '.', ' ') ?></div>
        </div>
    </div>
    <?php if (!empty($recentYtEmail)): ?>
    <table>
        <thead>
            <tr><th>Инструктор</th><th>YouTube</th><th>Email</th></tr>
        </thead>
        <tbody>
            <?php foreach ($recentYtEmail as $row):
                $ytUrl = $row['youtube_parcing'] ?? '';
                $ytShort = preg_replace('#https?://(www\.)?youtube\.com/#', '', $ytUrl);
                $ytShort = preg_replace('/[,;].*$/', '', $ytShort);
                $ytShort = rtrim($ytShort, '/');
            ?>
            <tr>
                <td class="instructor-cell" title="<?= htmlspecialchars($row['instructor'] ?? '') ?>">
                    <?= htmlspecialchars(mb_strimwidth($row['instructor'] ?? '', 0, 28, '…')) ?>
                </td>
                <td style="font-size:0.78rem; max-width:140px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">
                    <a href="<?= htmlspecialchars(explode(',', $ytUrl)[0]) ?>" target="_blank"
                       style="color:#f87171; text-decoration:none;" title="<?= htmlspecialchars($ytUrl) ?>">
                        <?= htmlspecialchars(mb_strimwidth($ytShort, 0, 24, '…')) ?>
                    </a>
                </td>
                <td class="email-cell" style="font-size:0.78rem;">
                    <?= htmlspecialchars(str_replace(';', ' ', $row['youtube_parcing_email'] ?? '')) ?>
                </td>
            </tr>
            <?php endforeach; ?>
        </tbody>
    </table>
    <?php endif; ?>
</div>

<!-- Twitter/X Email блок -->
<div class="panel" style="margin-bottom: 28px;">
    <div class="section-title">Twitter/X → Email</div>
    <div style="display:grid; grid-template-columns: repeat(4,1fr); gap:12px; margin-bottom:20px;">
        <div class="social-card">
            <div class="s-name">Всего Twitter</div>
            <div class="s-val" style="color:#94a3b8"><?= number_format((int)$twStats['total_tw'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">Найдено Email</div>
            <div class="s-val" style="color:#4ade80"><?= number_format((int)$twStats['tw_with_email'], 0, '.', ' ') ?></div>
            <div style="font-size:0.72rem; color:#64748b; margin-top:4px;">
                <?= $twStats['total_tw'] > 0 ? round($twStats['tw_with_email'] / $twStats['total_tw'] * 100, 1) : 0 ?>%
            </div>
        </div>
        <div class="social-card">
            <div class="s-name">Не найдено</div>
            <div class="s-val" style="color:#fbbf24"><?= number_format((int)$twStats['tw_not_found'], 0, '.', ' ') ?></div>
        </div>
        <div class="social-card">
            <div class="s-name">В очереди</div>
            <div class="s-val" style="color:#60a5fa"><?= number_format((int)$twStats['tw_pending'], 0, '.', ' ') ?></div>
        </div>
    </div>
    <?php if (!empty($recentTwEmail)): ?>
    <table>
        <thead>
            <tr><th>Инструктор</th><th>Twitter</th><th>Email</th></tr>
        </thead>
        <tbody>
            <?php foreach ($recentTwEmail as $row):
                $twUrl = $row['twitter_parcing'] ?? '';
                $twHandle = preg_replace('#https?://(www\.)?(twitter\.com|x\.com)/#', '@', $twUrl);
                $twHandle = preg_replace('/[,;].*$/', '', $twHandle);
                $twHandle = rtrim($twHandle, '/');
            ?>
            <tr>
                <td class="instructor-cell" title="<?= htmlspecialchars($row['instructor'] ?? '') ?>">
                    <?= htmlspecialchars(mb_strimwidth($row['instructor'] ?? '', 0, 28, '…')) ?>
                </td>
                <td style="font-size:0.78rem;">
                    <a href="<?= htmlspecialchars(explode(',', $twUrl)[0]) ?>" target="_blank"
                       style="color:#94a3b8; text-decoration:none;">
                        <?= htmlspecialchars(mb_strimwidth($twHandle, 0, 20, '…')) ?>
                    </a>
                </td>
                <td class="email-cell" style="font-size:0.78rem;">
                    <?= htmlspecialchars(str_replace(';', ' ', $row['twitter_parcing_email'] ?? '')) ?>
                </td>
            </tr>
            <?php endforeach; ?>
        </tbody>
    </table>
    <?php endif; ?>
</div>

<div class="two-col">
    <!-- Последние найденные email -->
    <div class="panel">
        <div class="section-title">Последние найденные email</div>
        <table>
            <thead>
                <tr>
                    <th>Инструктор</th>
                    <th>Email</th>
                    <th>Соцсети</th>
                    <th>Время</th>
                </tr>
            </thead>
            <tbody>
                <?php foreach ($recentEmails as $row): ?>
                <tr>
                    <td class="instructor-cell" title="<?= htmlspecialchars($row['instructor'] ?? '') ?>">
                        <?= htmlspecialchars(mb_strimwidth($row['instructor'] ?? '', 0, 30, '…')) ?>
                    </td>
                    <td class="email-cell">
                        <?= htmlspecialchars(str_replace(';', ' ', $row['email_parcing'] ?? '')) ?>
                    </td>
                    <td>
                        <div class="social-icons">
                            <?php if (!empty($row['linkedin_parcing'])): ?>
                                <span class="social-tag" style="color:#60a5fa">in</span>
                            <?php endif; ?>
                            <?php if (!empty($row['instagram_parcing'])): ?>
                                <span class="social-tag" style="color:#f472b6">ig</span>
                            <?php endif; ?>
                        </div>
                    </td>
                    <td style="color:#64748b; font-size:0.75rem; white-space:nowrap;">
                        <?php if (!empty($row['email_scraped_at'])): ?>
                            <?= date('H:i:s', strtotime($row['email_scraped_at'])) ?>
                        <?php else: ?>
                            —
                        <?php endif; ?>
                    </td>
                </tr>
                <?php endforeach; ?>
            </tbody>
        </table>
    </div>

    <!-- HTTP ошибки + лог -->
    <div style="display:flex; flex-direction:column; gap:20px;">
        <?php if (!empty($httpErrors)): ?>
        <div class="panel">
            <div class="section-title">HTTP ошибки</div>
            <table>
                <thead>
                    <tr><th>Код</th><th>Количество</th></tr>
                </thead>
                <tbody>
                    <?php foreach ($httpErrors as $e): ?>
                    <tr>
                        <td style="color:#f87171; font-family:monospace"><?= htmlspecialchars($e['code']) ?></td>
                        <td style="color:#e2e8f0"><?= number_format((int)$e['cnt'], 0, '.', ' ') ?></td>
                    </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        </div>
        <?php endif; ?>

        <div class="panel">
            <div class="section-title">Лог (последние строки)</div>
            <div class="log-box" id="logbox">
<?php
foreach ($logLines as $line) {
    $line = htmlspecialchars(rtrim($line));
    if (str_contains($line, 'Email:') || str_contains($line, 'Сохраняем email') || str_contains($line, '✓')) {
        echo '<span class="log-email">' . $line . '</span>' . "\n";
    } elseif (str_contains($line, 'ERROR') || str_contains($line, 'error') || str_contains($line, 'ВНИМАНИЕ')) {
        echo '<span class="log-error">' . $line . '</span>' . "\n";
    } elseif (str_contains($line, 'HTTP ')) {
        echo '<span class="log-http">' . $line . '</span>' . "\n";
    } elseif (str_contains($line, 'Site:') || str_contains($line, 'Checking:')) {
        echo '<span class="log-site">' . $line . '</span>' . "\n";
    } elseif (str_contains($line, 'Соцсети')) {
        echo '<span class="log-social">' . $line . '</span>' . "\n";
    } else {
        echo $line . "\n";
    }
}
?>
            </div>
        </div>
    </div>
</div>

<div class="refresh-note">
    Автообновление через <?= $refreshSec ?> сек &nbsp;·&nbsp;
    <a href="?" style="color:#3b82f6; text-decoration:none;">Обновить сейчас</a>
</div>

<script>
    // Прокрутить лог вниз
    const lb = document.getElementById('logbox');
    if (lb) lb.scrollTop = lb.scrollHeight;
</script>

</body>
</html>
