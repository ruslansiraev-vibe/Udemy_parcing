<?php
/**
 * Сводная таблица: Instagram аккаунт + email из всех источников.
 * Для каждого инструктора с Instagram — показывает все найденные email:
 *   - email_parcing (с сайта)
 *   - youtube_parcing_email (с YouTube About)
 *   - twitter_parcing_email (с Twitter bio)
 * + итоговая статистика.
 */

require_once __DIR__ . '/instagram_normalize.php';

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

$dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
$pdo = new PDO($dsn, DB_USER, DB_PASS, [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
]);

$skipValues = ['NOT_FOUND', 'JS_REQUIRED', 'SOCIAL_URL', 'UNAVAILABLE', 'INVALID_URL'];
$skipLike   = ['HTTP_%', 'ERROR:%', 'RETRY:%'];

function isRealEmail(?string $v): bool
{
    global $skipValues, $skipLike;
    if ($v === null || $v === '') return false;
    foreach ($skipValues as $s) {
        if ($v === $s) return false;
    }
    foreach ($skipLike as $p) {
        $pattern = str_replace(['%', '_'], ['.*', '.'], $p);
        if (preg_match('/^' . $pattern . '$/i', $v)) return false;
    }
    return true;
}

// ── CSV export ───────────────────────────────────────────────────────────────

if (isset($_GET['export']) && $_GET['export'] === 'csv') {
    $csvRows = $pdo->query("
        SELECT
            `instructor`,
            `instagram_parcing`,
            `email_parcing`,
            `youtube_parcing_email`,
            `twitter_parcing_email`
        FROM `" . DB_TABLE . "`
        WHERE instagram_parcing IS NOT NULL AND instagram_parcing != ''
          AND (
            (email_parcing IS NOT NULL AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
             AND email_parcing NOT LIKE 'HTTP_%' AND email_parcing NOT LIKE 'ERROR:%' AND email_parcing NOT LIKE 'RETRY:%')
            OR (youtube_parcing_email IS NOT NULL AND youtube_parcing_email NOT IN ('NOT_FOUND','INVALID_URL'))
            OR (twitter_parcing_email IS NOT NULL AND twitter_parcing_email NOT IN ('NOT_FOUND','INVALID_URL'))
          )
        ORDER BY `_rowid` ASC
    ")->fetchAll();

    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename="instagram_emails_summary_' . date('Y-m-d') . '.csv"');

    $out = fopen('php://output', 'w');
    fprintf($out, chr(0xEF) . chr(0xBB) . chr(0xBF));
    fputcsv($out, ['instagram', 'email_site', 'email_youtube', 'email_twitter', 'instructor']);

    foreach ($csvRows as $r) {
        $igRaw = $r['instagram_parcing'] ?? '';
        $igUrls = normalizeInstagramFieldToUrls($igRaw);
        $igNames = [];
        foreach ($igUrls as $u) {
            $n = instagramNormalizedUrlToUsername($u);
            if ($n !== '') $igNames[] = $n;
        }
        if (empty($igNames)) {
            $n = instagramRawToUsername($igRaw);
            if ($n !== '') $igNames[] = $n;
        }
        if (empty($igNames)) {
            $igNames[] = preg_replace('#https?://(www\.)?instagram\.com/#', '', explode(',', $igRaw)[0]);
            $igNames[0] = rtrim($igNames[0], '/');
        }

        $siteEmail = isRealEmail($r['email_parcing']) ? $r['email_parcing'] : '';
        $ytEmail   = isRealEmail($r['youtube_parcing_email']) ? $r['youtube_parcing_email'] : '';
        $twEmail   = isRealEmail($r['twitter_parcing_email']) ? $r['twitter_parcing_email'] : '';
        $instructor = $r['instructor'] ?? '';

        foreach ($igNames as $igName) {
            fputcsv($out, [$igName, $siteEmail, $ytEmail, $twEmail, $instructor]);
        }
    }

    fclose($out);
    exit;
}

// ── Stats ────────────────────────────────────────────────────────────────────

$stats = $pdo->query("
    SELECT
        COUNT(*) AS total,
        COUNT(CASE WHEN instagram_parcing IS NOT NULL AND instagram_parcing != '' THEN 1 END) AS has_insta,
        COUNT(CASE WHEN email_parcing IS NOT NULL
                    AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
                    AND email_parcing NOT LIKE 'HTTP_%'
                    AND email_parcing NOT LIKE 'ERROR:%'
                    AND email_parcing NOT LIKE 'RETRY:%' THEN 1 END) AS has_site_email,
        COUNT(CASE WHEN youtube_parcing_email IS NOT NULL
                    AND youtube_parcing_email NOT IN ('NOT_FOUND','INVALID_URL') THEN 1 END) AS has_yt_email,
        COUNT(CASE WHEN twitter_parcing_email IS NOT NULL
                    AND twitter_parcing_email NOT IN ('NOT_FOUND','INVALID_URL') THEN 1 END) AS has_tw_email
    FROM `" . DB_TABLE . "`
")->fetch();

$comboStats = $pdo->query("
    SELECT
        COUNT(*) AS insta_total,
        COUNT(CASE WHEN
            (email_parcing IS NOT NULL AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
             AND email_parcing NOT LIKE 'HTTP_%' AND email_parcing NOT LIKE 'ERROR:%' AND email_parcing NOT LIKE 'RETRY:%')
            OR (youtube_parcing_email IS NOT NULL AND youtube_parcing_email NOT IN ('NOT_FOUND','INVALID_URL'))
            OR (twitter_parcing_email IS NOT NULL AND twitter_parcing_email NOT IN ('NOT_FOUND','INVALID_URL'))
        THEN 1 END) AS insta_with_any_email
    FROM `" . DB_TABLE . "`
    WHERE instagram_parcing IS NOT NULL AND instagram_parcing != ''
")->fetch();

// ── Data rows: Instagram + any email ─────────────────────────────────────────

$page    = max(1, (int) ($_GET['page'] ?? 1));
$perPage = 50;
$offset  = ($page - 1) * $perPage;

$totalRows = (int) $comboStats['insta_with_any_email'];
$totalPages = max(1, (int) ceil($totalRows / $perPage));

$rows = $pdo->query("
    SELECT
        `_rowid`,
        `instructor`,
        `instagram_parcing`,
        `email_parcing`,
        `youtube_parcing_email`,
        `twitter_parcing_email`
    FROM `" . DB_TABLE . "`
    WHERE instagram_parcing IS NOT NULL AND instagram_parcing != ''
      AND (
        (email_parcing IS NOT NULL AND email_parcing NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
         AND email_parcing NOT LIKE 'HTTP_%' AND email_parcing NOT LIKE 'ERROR:%' AND email_parcing NOT LIKE 'RETRY:%')
        OR (youtube_parcing_email IS NOT NULL AND youtube_parcing_email NOT IN ('NOT_FOUND','INVALID_URL'))
        OR (twitter_parcing_email IS NOT NULL AND twitter_parcing_email NOT IN ('NOT_FOUND','INVALID_URL'))
      )
    ORDER BY `_rowid` ASC
    LIMIT $perPage OFFSET $offset
")->fetchAll();

?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Instagram + Email Summary</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0f1117;
            color: #e2e8f0;
            min-height: 100vh;
            padding: 24px;
        }

        h1 { font-size: 1.5rem; font-weight: 700; color: #fff; margin-bottom: 4px; }

        .subtitle { color: #64748b; font-size: 0.85rem; margin-bottom: 28px; }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
            gap: 14px;
            margin-bottom: 28px;
        }

        .stat-card {
            background: #1e2130;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 18px;
            text-align: center;
        }

        .stat-label {
            font-size: 0.72rem;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 6px;
        }

        .stat-val {
            font-size: 1.8rem;
            font-weight: 700;
            line-height: 1;
        }

        .stat-sub { font-size: 0.72rem; color: #64748b; margin-top: 4px; }

        .summary-bar {
            background: #1e2130;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 18px 22px;
            margin-bottom: 28px;
            display: flex;
            align-items: center;
            gap: 24px;
            flex-wrap: wrap;
        }

        .summary-bar .big {
            font-size: 2.2rem;
            font-weight: 800;
            color: #4ade80;
        }

        .summary-bar .desc {
            font-size: 0.9rem;
            color: #94a3b8;
            line-height: 1.5;
        }

        .panel {
            background: #1e2130;
            border: 1px solid #2d3148;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 28px;
        }

        .section-title {
            font-size: 0.9rem;
            font-weight: 600;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 14px;
        }

        table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }

        th {
            text-align: left;
            padding: 8px 10px;
            color: #64748b;
            font-weight: 500;
            border-bottom: 1px solid #2d3148;
            position: sticky;
            top: 0;
            background: #1e2130;
        }

        td {
            padding: 8px 10px;
            border-bottom: 1px solid #1a1f2e;
            vertical-align: top;
        }

        tr:hover td { background: #252a3a; }

        .email-cell { color: #4ade80; font-family: monospace; font-size: 0.78rem; }
        .email-none { color: #334155; font-size: 0.75rem; }
        .insta-link { color: #f472b6; text-decoration: none; }
        .insta-link:hover { text-decoration: underline; }

        .instructor-cell {
            max-width: 180px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .pagination {
            display: flex;
            gap: 6px;
            align-items: center;
            justify-content: center;
            margin-top: 20px;
            flex-wrap: wrap;
        }

        .pagination a, .pagination span {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.82rem;
            text-decoration: none;
        }

        .pagination a {
            background: #252a3a;
            color: #94a3b8;
            border: 1px solid #2d3148;
        }

        .pagination a:hover { background: #2d3148; color: #fff; }

        .pagination .current {
            background: #3b82f6;
            color: #fff;
            border: 1px solid #3b82f6;
        }

        .pagination .info { color: #64748b; font-size: 0.78rem; }

        .back-link {
            display: inline-block;
            margin-bottom: 20px;
            color: #3b82f6;
            text-decoration: none;
            font-size: 0.85rem;
        }

        .back-link:hover { text-decoration: underline; }

        .csv-btn {
            display: inline-block;
            padding: 10px 22px;
            background: #166534;
            color: #4ade80;
            border: 1px solid #22c55e;
            border-radius: 8px;
            font-size: 0.9rem;
            font-weight: 600;
            text-decoration: none;
            transition: background 0.2s;
        }

        .csv-btn:hover { background: #15803d; }

        .src-tag {
            display: inline-block;
            font-size: 0.65rem;
            padding: 1px 5px;
            border-radius: 3px;
            font-weight: 600;
            margin-right: 2px;
        }

        .src-site    { background: #1e3a5f; color: #60a5fa; }
        .src-youtube { background: #3f1515; color: #f87171; }
        .src-twitter { background: #1e293b; color: #94a3b8; }
    </style>
</head>
<body>

<div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px; margin-bottom:20px;">
    <div>
        <a href="dashboard.php" class="back-link" style="margin-bottom:8px;">&larr; Dashboard</a>
        <h1>Instagram + Email — Сводная таблица</h1>
        <div class="subtitle"><?= date('d.m.Y H:i:s') ?></div>
    </div>
    <a href="?export=csv" class="csv-btn">&#8615; Скачать CSV</a>
</div>

<!-- Итоговая строка -->
<div class="summary-bar">
    <div class="big"><?= number_format($totalRows, 0, '.', ' ') ?></div>
    <div class="desc">
        Инструкторов с <strong>Instagram</strong> аккаунтом<br>
        и хотя бы одним email (сайт / YouTube / Twitter)
        <span style="color:#64748b;">из <?= number_format((int)$comboStats['insta_total'], 0, '.', ' ') ?> с Instagram
        (<?= $comboStats['insta_total'] > 0 ? round($totalRows / $comboStats['insta_total'] * 100, 1) : 0 ?>%)</span>
    </div>
</div>

<!-- Карточки -->
<div class="stats-grid">
    <div class="stat-card">
        <div class="stat-label">Всего в базе</div>
        <div class="stat-val" style="color:#60a5fa"><?= number_format((int)$stats['total'], 0, '.', ' ') ?></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Есть Instagram</div>
        <div class="stat-val" style="color:#f472b6"><?= number_format((int)$stats['has_insta'], 0, '.', ' ') ?></div>
        <div class="stat-sub"><?= round($stats['has_insta'] / $stats['total'] * 100, 1) ?>%</div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Email с сайта</div>
        <div class="stat-val" style="color:#60a5fa"><?= number_format((int)$stats['has_site_email'], 0, '.', ' ') ?></div>
        <div class="stat-sub"><span class="src-tag src-site">SITE</span></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Email с YouTube</div>
        <div class="stat-val" style="color:#f87171"><?= number_format((int)$stats['has_yt_email'], 0, '.', ' ') ?></div>
        <div class="stat-sub"><span class="src-tag src-youtube">YT</span></div>
    </div>
    <div class="stat-card">
        <div class="stat-label">Email с Twitter</div>
        <div class="stat-val" style="color:#94a3b8"><?= number_format((int)$stats['has_tw_email'], 0, '.', ' ') ?></div>
        <div class="stat-sub"><span class="src-tag src-twitter">TW</span></div>
    </div>
</div>

<!-- Таблица -->
<div class="panel">
    <div class="section-title">Инструкторы с Instagram + Email (стр. <?= $page ?> / <?= $totalPages ?>)</div>
    <div style="overflow-x:auto;">
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Инструктор</th>
                <th>Instagram</th>
                <th><span class="src-tag src-site">SITE</span> Email с сайта</th>
                <th><span class="src-tag src-youtube">YT</span> Email с YouTube</th>
                <th><span class="src-tag src-twitter">TW</span> Email с Twitter</th>
            </tr>
        </thead>
        <tbody>
            <?php foreach ($rows as $i => $row):
                $igRaw = $row['instagram_parcing'] ?? '';
                $igHandle = preg_replace('#https?://(www\.)?instagram\.com/#', '@', $igRaw);
                $igHandle = preg_replace('/[,;].*$/', '', $igHandle);
                $igHandle = rtrim($igHandle, '/');

                $siteEmail = isRealEmail($row['email_parcing']) ? $row['email_parcing'] : null;
                $ytEmail   = isRealEmail($row['youtube_parcing_email']) ? $row['youtube_parcing_email'] : null;
                $twEmail   = isRealEmail($row['twitter_parcing_email']) ? $row['twitter_parcing_email'] : null;

                $num = $offset + $i + 1;
            ?>
            <tr>
                <td style="color:#334155; font-size:0.75rem;"><?= $num ?></td>
                <td class="instructor-cell" title="<?= htmlspecialchars($row['instructor'] ?? '') ?>">
                    <?= htmlspecialchars(mb_strimwidth($row['instructor'] ?? '', 0, 30, '…')) ?>
                </td>
                <td style="font-size:0.78rem; white-space:nowrap;">
                    <a href="<?= htmlspecialchars(explode(',', $igRaw)[0]) ?>" target="_blank" class="insta-link">
                        <?= htmlspecialchars(mb_strimwidth($igHandle, 0, 22, '…')) ?>
                    </a>
                </td>
                <td class="<?= $siteEmail ? 'email-cell' : 'email-none' ?>">
                    <?= $siteEmail ? htmlspecialchars(str_replace(';', "\n", $siteEmail)) : '—' ?>
                </td>
                <td class="<?= $ytEmail ? 'email-cell' : 'email-none' ?>">
                    <?= $ytEmail ? htmlspecialchars(str_replace(';', "\n", $ytEmail)) : '—' ?>
                </td>
                <td class="<?= $twEmail ? 'email-cell' : 'email-none' ?>">
                    <?= $twEmail ? htmlspecialchars(str_replace(';', "\n", $twEmail)) : '—' ?>
                </td>
            </tr>
            <?php endforeach; ?>
        </tbody>
    </table>
    </div>

    <!-- Pagination -->
    <div class="pagination">
        <?php if ($page > 1): ?>
            <a href="?page=1">&laquo;</a>
            <a href="?page=<?= $page - 1 ?>">&lsaquo;</a>
        <?php endif; ?>

        <?php
        $start = max(1, $page - 3);
        $end   = min($totalPages, $page + 3);
        for ($p = $start; $p <= $end; $p++):
        ?>
            <?php if ($p === $page): ?>
                <span class="current"><?= $p ?></span>
            <?php else: ?>
                <a href="?page=<?= $p ?>"><?= $p ?></a>
            <?php endif; ?>
        <?php endfor; ?>

        <?php if ($page < $totalPages): ?>
            <a href="?page=<?= $page + 1 ?>">&rsaquo;</a>
            <a href="?page=<?= $totalPages ?>">&raquo;</a>
        <?php endif; ?>

        <span class="info">&nbsp; <?= number_format($totalRows, 0, '.', ' ') ?> записей</span>
    </div>
</div>

</body>
</html>
