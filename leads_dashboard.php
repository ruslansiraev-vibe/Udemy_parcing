<?php

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');
define('PER_PAGE', 100);

$pdo = new PDO(
    "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4",
    DB_USER, DB_PASS,
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC]
);

$columns = [
    'instructor'            => 'Instructor',
    'UdemyProfile1_parcing' => 'Udemy Profile 1',
    'UdemyProfile2_parcing' => 'Udemy Profile 2',
    'UdemyProfile3_parcing' => 'Udemy Profile 3',
    'website_parcing'       => 'Website',
    'linkedin_parcing'      => 'LinkedIn',
    'youtube_parcing'       => 'YouTube',
    'facebook_parcing'      => 'Facebook',
    'twitter_parcing'       => 'Twitter',
    'instagram_parcing'     => 'Instagram',
    'tiktok_parcing'        => 'TikTok',
    'email_parcing'         => 'Email',
];

$filterableCols = array_keys($columns);
array_shift($filterableCols); // remove 'instructor' — always shown

$search  = trim($_GET['q'] ?? '');
$page    = max(1, (int)($_GET['page'] ?? 1));
$filters = [];
foreach ($filterableCols as $col) {
    if (isset($_GET['f_' . $col])) {
        $filters[] = $col;
    }
}

// --- Build WHERE clause ---
$where = [];
$params = [];

if ($search !== '') {
    $searchWhere = [];
    foreach (array_keys($columns) as $col) {
        $searchWhere[] = "`$col` LIKE :search_$col";
        $params["search_$col"] = "%$search%";
    }
    $where[] = '(' . implode(' OR ', $searchWhere) . ')';
}

foreach ($filters as $col) {
    $where[] = "(`$col` IS NOT NULL AND `$col` != '')";
}

$whereSQL = $where ? 'WHERE ' . implode(' AND ', $where) : '';

// --- Counts ---
$countSQL = "SELECT COUNT(*) AS cnt FROM `" . DB_TABLE . "` $whereSQL";
$stmt = $pdo->prepare($countSQL);
$stmt->execute($params);
$totalFiltered = (int)$stmt->fetch()['cnt'];

$totalPages = max(1, (int)ceil($totalFiltered / PER_PAGE));
if ($page > $totalPages) $page = $totalPages;
$offset = ($page - 1) * PER_PAGE;

// --- Column stats (always computed on filtered set) ---
$statParts = [];
foreach ($filterableCols as $col) {
    $statParts[] = "COUNT(CASE WHEN `$col` IS NOT NULL AND `$col` != '' THEN 1 END) AS `cnt_$col`";
}
$statsSQL = "SELECT " . implode(', ', $statParts) . " FROM `" . DB_TABLE . "` $whereSQL";
$stmt = $pdo->prepare($statsSQL);
$stmt->execute($params);
$colStats = $stmt->fetch();

// --- Fetch rows ---
$selectCols = implode(', ', array_map(fn($c) => "`$c`", array_keys($columns)));
$dataSQL = "SELECT $selectCols FROM `" . DB_TABLE . "` $whereSQL ORDER BY `instructor` ASC LIMIT $offset, " . PER_PAGE;
$stmt = $pdo->prepare($dataSQL);
$stmt->execute($params);
$rows = $stmt->fetchAll();

// --- Total (unfiltered) ---
$totalAll = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "`")->fetchColumn();

function buildUrl(array $overrides): string {
    $params = $_GET;
    foreach ($overrides as $k => $v) {
        if ($v === null) unset($params[$k]);
        else $params[$k] = $v;
    }
    return '?' . http_build_query($params);
}

$colColors = [
    'UdemyProfile1_parcing' => '#60a5fa',
    'UdemyProfile2_parcing' => '#60a5fa',
    'UdemyProfile3_parcing' => '#60a5fa',
    'website_parcing'       => '#fbbf24',
    'linkedin_parcing'      => '#3b82f6',
    'youtube_parcing'       => '#f87171',
    'facebook_parcing'      => '#818cf8',
    'twitter_parcing'       => '#94a3b8',
    'instagram_parcing'     => '#f472b6',
    'tiktok_parcing'        => '#2dd4bf',
    'email_parcing'         => '#4ade80',
];

?>
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Leads Explorer</title>
    <style>
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
            background: #0f1117;
            color: #e2e8f0;
            min-height: 100vh;
        }

        .container { margin: 0 auto; padding: 24px; }

        h1 { font-size: 1.5rem; font-weight: 700; color: #fff; }
        .subtitle { color: #64748b; font-size: 0.82rem; margin-top: 2px; }

        .header {
            display: flex; align-items: center; justify-content: space-between;
            margin-bottom: 24px; flex-wrap: wrap; gap: 12px;
        }

        /* --- Summary cards --- */
        .stats-row {
            display: flex; flex-wrap: wrap; gap: 10px;
            margin-bottom: 20px;
        }

        .stat-chip {
            background: #1e2130; border: 1px solid #2d3148; border-radius: 10px;
            padding: 12px 16px; min-width: 120px; text-align: center;
            transition: border-color .2s;
        }
        .stat-chip:hover { border-color: #475569; }
        .stat-label { font-size: 0.68rem; color: #64748b; text-transform: uppercase; letter-spacing: .04em; margin-bottom: 4px; }
        .stat-value { font-size: 1.35rem; font-weight: 700; color: #fff; line-height: 1; }

        /* --- Search + filters --- */
        .toolbar {
            background: #1e2130; border: 1px solid #2d3148; border-radius: 12px;
            padding: 16px 20px; margin-bottom: 20px;
            display: flex; flex-wrap: wrap; gap: 16px; align-items: center;
        }

        .search-box {
            display: flex; align-items: center; gap: 8px; flex: 1; min-width: 240px;
        }

        .search-box input {
            flex: 1; background: #0f1117; border: 1px solid #2d3148; border-radius: 8px;
            padding: 8px 14px; color: #e2e8f0; font-size: 0.88rem; outline: none;
            transition: border-color .2s;
        }
        .search-box input:focus { border-color: #3b82f6; }
        .search-box input::placeholder { color: #475569; }

        .btn {
            background: #3b82f6; color: #fff; border: none; border-radius: 8px;
            padding: 8px 18px; font-size: 0.84rem; font-weight: 600; cursor: pointer;
            transition: background .2s;
        }
        .btn:hover { background: #2563eb; }
        .btn-ghost {
            background: transparent; border: 1px solid #2d3148; color: #94a3b8;
        }
        .btn-ghost:hover { background: #252a3a; color: #e2e8f0; }

        .filters-wrap {
            display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
        }

        .filter-label { font-size: 0.75rem; color: #64748b; font-weight: 600; text-transform: uppercase; }

        .cb-label {
            display: flex; align-items: center; gap: 5px;
            font-size: 0.78rem; color: #94a3b8; cursor: pointer;
            background: #252a3a; border-radius: 6px; padding: 4px 10px;
            border: 1px solid transparent; transition: all .2s; user-select: none;
        }
        .cb-label:hover { border-color: #475569; color: #e2e8f0; }
        .cb-label.active { border-color: #3b82f6; background: #1e293b; color: #60a5fa; }
        .cb-label input { display: none; }

        /* --- Table --- */
        .table-wrap {
            background: #1e2130; border: 1px solid #2d3148; border-radius: 12px;
            overflow: hidden;
        }

        .table-scroll { overflow-x: auto; }

        table { border-collapse: collapse; font-size: 0.8rem; width: max-content; min-width: 100%; }

        thead th {
            position: sticky; top: 0; z-index: 2;
            background: #181b28; text-align: left; padding: 10px 12px;
            color: #64748b; font-weight: 600; font-size: 0.72rem;
            text-transform: uppercase; letter-spacing: .04em;
            border-bottom: 1px solid #2d3148; white-space: nowrap;
        }

        tbody td {
            padding: 8px 12px; border-bottom: 1px solid #1a1f2e;
            vertical-align: top; white-space: nowrap;
        }

        tbody tr:hover td { background: #252a3a; }
        tbody tr:last-child td { border-bottom: none; }

        .cell-instructor { color: #e2e8f0; font-weight: 500; }
        .cell-link { color: #60a5fa; text-decoration: none; }
        .cell-link:hover { text-decoration: underline; }
        .cell-email { color: #4ade80; font-family: monospace; font-size: 0.78rem; }
        .cell-empty { color: #334155; }

        /* --- Pagination --- */
        .pagination {
            display: flex; align-items: center; justify-content: space-between;
            padding: 14px 20px; border-top: 1px solid #2d3148;
            font-size: 0.82rem; color: #64748b;
        }

        .page-links { display: flex; gap: 4px; }

        .page-link {
            display: inline-flex; align-items: center; justify-content: center;
            min-width: 34px; height: 34px; border-radius: 6px;
            background: #252a3a; color: #94a3b8; text-decoration: none;
            font-size: 0.82rem; border: 1px solid transparent; transition: all .15s;
        }
        .page-link:hover { border-color: #475569; color: #e2e8f0; }
        .page-link.active { background: #3b82f6; color: #fff; border-color: #3b82f6; }
        .page-link.disabled { opacity: .35; pointer-events: none; }

        .result-info { font-size: 0.78rem; color: #475569; }

        @media (max-width: 768px) {
            .container { padding: 12px; }
            .stats-row { gap: 6px; }
            .stat-chip { min-width: 90px; padding: 8px 10px; }
            .stat-value { font-size: 1.1rem; }
        }
    </style>
</head>
<body>
<div class="container">

<div class="header">
    <div>
        <h1>Leads Explorer</h1>
        <div class="subtitle"><?= number_format($totalAll, 0, '.', ' ') ?> instructors &middot; <?= date('d.m.Y H:i') ?></div>
    </div>
    <a href="dashboard.php" class="btn btn-ghost" style="text-decoration:none;">Main Dashboard</a>
</div>

<!-- Summary stats -->
<div class="stats-row">
    <div class="stat-chip">
        <div class="stat-label">Результат</div>
        <div class="stat-value" style="color:#60a5fa"><?= number_format($totalFiltered, 0, '.', ' ') ?></div>
    </div>
    <?php foreach ($filterableCols as $col): ?>
    <div class="stat-chip">
        <div class="stat-label"><?= $columns[$col] ?></div>
        <div class="stat-value" style="color:<?= $colColors[$col] ?? '#fff' ?>"><?= number_format((int)$colStats["cnt_$col"], 0, '.', ' ') ?></div>
    </div>
    <?php endforeach; ?>
</div>

<!-- Toolbar: search + filters -->
<form method="get" id="filterForm">
<div class="toolbar">
    <div class="search-box">
        <input type="text" name="q" value="<?= htmlspecialchars($search) ?>" placeholder="Search by any field...">
        <button type="submit" class="btn">Search</button>
        <?php if ($search !== '' || !empty($filters)): ?>
            <a href="?" class="btn btn-ghost" style="text-decoration:none;">Reset</a>
        <?php endif; ?>
    </div>
    <div class="filters-wrap">
        <span class="filter-label">Only filled:</span>
        <?php foreach ($filterableCols as $col):
            $checked = in_array($col, $filters);
        ?>
        <label class="cb-label <?= $checked ? 'active' : '' ?>">
            <input type="checkbox" name="f_<?= $col ?>" value="1" <?= $checked ? 'checked' : '' ?>
                   onchange="document.getElementById('filterForm').submit()">
            <?= $columns[$col] ?>
        </label>
        <?php endforeach; ?>
    </div>
</div>
</form>

<!-- Table -->
<div class="table-wrap">
<div class="table-scroll">
<table>
    <thead>
        <tr>
            <th>#</th>
            <?php foreach ($columns as $label): ?>
                <th><?= $label ?></th>
            <?php endforeach; ?>
        </tr>
    </thead>
    <tbody>
        <?php if (empty($rows)): ?>
            <tr><td colspan="<?= count($columns) + 1 ?>" style="text-align:center; padding:40px; color:#475569;">No results</td></tr>
        <?php endif; ?>
        <?php foreach ($rows as $i => $row): ?>
        <tr>
            <td style="color:#475569; font-size:0.75rem;"><?= $offset + $i + 1 ?></td>
            <?php foreach (array_keys($columns) as $col):
                $val = $row[$col] ?? '';
                if ($col === 'instructor'): ?>
                    <td class="cell-instructor"><?= htmlspecialchars($val) ?></td>
                <?php elseif ($col === 'email_parcing'): ?>
                    <td class="cell-email"><?= $val !== '' ? htmlspecialchars($val) : '<span class="cell-empty">&mdash;</span>' ?></td>
                <?php elseif ($val !== ''):
                    $isUrl = str_starts_with($val, 'http');
                ?>
                    <td>
                        <?php if ($isUrl): ?>
                            <a href="<?= htmlspecialchars(explode(',', $val)[0]) ?>" target="_blank" class="cell-link"><?= htmlspecialchars($val) ?></a>
                        <?php else: ?>
                            <?= htmlspecialchars($val) ?>
                        <?php endif; ?>
                    </td>
                <?php else: ?>
                    <td><span class="cell-empty">&mdash;</span></td>
                <?php endif; ?>
            <?php endforeach; ?>
        </tr>
        <?php endforeach; ?>
    </tbody>
</table>
</div>

<!-- Pagination -->
<div class="pagination">
    <div class="result-info">
        <?= number_format($totalFiltered, 0, '.', ' ') ?> results
        <?php if ($search !== ''): ?> for &laquo;<?= htmlspecialchars($search) ?>&raquo;<?php endif; ?>
        <?php if (!empty($filters)): ?> &middot; filters: <?= count($filters) ?><?php endif; ?>
    </div>
    <div class="page-links">
        <?php
        $maxVisible = 7;
        $half = (int)floor($maxVisible / 2);
        $startP = max(1, $page - $half);
        $endP = min($totalPages, $startP + $maxVisible - 1);
        if ($endP - $startP + 1 < $maxVisible) $startP = max(1, $endP - $maxVisible + 1);
        ?>
        <a href="<?= buildUrl(['page' => $page - 1]) ?>" class="page-link <?= $page <= 1 ? 'disabled' : '' ?>">&lsaquo;</a>
        <?php if ($startP > 1): ?>
            <a href="<?= buildUrl(['page' => 1]) ?>" class="page-link">1</a>
            <?php if ($startP > 2): ?><span class="page-link disabled">&hellip;</span><?php endif; ?>
        <?php endif; ?>
        <?php for ($p = $startP; $p <= $endP; $p++): ?>
            <a href="<?= buildUrl(['page' => $p]) ?>" class="page-link <?= $p === $page ? 'active' : '' ?>"><?= $p ?></a>
        <?php endfor; ?>
        <?php if ($endP < $totalPages): ?>
            <?php if ($endP < $totalPages - 1): ?><span class="page-link disabled">&hellip;</span><?php endif; ?>
            <a href="<?= buildUrl(['page' => $totalPages]) ?>" class="page-link"><?= $totalPages ?></a>
        <?php endif; ?>
        <a href="<?= buildUrl(['page' => $page + 1]) ?>" class="page-link <?= $page >= $totalPages ? 'disabled' : '' ?>">&rsaquo;</a>
    </div>
</div>

</div><!-- table-wrap -->

</div><!-- container -->
</body>
</html>
