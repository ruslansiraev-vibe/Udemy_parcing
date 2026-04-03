<?php
// ─── DB config ────────────────────────────────────────────────────────────────
define('DB_HOST',  '127.0.0.1');
define('DB_PORT',  3306);
define('DB_NAME',  'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER',  'root');
define('DB_PASS',  '');

// ─── Query ────────────────────────────────────────────────────────────────────
$sql = "
SELECT метрика, значение FROM (

    SELECT 1 AS ord, 'Всего строк'          AS метрика, CAST(COUNT(*)                                         AS CHAR) AS значение FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 2,        'Обработано',           CAST(SUM(social_scraped = 1)                                     AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 3,        'Ещё в очереди',        CAST(SUM(social_scraped IS NULL)                                 AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 4,        '% обработано',         CONCAT(ROUND(SUM(social_scraped=1)/COUNT(*)*100,1),'%')          FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 5,        '---',                  '---'                                                             FROM dual

    UNION ALL

    SELECT 6,  'Website',    CAST(SUM(website_parcing   != '' AND website_parcing   IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 7,  'LinkedIn',   CAST(SUM(linkedin_parcing  != '' AND linkedin_parcing  IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 8,  'YouTube',    CAST(SUM(youtube_parcing   != '' AND youtube_parcing   IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 9,  'Facebook',   CAST(SUM(facebook_parcing  != '' AND facebook_parcing  IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 10, 'Twitter',    CAST(SUM(twitter_parcing   != '' AND twitter_parcing   IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 11, 'Instagram',  CAST(SUM(instagram_parcing != '' AND instagram_parcing IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 12, 'TikTok',     CAST(SUM(tiktok_parcing    != '' AND tiktok_parcing    IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 13, 'Email',      CAST(SUM(email_parcing     != '' AND email_parcing     IS NOT NULL) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 14, '---',        '---'                                                                         FROM dual

    UNION ALL

    SELECT 15, 'Website %',   CONCAT(ROUND(SUM(website_parcing   != '' AND website_parcing   IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 16, 'LinkedIn %',  CONCAT(ROUND(SUM(linkedin_parcing  != '' AND linkedin_parcing  IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 17, 'YouTube %',   CONCAT(ROUND(SUM(youtube_parcing   != '' AND youtube_parcing   IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 18, 'Facebook %',  CONCAT(ROUND(SUM(facebook_parcing  != '' AND facebook_parcing  IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 19, 'Twitter %',   CONCAT(ROUND(SUM(twitter_parcing   != '' AND twitter_parcing   IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 20, 'Instagram %', CONCAT(ROUND(SUM(instagram_parcing != '' AND instagram_parcing IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 21, 'TikTok %',    CONCAT(ROUND(SUM(tiktok_parcing    != '' AND tiktok_parcing    IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 22, 'Email %',     CONCAT(ROUND(SUM(email_parcing     != '' AND email_parcing     IS NOT NULL)/NULLIF(SUM(social_scraped=1),0)*100,1),'%') FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 23, '---',         '---'                                                                                                                    FROM dual

    UNION ALL

    SELECT 24, 'Хотя бы 1 контакт', CAST(SUM(
        (website_parcing   != '' AND website_parcing   IS NOT NULL) OR
        (linkedin_parcing  != '' AND linkedin_parcing  IS NOT NULL) OR
        (youtube_parcing   != '' AND youtube_parcing   IS NOT NULL) OR
        (facebook_parcing  != '' AND facebook_parcing  IS NOT NULL) OR
        (twitter_parcing   != '' AND twitter_parcing   IS NOT NULL) OR
        (instagram_parcing != '' AND instagram_parcing IS NOT NULL) OR
        (tiktok_parcing    != '' AND tiktok_parcing    IS NOT NULL) OR
        (email_parcing     != '' AND email_parcing     IS NOT NULL)
    ) AS CHAR) FROM `" . DB_TABLE . "`
    UNION ALL
    SELECT 25, 'Хотя бы 1 контакт %', CONCAT(ROUND(SUM(
        (website_parcing   != '' AND website_parcing   IS NOT NULL) OR
        (linkedin_parcing  != '' AND linkedin_parcing  IS NOT NULL) OR
        (youtube_parcing   != '' AND youtube_parcing   IS NOT NULL) OR
        (facebook_parcing  != '' AND facebook_parcing  IS NOT NULL) OR
        (twitter_parcing   != '' AND twitter_parcing   IS NOT NULL) OR
        (instagram_parcing != '' AND instagram_parcing IS NOT NULL) OR
        (tiktok_parcing    != '' AND tiktok_parcing    IS NOT NULL) OR
        (email_parcing     != '' AND email_parcing     IS NOT NULL)
    ) / COUNT(*) * 100, 1),'%') FROM `" . DB_TABLE . "`

) AS t
ORDER BY ord
";

// ─── Fetch data ───────────────────────────────────────────────────────────────
$rows  = [];
$error = null;
$generatedAt = date('d.m.Y H:i:s');

try {
    $dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
    $pdo = new PDO($dsn, DB_USER, DB_PASS, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $rows = $pdo->query($sql)->fetchAll(PDO::FETCH_NUM);
} catch (PDOException $e) {
    $error = $e->getMessage();
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
$icons = [
    'Website'    => '🌐',
    'LinkedIn'   => '💼',
    'YouTube'    => '▶️',
    'Facebook'   => '📘',
    'Twitter'    => '🐦',
    'Instagram'  => '📸',
    'TikTok'     => '🎵',
    'Email'      => '✉️',
    'Website %'  => '🌐',
    'LinkedIn %' => '💼',
    'YouTube %'  => '▶️',
    'Facebook %' => '📘',
    'Twitter %'  => '🐦',
    'Instagram %'=> '📸',
    'TikTok %'   => '🎵',
    'Email %'    => '✉️',
];

function isPercent(string $val): bool {
    return str_ends_with($val, '%') && $val !== '---';
}

function percentBar(string $val): string {
    $num = (float) rtrim($val, '%');
    $color = $num >= 50 ? '#22c55e' : ($num >= 20 ? '#f59e0b' : '#ef4444');
    return '<div class="bar-wrap"><div class="bar" style="width:' . min($num, 100) . '%;background:' . $color . '"></div>'
         . '<span class="bar-label">' . htmlspecialchars($val) . '</span></div>';
}
?>
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Статистика парсинга — <?= DB_TABLE ?></title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #0f172a;
    color: #e2e8f0;
    min-height: 100vh;
    padding: 32px 16px;
  }

  .container { max-width: 680px; margin: 0 auto; }

  header {
    margin-bottom: 28px;
  }
  header h1 {
    font-size: 1.5rem;
    font-weight: 700;
    color: #f8fafc;
  }
  header .sub {
    font-size: 0.85rem;
    color: #64748b;
    margin-top: 4px;
  }
  header .db-badge {
    display: inline-block;
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 6px;
    padding: 2px 10px;
    font-size: 0.8rem;
    color: #94a3b8;
    margin-top: 8px;
  }

  .refresh-btn {
    float: right;
    background: #3b82f6;
    color: #fff;
    border: none;
    border-radius: 8px;
    padding: 8px 18px;
    font-size: 0.85rem;
    cursor: pointer;
    text-decoration: none;
    display: inline-block;
    transition: background .15s;
  }
  .refresh-btn:hover { background: #2563eb; }

  .error-box {
    background: #450a0a;
    border: 1px solid #7f1d1d;
    border-radius: 10px;
    padding: 16px 20px;
    color: #fca5a5;
    font-size: 0.9rem;
    margin-bottom: 20px;
  }

  .card {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
    overflow: hidden;
    margin-bottom: 20px;
  }
  .card-title {
    padding: 14px 20px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: .08em;
    text-transform: uppercase;
    color: #64748b;
    border-bottom: 1px solid #334155;
    background: #162032;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }
  tr.sep td {
    padding: 0;
    height: 1px;
    background: #334155;
  }
  tr:not(.sep):hover td { background: #243044; }

  td {
    padding: 11px 20px;
    font-size: 0.9rem;
    border-bottom: 1px solid #1e293b;
  }
  tr:last-child td { border-bottom: none; }

  td.label {
    color: #94a3b8;
    width: 55%;
  }
  td.label .icon { margin-right: 6px; }

  td.value {
    color: #f1f5f9;
    font-weight: 600;
    text-align: right;
    width: 45%;
  }
  td.value.highlight { color: #38bdf8; }
  td.value.done      { color: #22c55e; }

  .bar-wrap {
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .bar {
    height: 8px;
    border-radius: 4px;
    min-width: 2px;
    flex-shrink: 0;
    transition: width .3s;
  }
  .bar-label {
    font-size: 0.85rem;
    font-weight: 600;
    color: #f1f5f9;
    white-space: nowrap;
  }

  footer {
    text-align: center;
    font-size: 0.78rem;
    color: #475569;
    margin-top: 12px;
  }
</style>
</head>
<body>
<div class="container">

  <header>
    <a class="refresh-btn" href="">↻ Обновить</a>
    <h1>Статистика парсинга соцсетей</h1>
    <div class="sub">Сгенерировано: <?= $generatedAt ?></div>
    <div class="db-badge"><?= DB_NAME ?> › <?= DB_TABLE ?></div>
  </header>

<?php if ($error): ?>
  <div class="error-box">
    <strong>Ошибка подключения к БД:</strong><br><?= htmlspecialchars($error) ?>
  </div>
<?php else: ?>

<?php
// Split rows into sections by separator '---'
$sections = [];
$current  = [];
$sectionTitles = ['Прогресс обработки', 'Найдено контактов', '% от обработанных', 'Итог'];
$sIdx = 0;

foreach ($rows as $row) {
    if ($row[0] === '---') {
        $sections[] = $current;
        $current = [];
        $sIdx++;
    } else {
        $current[] = $row;
    }
}
if (!empty($current)) {
    $sections[] = $current;
}
?>

<?php foreach ($sections as $i => $section): ?>
<?php if (empty($section)) continue; ?>
<div class="card">
  <div class="card-title"><?= $sectionTitles[$i] ?? ('Раздел ' . ($i + 1)) ?></div>
  <table>
  <?php foreach ($section as $row): ?>
    <?php
      [$metric, $value] = $row;
      $icon = $icons[$metric] ?? '';
      $isBar = isPercent($value);
      $extraClass = '';
      if ($metric === '% обработано' || str_ends_with($metric, '%')) $extraClass = 'highlight';
      if ($metric === 'Обработано') $extraClass = 'done';
    ?>
    <tr>
      <td class="label">
        <?php if ($icon): ?><span class="icon"><?= $icon ?></span><?php endif; ?>
        <?= htmlspecialchars($metric) ?>
      </td>
      <td class="value <?= $extraClass ?>">
        <?php if ($isBar): ?>
          <?= percentBar($value) ?>
        <?php else: ?>
          <?= htmlspecialchars($value) ?>
        <?php endif; ?>
      </td>
    </tr>
  <?php endforeach; ?>
  </table>
</div>
<?php endforeach; ?>

<?php endif; ?>

  <footer>udemy_leads › <?= DB_TABLE ?> · stats_social.php</footer>
</div>
</body>
</html>
