<?php
/**
 * Экспорт строк leads_copy: заполнены instagram_parcing и email_parcing.
 * В CSV только instagram (username без https://www.instagram.com/) и email_parcing.
 * Несколько профилей в ячейке — отдельная строка на каждый, email_parcing повторяется.
 * Результат: instagram_email_parcing.csv
 */

require_once __DIR__ . '/instagram_normalize.php';

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');
define('OUTPUT_FILE', __DIR__ . '/instagram_email_parcing.csv');

$dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
$pdo = new PDO($dsn, DB_USER, DB_PASS, [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

$sql = "
    SELECT `instagram_parcing`, `email_parcing`
    FROM `" . DB_TABLE . "`
    WHERE TRIM(COALESCE(`instagram_parcing`, '')) != ''
      AND TRIM(COALESCE(`email_parcing`, '')) != ''
    ORDER BY `_rowid` ASC
";

$stmt = $pdo->query($sql);
$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

$fh = fopen(OUTPUT_FILE, 'w');
if ($fh === false) {
    fwrite(STDERR, "Cannot write: " . OUTPUT_FILE . "\n");
    exit(1);
}

$csvOpts = [',', '"', '\\'];
fputcsv($fh, ['instagram', 'email_parcing'], ...$csvOpts);

$csvRows = 0;
foreach ($rows as $r) {
    $rawIg = (string) ($r['instagram_parcing'] ?? '');
    $urls  = normalizeInstagramFieldToUrls($rawIg);

    if ($urls === []) {
        $name = instagramRawToUsername($rawIg);
        if ($name === '') {
            continue;
        }
        fputcsv($fh, [$name, $r['email_parcing']], ...$csvOpts);
        $csvRows++;
        continue;
    }

    foreach ($urls as $ig) {
        $name = instagramNormalizedUrlToUsername($ig);
        if ($name === '') {
            continue;
        }
        fputcsv($fh, [$name, $r['email_parcing']], ...$csvOpts);
        $csvRows++;
    }
}
fclose($fh);

echo "Сохранено: " . OUTPUT_FILE . "\n";
echo "Строк в БД (исходных): " . count($rows) . "\n";
echo "Строк в CSV (развёрнуто): " . $csvRows . "\n";
