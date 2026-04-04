<?php
/**
 * Очистка и дедупликация колонки instagram_parcing из таблицы leads_copy.
 *
 * Обрабатывает:
 *  - Дублированные URL: https://www.instagram.com/https://www.instagram.com/user → https://www.instagram.com/user
 *  - Ссылки через запятую — разбивает на отдельные
 *  - Убирает query-параметры (?igsh=..., ?hl=..., ?utm_source=..., ?next=...)
 *  - Убирает фрагменты (#)
 *  - Убирает trailing слэш для единообразия
 *  - Убирает обратные слэши
 *  - Фильтрует «пустые» ссылки (просто https://www.instagram.com без username)
 *  - Фильтрует ссылки на threads.com и прочий мусор
 *  - Приводит к нижнему регистру (Instagram case-insensitive)
 *  - Удаляет дубли
 *
 * Результат: instagram_clean.txt — по одной ссылке на строку, без дублей.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

define('OUTPUT_FILE', __DIR__ . '/instagram_clean.txt');

require_once __DIR__ . '/instagram_normalize.php';

$dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
$pdo = new PDO($dsn, DB_USER, DB_PASS, [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_COLUMN,
]);

$rows = $pdo->query("
    SELECT `instagram_parcing`
    FROM `" . DB_TABLE . "`
    WHERE `instagram_parcing` IS NOT NULL
      AND `instagram_parcing` != ''
")->fetchAll();

echo "Загружено строк из БД: " . count($rows) . "\n";

$allLinks = [];
$skipped  = [];

foreach ($rows as $raw) {
    $parts = preg_split('/\s*,\s*/', trim($raw));

    foreach ($parts as $chunk) {
        // Разбиваем склеенные ссылки: ...userhttps://www.instagram.com/...
        $urls = preg_split('#(?<=.)(?=https?://)#i', $chunk);

        foreach ($urls as $url) {
            $url = trim($url);
            if ($url === '') continue;

            $cleaned = normalizeInstagramUrl($url);
            if ($cleaned === null) {
                $skipped[] = $url;
                continue;
            }

            $allLinks[] = $cleaned;
        }
    }
}

$unique = array_values(array_unique($allLinks));
sort($unique, SORT_STRING);

file_put_contents(OUTPUT_FILE, implode("\n", $unique) . "\n");

echo "Всего ссылок (после разбиения по запятой): " . count($allLinks) . "\n";
echo "Уникальных:                                " . count($unique) . "\n";
echo "Пропущено (мусор/пустые):                  " . count($skipped) . "\n";
echo "Сохранено в: " . OUTPUT_FILE . "\n";

if (!empty($skipped)) {
    $skippedUnique = array_unique($skipped);
    echo "\nПропущенные (" . count($skippedUnique) . " шт):\n";
    foreach ($skippedUnique as $s) {
        echo "  · $s\n";
    }
}
