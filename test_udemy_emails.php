<?php
/**
 * Тестовый скрипт — проверка email по первым 10 инструкторам Udemy
 * Без записи в базу данных, только анализ и вывод.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

try {
    $pdo = new PDO(
        "mysql:host=".DB_HOST.";port=".DB_PORT.";dbname=".DB_NAME.";charset=utf8mb4", 
        DB_USER, 
        DB_PASS,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );

    echo "=== Тестовый сбор email Udemy (первые 10 инструкторов) ===\n\n";

    $sql = "
        SELECT _rowid, instructor, 
               UdemyProfile1_parcing as p1,
               UdemyProfile2_parcing as p2,
               UdemyProfile3_parcing as p3
        FROM `" . DB_TABLE . "` 
        WHERE (UdemyProfile1_parcing IS NOT NULL AND UdemyProfile1_parcing != '' 
           OR UdemyProfile2_parcing IS NOT NULL AND UdemyProfile2_parcing != ''
           OR UdemyProfile3_parcing IS NOT NULL AND UdemyProfile3_parcing != '')
          AND UdemyProfile1_parcing NOT IN ('SKIP','NOT_FOUND','HTTP_ERROR')
        ORDER BY _rowid ASC 
        LIMIT 10
    ";

    $stmt = $pdo->query($sql);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

    echo "Найдено записей для теста: " . count($rows) . "\n\n";

    function extractId(string $url): ?string {
        if (preg_match('#/(?:user|instructor|users)/([a-zA-Z0-9_-]+)#i', $url, $m)) {
            return $m[1];
        }
        return null;
    }

    $totalFound = 0;

    foreach ($rows as $row) {
        $rowid = $row['_rowid'];
        $name = trim($row['instructor'] ?: 'Без имени');
        
        echo "[$rowid] $name\n";
        
        $profiles = array_filter([$row['p1'], $row['p2'], $row['p3']]);
        $foundInRow = 0;

        foreach ($profiles as $url) {
            if (empty($url)) continue;
            
            $id = extractId($url);
            if (!$id) {
                echo "   ↳ Профиль: $url (не удалось извлечь ID)\n";
                continue;
            }
            
            echo "   ↳ Профиль: $url\n";
            echo "      ID: $id\n";
            
            // Генерируем возможные email
            $candidates = [
                strtolower($id) . '@udemy.com',
                str_replace(['-', '_'], '.', strtolower($id)) . '@udemy.com',
                str_replace(['-', '_'], '', strtolower($id)) . '@udemy.com',
            ];
            
            foreach ($candidates as $email) {
                if (filter_var($email, FILTER_VALIDATE_EMAIL)) {
                    echo "      ✉️  $email  (guessed)\n";
                    $foundInRow++;
                    $totalFound++;
                }
            }
        }
        
        if ($foundInRow === 0) {
            echo "      Нет предположений по email\n";
        }
        echo str_repeat('─', 80) . "\n";
    }

    echo "\n=== ИТОГО ===\n";
    echo "Проанализировано инструкторов: " . count($rows) . "\n";
    echo "Найдено потенциальных email: $totalFound\n";
    echo "\nДля полноценного парсинга через API запустите:\n";
    echo "   php v1_scrape_emails.php\n";

} catch (Exception $e) {
    echo "Ошибка: " . $e->getMessage() . "\n";
}
?>
