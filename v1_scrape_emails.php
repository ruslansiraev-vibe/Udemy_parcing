<?php
/**
 * v1_scrape_emails.php
 * 
 * Реальный сбор email через Udemy API + клиентский код
 * Делает настоящие запросы, ничего не придумывает.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

$pdo = new PDO("mysql:host=".DB_HOST.";port=".DB_PORT.";dbname=".DB_NAME.";charset=utf8mb4", DB_USER, DB_PASS);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

echo "=== v1 Udemy Email Scraper (реальный) ===\n";
echo "Проверяем первые 5 инструкторов...\n\n";

function extractId(string $url): ?string {
    if (preg_match('#/(?:user|instructor)/([a-zA-Z0-9_-]+)#i', $url, $m)) {
        return $m[1];
    }
    return null;
}

function fetchUrl(string $url): array {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_TIMEOUT => 10,
        CURLOPT_USERAGENT => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        CURLOPT_HTTPHEADER => [
            'Accept: application/json, text/plain, */*',
            'Accept-Language: en-US,en;q=0.9',
            'Referer: https://www.udemy.com/',
        ]
    ]);
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);
    
    return [
        'code' => $httpCode,
        'body' => $response ?: '',
        'error' => $error
    ];
}

// Берем первые 5 записей
$stmt = $pdo->query("
    SELECT _rowid, instructor, UdemyProfile1_parcing 
    FROM `" . DB_TABLE . "` 
    WHERE UdemyProfile1_parcing IS NOT NULL 
      AND UdemyProfile1_parcing LIKE '%udemy.com/user/%'
    ORDER BY _rowid ASC 
    LIMIT 5
");

$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

if (empty($rows)) {
    echo "Нет записей с Udemy профилями.\n";
    exit;
}

foreach ($rows as $row) {
    $name = trim($row['instructor'] ?: 'Без имени');
    $profileUrl = $row['UdemyProfile1_parcing'];
    $id = extractId($profileUrl);
    
    echo "📌 Инструктор: $name\n";
    echo "🔗 Профиль: $profileUrl\n";
    
    if ($id) {
        echo "🆔 ID: $id\n\n";
        
        $testUrls = [
            "https://www.udemy.com/api-2.0/users/{$id}/" => "Users API",
            "https://www.udemy.com/api-2.0/instructors/{$id}/" => "Instructors API",
        ];
        
        foreach ($testUrls as $url => $label) {
            echo "Запрос → $label\n";
            $result = fetchUrl($url);
            
            echo "   Код: {$result['code']}\n";
            
            if ($result['code'] === 200) {
                $hasEmail = preg_match('/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/', $result['body'], $m);
                if ($hasEmail) {
                    echo "   ✅ Найден email: " . $m[0] . "\n";
                } else {
                    echo "   Нет email в ответе\n";
                }
                
                if (str_contains($result['body'], 'email')) {
                    echo "   Упоминание 'email' в ответе\n";
                }
            } elseif ($result['code'] === 403) {
                echo "   ⛔ 403 Forbidden (Cloudflare / блокировка)\n";
            } elseif ($result['code'] === 404) {
                echo "   404 Not Found\n";
            } else {
                echo "   Ответ: {$result['code']}\n";
            }
            echo "\n";
        }
    } else {
        echo "   Не удалось извлечь ID\n";
    }
    
    echo str_repeat('═', 90) . "\n\n";
}

echo "Тест завершён.\n";
echo "Udemy очень хорошо защищает свои API. Результаты помогут понять, насколько это реально.\n";
?>
