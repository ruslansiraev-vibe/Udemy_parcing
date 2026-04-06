<?php
/**
 * Проверка ANTHROPIC_KEY из .env и одного запроса к api.anthropic.com
 * Запуск на сервере: php check_anthropic.php
 */
$envFile = __DIR__ . '/.env';
if (is_readable($envFile)) {
    foreach (file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
        if (str_starts_with(trim($line), '#')) {
            continue;
        }
        if (str_contains($line, '=')) {
            putenv(trim($line));
        }
    }
}
$key = getenv('ANTHROPIC_KEY') ?: '';
if ($key === '') {
    fwrite(STDERR, "ANTHROPIC_KEY пустой или нет .env в " . $envFile . "\n");
    exit(1);
}
echo 'Ключ: длина ' . strlen($key) . ', префикс ' . substr($key, 0, 15) . "...\n";

$payload = json_encode([
    'model'       => 'claude-haiku-4-5-20251001',
    'max_tokens'  => 20,
    'messages'    => [['role' => 'user', 'content' => 'Reply with exactly: ok']],
], JSON_UNESCAPED_UNICODE);

$ch = curl_init('https://api.anthropic.com/v1/messages');
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_POST           => true,
    CURLOPT_POSTFIELDS     => $payload,
    CURLOPT_HTTPHEADER     => [
        'Content-Type: application/json',
        'x-api-key: ' . $key,
        'anthropic-version: 2023-06-01',
    ],
    CURLOPT_TIMEOUT        => 25,
]);
$body = curl_exec($ch);
$code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
$err  = curl_error($ch);
curl_close($ch);

echo "HTTP: {$code}\n";
if ($err !== '') {
    echo "cURL: {$err}\n";
}
if ($code === 200) {
    echo "Anthropic API: подключение OK\n";
    exit(0);
}
echo $body . "\n";
exit(2);
