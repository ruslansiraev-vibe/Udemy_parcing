<?php
/**
 * validate_leads.php — валидация принадлежности контактных данных через Claude Haiku.
 *
 * Для каждого лида из leads_copy отправляет в Anthropic API (Claude Haiku):
 *   - имя/фамилию инструктора
 *   - email, сайт, Instagram, LinkedIn
 * Claude оценивает, принадлежат ли эти данные одному человеку / его компании.
 *
 * Usage:
 *   php validate_leads.php                          # все непроверенные лиды с email
 *   php validate_leads.php --limit=50               # ограничить выборку
 *   php validate_leads.php --rowid=12345            # один лид по _rowid
 *   php validate_leads.php --revalidate             # повторная проверка уже проверенных
 *   php validate_leads.php --stats                  # статистика валидации
 *   php validate_leads.php --from=100 --to=500      # диапазон по порядковому номеру
 *   php validate_leads.php --dry-run                # показать данные без отправки в API
 */

// ── Load .env ────────────────────────────────────────────────────────────────

$envFile = __DIR__ . '/.env';
if (file_exists($envFile)) {
    foreach (file($envFile, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
        if (str_starts_with(trim($line), '#')) continue;
        if (str_contains($line, '=')) putenv(trim($line));
    }
}

// ── Config ───────────────────────────────────────────────────────────────────

define('DB_HOST',  '127.0.0.1');
define('DB_PORT',  3306);
define('DB_NAME',  'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER',  'root');
define('DB_PASS',  '');

define('ANTHROPIC_KEY', getenv('ANTHROPIC_KEY') ?: '');
define('MODEL_HAIKU',   'claude-haiku-4-5-20251001');

define('REQUEST_TIMEOUT', 60);
define('DELAY_MS',        300);
define('FETCH_BATCH_SIZE', 100);

// ── Valid enum values for response normalization ─────────────────────────────

const MATCH_VALUES   = ['yes', 'no', 'likely', 'unclear', 'not_applicable'];
const VERDICT_VALUES = ['valid', 'suspicious', 'mismatch', 'insufficient_data'];

// ── Prompt ───────────────────────────────────────────────────────────────────

const VALIDATION_SYSTEM_PROMPT = <<<'PROMPT'
Ты — эксперт по верификации цифровых данных. Тебе дают набор контактных данных одного человека (инструктора онлайн-курсов), собранных из разных источников. Твоя задача — оценить, действительно ли каждый контакт принадлежит этому человеку или его компании.

Правила анализа:

1. Email: Проверь, совпадает ли домен email с доменом сайта. Проверь, содержит ли email части имени/фамилии. Бесплатные провайдеры (gmail, yahoo, hotmail) — нейтральный сигнал, не ошибка.

2. Сайт: Оцени, может ли сайт принадлежать этому человеку или его компании. Проверь, логически ли связан домен с именем человека или его сферой деятельности.

3. Instagram: Проверь, похож ли username на имя человека, его бренд или бизнес. Username может быть сокращением или брендовым названием.

4. LinkedIn: Проверь, содержит ли URL имя человека или его компании.

5. Перекрёстная проверка: Оцени общую согласованность — если домен email совпадает с сайтом, это сильный положительный сигнал. Если Instagram-username содержит имя — тоже.

Формат ответа — строго JSON без комментариев:

{
  "email_match": "yes|no|likely|unclear|not_applicable",
  "email_reason": "краткое объяснение",
  "website_match": "yes|no|likely|unclear|not_applicable",
  "website_reason": "краткое объяснение",
  "instagram_match": "yes|no|likely|unclear|not_applicable",
  "instagram_reason": "краткое объяснение",
  "linkedin_match": "yes|no|likely|unclear|not_applicable",
  "linkedin_reason": "краткое объяснение",
  "overall_confidence": 0.0,
  "overall_verdict": "valid|suspicious|mismatch|insufficient_data",
  "summary": "общий вывод одним предложением"
}

Правила оценки:
- "yes" — данные явно совпадают (домен email = домен сайта, username содержит имя)
- "likely" — высокая вероятность совпадения, но нет 100% уверенности
- "unclear" — недостаточно информации для вывода
- "no" — данные явно не совпадают или принадлежат другому человеку
- "not_applicable" — поле пустое или отсутствует
- overall_confidence: 0.0 = полное несовпадение, 1.0 = все данные идеально согласованы
PROMPT;

// ── DB ───────────────────────────────────────────────────────────────────────

require_once __DIR__ . '/validate_migrate.php';

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

function ensureColumns(): void
{
    $added = validate_ensure_columns(getDb(), DB_TABLE);
    foreach ($added as $name) {
        echo "  + Added column: {$name}\n";
    }
}

// ── HTTP helper ──────────────────────────────────────────────────────────────

function curlPost(string $url, string $body, array $headers): string|false
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST           => true,
        CURLOPT_POSTFIELDS     => $body,
        CURLOPT_HTTPHEADER     => $headers,
        CURLOPT_TIMEOUT        => REQUEST_TIMEOUT,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_SSL_VERIFYHOST => 0,
        CURLOPT_ENCODING       => '',
    ]);
    $result = curl_exec($ch);
    $err = curl_error($ch);
    curl_close($ch);
    if ($err) {
        fwrite(STDERR, "  [cURL] {$err}\n");
        return false;
    }
    return $result;
}

// ── Build prompt for one lead ────────────────────────────────────────────────

function buildValidationPrompt(array $row): string
{
    $instructor = trim($row['instructor'] ?? '');
    $email      = trim($row['email_parcing'] ?? '');
    $website    = trim($row['website_parcing'] ?? '');
    $instagram  = trim($row['instagram_parcing'] ?? '');
    $linkedin   = trim($row['linkedin_parcing'] ?? '');

    $sentinels = ['NOT_FOUND', 'JS_REQUIRED', 'SOCIAL_URL', 'UNAVAILABLE', 'INVALID_URL'];
    foreach ($sentinels as $s) {
        if (strcasecmp($email, $s) === 0) $email = '';
    }
    if (preg_match('/^(HTTP_|ERROR:|RETRY:)/i', $email)) $email = '';

    $lines = [];
    $lines[] = "Проверь соответствие контактных данных этому человеку:";
    $lines[] = "";
    $lines[] = "- Имя инструктора: " . ($instructor ?: '(не указано)');
    $lines[] = "- Email: " . ($email ?: '(нет)');
    $lines[] = "- Сайт: " . ($website ?: '(нет)');
    $lines[] = "- Instagram: " . ($instagram ?: '(нет)');
    $lines[] = "- LinkedIn: " . ($linkedin ?: '(нет)');
    $lines[] = "";
    $lines[] = "Оцени, принадлежат ли email, сайт, Instagram и LinkedIn этому человеку или его компании.";
    $lines[] = "Верни строго JSON по схеме из инструкций.";

    return implode("\n", $lines);
}

// ── Call Claude Haiku ────────────────────────────────────────────────────────

function callValidation(string $promptText): ?array
{
    if (ANTHROPIC_KEY === '') {
        fwrite(STDERR, "  [Claude] ANTHROPIC_KEY не задан в .env\n");
        return null;
    }

    $payload = json_encode([
        'model'      => MODEL_HAIKU,
        'max_tokens' => 800,
        'system'     => [[
            'type'          => 'text',
            'text'          => VALIDATION_SYSTEM_PROMPT,
            'cache_control' => ['type' => 'ephemeral'],
        ]],
        'messages' => [[
            'role'    => 'user',
            'content' => $promptText,
        ]],
    ], JSON_UNESCAPED_UNICODE);

    $headers = [
        'Content-Type: application/json',
        'x-api-key: ' . ANTHROPIC_KEY,
        'anthropic-version: 2023-06-01',
    ];

    $body = curlPost('https://api.anthropic.com/v1/messages', $payload, $headers);
    if ($body === false) {
        fwrite(STDERR, "  [Claude] cURL failed\n");
        return null;
    }

    $resp = @json_decode($body, true);
    if (!is_array($resp) || !empty($resp['error'])) {
        fwrite(STDERR, "  [Claude] API error: " . substr($body, 0, 300) . "\n");
        return null;
    }

    $raw = trim($resp['content'][0]['text'] ?? '');
    $raw = preg_replace('/^```(?:json)?\s*/i', '', $raw);
    $raw = preg_replace('/\s*```\s*$/', '', $raw);
    $raw = trim($raw);

    $start = strpos($raw, '{');
    $end   = strrpos($raw, '}');
    if ($start === false || $end === false || $end <= $start) {
        fwrite(STDERR, "  [Claude] No JSON in response: " . substr($raw, 0, 300) . "\n");
        return null;
    }

    $result = @json_decode(substr($raw, $start, $end - $start + 1), true);
    if (!is_array($result)) {
        fwrite(STDERR, "  [Claude] JSON parse failed: " . substr($raw, 0, 300) . "\n");
        return null;
    }
    return $result;
}

// ── Normalize Claude response to safe enum values ────────────────────────────

function pickEnum(string $value, array $allowed, string $fallback): string
{
    $v = strtolower(trim($value));
    return in_array($v, $allowed, true) ? $v : $fallback;
}

function normalizeValidationResult(array $raw): array
{
    return [
        'email_match'      => pickEnum($raw['email_match'] ?? '', MATCH_VALUES, 'unclear'),
        'email_reason'     => mb_substr(trim($raw['email_reason'] ?? ''), 0, 500),
        'website_match'    => pickEnum($raw['website_match'] ?? '', MATCH_VALUES, 'unclear'),
        'website_reason'   => mb_substr(trim($raw['website_reason'] ?? ''), 0, 500),
        'instagram_match'  => pickEnum($raw['instagram_match'] ?? '', MATCH_VALUES, 'unclear'),
        'instagram_reason' => mb_substr(trim($raw['instagram_reason'] ?? ''), 0, 500),
        'linkedin_match'   => pickEnum($raw['linkedin_match'] ?? '', MATCH_VALUES, 'unclear'),
        'linkedin_reason'  => mb_substr(trim($raw['linkedin_reason'] ?? ''), 0, 500),
        'confidence'       => max(0.0, min(1.0, (float)($raw['overall_confidence'] ?? 0))),
        'verdict'          => pickEnum($raw['overall_verdict'] ?? '', VERDICT_VALUES, 'insufficient_data'),
        'summary'          => mb_substr(trim($raw['summary'] ?? ''), 0, 1000),
    ];
}

// ── Save result to DB ────────────────────────────────────────────────────────

function saveValidation(int $rowid, array $norm, array $rawJson): void
{
    $pdo = getDb();
    $pdo->prepare("
        UPDATE `" . DB_TABLE . "` SET
            `validate_email_match`     = ?,
            `validate_email_reason`    = ?,
            `validate_website_match`   = ?,
            `validate_website_reason`  = ?,
            `validate_instagram_match` = ?,
            `validate_instagram_reason`= ?,
            `validate_linkedin_match`  = ?,
            `validate_linkedin_reason` = ?,
            `validate_confidence`      = ?,
            `validate_verdict`         = ?,
            `validate_summary`         = ?,
            `validate_raw_json`        = ?,
            `validated_at`             = NOW()
        WHERE `_rowid` = ?
    ")->execute([
        $norm['email_match'],
        $norm['email_reason'] ?: null,
        $norm['website_match'],
        $norm['website_reason'] ?: null,
        $norm['instagram_match'],
        $norm['instagram_reason'] ?: null,
        $norm['linkedin_match'],
        $norm['linkedin_reason'] ?: null,
        $norm['confidence'],
        $norm['verdict'],
        $norm['summary'] ?: null,
        json_encode($rawJson, JSON_UNESCAPED_UNICODE),
        $rowid,
    ]);
}

// ── Print one result ─────────────────────────────────────────────────────────

function printResult(array $row, array $norm): void
{
    $sep = str_repeat('─', 60);
    $icon = fn(string $m) => match ($m) {
        'yes'    => '✅',
        'likely' => '🟡',
        'no'     => '❌',
        'not_applicable' => '⬜',
        default  => '❓',
    };

    echo "\n{$sep}\n";
    echo "  Инструктор: " . ($row['instructor'] ?? '?') . "  (rowid={$row['_rowid']})\n";
    echo "{$sep}\n";
    echo "  Email:     {$icon($norm['email_match'])} {$norm['email_match']}";
    echo ($norm['email_reason'] ? " — {$norm['email_reason']}" : '') . "\n";
    echo "  Сайт:      {$icon($norm['website_match'])} {$norm['website_match']}";
    echo ($norm['website_reason'] ? " — {$norm['website_reason']}" : '') . "\n";
    echo "  Instagram: {$icon($norm['instagram_match'])} {$norm['instagram_match']}";
    echo ($norm['instagram_reason'] ? " — {$norm['instagram_reason']}" : '') . "\n";
    echo "  LinkedIn:  {$icon($norm['linkedin_match'])} {$norm['linkedin_match']}";
    echo ($norm['linkedin_reason'] ? " — {$norm['linkedin_reason']}" : '') . "\n";
    echo "\n  Confidence: {$norm['confidence']}  |  Verdict: {$norm['verdict']}\n";
    if ($norm['summary']) echo "  Summary: {$norm['summary']}\n";
    echo "{$sep}\n";
}

// ── Stats ────────────────────────────────────────────────────────────────────

function showStats(): void
{
    $pdo = getDb();
    $total     = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `validated_at` IS NOT NULL")->fetchColumn();
    $valid     = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `validate_verdict` = 'valid'")->fetchColumn();
    $suspicious= (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `validate_verdict` = 'suspicious'")->fetchColumn();
    $mismatch  = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `validate_verdict` = 'mismatch'")->fetchColumn();
    $insuf     = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `validate_verdict` = 'insufficient_data'")->fetchColumn();

    $eligible  = (int)$pdo->query("
        SELECT COUNT(*) FROM `" . DB_TABLE . "`
        WHERE `instructor` IS NOT NULL AND TRIM(`instructor`) != ''
          AND (
            (`email_parcing` IS NOT NULL AND TRIM(`email_parcing`) != ''
             AND `email_parcing` NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
             AND `email_parcing` NOT LIKE 'HTTP_%' AND `email_parcing` NOT LIKE 'ERROR:%' AND `email_parcing` NOT LIKE 'RETRY:%')
            OR (`website_parcing` IS NOT NULL AND TRIM(`website_parcing`) != '')
            OR (`instagram_parcing` IS NOT NULL AND TRIM(`instagram_parcing`) != '')
            OR (`linkedin_parcing` IS NOT NULL AND TRIM(`linkedin_parcing`) != '')
          )
    ")->fetchColumn();

    $bar = str_repeat('═', 50);
    echo "\n{$bar}\n  СТАТИСТИКА ВАЛИДАЦИИ\n{$bar}\n";
    echo "  Подходящих для проверки:  " . number_format($eligible) . "\n";
    echo "  Проверено:               " . number_format($total) . "\n";
    echo "  Осталось:                " . number_format(max(0, $eligible - $total)) . "\n";
    echo "\n  Результаты:\n";
    echo "    valid:             " . number_format($valid) . "\n";
    echo "    suspicious:        " . number_format($suspicious) . "\n";
    echo "    mismatch:          " . number_format($mismatch) . "\n";
    echo "    insufficient_data: " . number_format($insuf) . "\n";

    $avgConf = $pdo->query("SELECT ROUND(AVG(`validate_confidence`), 2) FROM `" . DB_TABLE . "` WHERE `validated_at` IS NOT NULL")->fetchColumn();
    echo "\n  Средний confidence:      " . ($avgConf ?? 'N/A') . "\n";

    $emailStats = $pdo->query("
        SELECT `validate_email_match`, COUNT(*) AS cnt
        FROM `" . DB_TABLE . "`
        WHERE `validated_at` IS NOT NULL
        GROUP BY `validate_email_match`
        ORDER BY cnt DESC
    ")->fetchAll();
    if ($emailStats) {
        echo "\n  Email match:\n";
        foreach ($emailStats as $r) {
            echo "    " . str_pad($r['validate_email_match'] ?? '?', 18) . str_pad($r['cnt'], 6, ' ', STR_PAD_LEFT) . "\n";
        }
    }

    echo "\n  DB: " . DB_NAME . " › " . DB_TABLE . "\n{$bar}\n\n";
}

// ── Validate one lead ────────────────────────────────────────────────────────

function validateLead(array $row, bool $verbose, bool $dryRun): ?array
{
    $rowid      = (int)$row['_rowid'];
    $instructor = trim($row['instructor'] ?? '');

    if ($verbose) {
        echo "\n  ▶ [{$rowid}] {$instructor}";
        echo "  email=" . ($row['email_parcing'] ?? '-');
        echo "  site=" . ($row['website_parcing'] ?? '-');
        echo "  ig=" . ($row['instagram_parcing'] ?? '-');
        echo "  li=" . ($row['linkedin_parcing'] ?? '-');
        echo "\n";
    }

    $promptText = buildValidationPrompt($row);

    if ($dryRun) {
        echo "  [dry-run] Промпт:\n{$promptText}\n";
        return null;
    }

    $rawResult = callValidation($promptText);
    if ($rawResult === null) {
        fwrite(STDERR, "  ✗ [{$rowid}] {$instructor} — Claude не вернул результат\n");
        return null;
    }

    $norm = normalizeValidationResult($rawResult);
    saveValidation($rowid, $norm, $rawResult);

    if ($verbose) {
        printResult($row, $norm);
    }

    return $norm;
}

// ── CLI args ─────────────────────────────────────────────────────────────────

$limit        = 0;
$singleRowid  = 0;
$showStatsOnly = false;
$revalidate   = false;
$dryRun       = false;
$rangeFrom    = 0;
$rangeTo      = 0;

foreach ($argv as $arg) {
    if (preg_match('/^--limit=(\d+)$/',  $arg, $m)) $limit       = (int)$m[1];
    if (preg_match('/^--rowid=(\d+)$/',  $arg, $m)) $singleRowid = (int)$m[1];
    if (preg_match('/^--from=(\d+)$/',   $arg, $m)) $rangeFrom   = (int)$m[1];
    if (preg_match('/^--to=(\d+)$/',     $arg, $m)) $rangeTo     = (int)$m[1];
    if ($arg === '--stats')       $showStatsOnly = true;
    if ($arg === '--revalidate')  $revalidate    = true;
    if ($arg === '--dry-run')     $dryRun        = true;
}

// ── Init ─────────────────────────────────────────────────────────────────────

$pdo = getDb();
ensureColumns();

if ($showStatsOnly) {
    showStats();
    exit(0);
}

if (ANTHROPIC_KEY === '' && !$dryRun) {
    fwrite(STDERR, "ANTHROPIC_KEY пустой. Задайте в .env файле.\n");
    exit(1);
}

// ── Single row mode ──────────────────────────────────────────────────────────

if ($singleRowid > 0) {
    $stmt = $pdo->prepare("
        SELECT `_rowid`, `instructor`, `email_parcing`, `website_parcing`,
               `instagram_parcing`, `linkedin_parcing`
        FROM `" . DB_TABLE . "`
        WHERE `_rowid` = ?
    ");
    $stmt->execute([$singleRowid]);
    $row = $stmt->fetch();

    if (!$row) {
        echo "  ❌ Строка _rowid={$singleRowid} не найдена\n";
        exit(1);
    }

    $result = validateLead($row, true, $dryRun);
    exit($result === null && !$dryRun ? 1 : 0);
}

// ── Batch mode ───────────────────────────────────────────────────────────────

$eligibleCondition = "
    `instructor` IS NOT NULL AND TRIM(`instructor`) != ''
    AND (
        (`email_parcing` IS NOT NULL AND TRIM(`email_parcing`) != ''
         AND `email_parcing` NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
         AND `email_parcing` NOT LIKE 'HTTP_%' AND `email_parcing` NOT LIKE 'ERROR:%' AND `email_parcing` NOT LIKE 'RETRY:%')
        OR (`website_parcing` IS NOT NULL AND TRIM(`website_parcing`) != '')
        OR (`instagram_parcing` IS NOT NULL AND TRIM(`instagram_parcing`) != '')
        OR (`linkedin_parcing` IS NOT NULL AND TRIM(`linkedin_parcing`) != '')
    )
";

if (!$revalidate) {
    $eligibleCondition .= " AND `validated_at` IS NULL";
}

if ($rangeFrom > 0) {
    $eligibleCondition .= " AND `_rowid` >= {$rangeFrom}";
}
if ($rangeTo > 0) {
    $eligibleCondition .= " AND `_rowid` <= {$rangeTo}";
}

$totalStmt = $pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE {$eligibleCondition}");
$total = (int)$totalStmt->fetchColumn();

echo str_repeat('═', 50) . "\n";
echo "  Валидация контактных данных через Claude Haiku\n";
echo "  Лидов к проверке: " . number_format($total) . "\n";
if ($limit > 0) echo "  Лимит: {$limit}\n";
if ($dryRun) echo "  Режим: DRY-RUN (без отправки в API)\n";
echo str_repeat('═', 50) . "\n\n";

if ($total === 0) {
    echo "  Нет лидов для проверки.\n";
    exit(0);
}

$offset   = 0;
$processed = 0;
$success   = 0;
$failed    = 0;
$verdicts  = ['valid' => 0, 'suspicious' => 0, 'mismatch' => 0, 'insufficient_data' => 0];

while (true) {
    $batchLimit = FETCH_BATCH_SIZE;
    if ($limit > 0) {
        $remaining = $limit - $processed;
        if ($remaining <= 0) break;
        $batchLimit = min($batchLimit, $remaining);
    }

    $sql = "
        SELECT `_rowid`, `instructor`, `email_parcing`, `website_parcing`,
               `instagram_parcing`, `linkedin_parcing`
        FROM `" . DB_TABLE . "`
        WHERE {$eligibleCondition}
        ORDER BY `_rowid` ASC
        LIMIT {$batchLimit} OFFSET {$offset}
    ";
    $rows = $pdo->query($sql)->fetchAll();
    if (empty($rows)) break;

    foreach ($rows as $row) {
        $processed++;
        $norm = validateLead($row, true, $dryRun);

        if ($norm !== null) {
            $success++;
            $v = $norm['verdict'];
            if (isset($verdicts[$v])) $verdicts[$v]++;
        } else {
            if (!$dryRun) $failed++;
        }

        if ($limit > 0 && $processed >= $limit) break;

        if (!$dryRun) {
            usleep(DELAY_MS * 1000);
        }
    }

    $offset += count($rows);
    if (count($rows) < $batchLimit) break;
}

// ── Summary ──────────────────────────────────────────────────────────────────

$bar = str_repeat('═', 50);
echo "\n{$bar}\n  ИТОГО\n{$bar}\n";
echo "  Обработано:  {$processed}\n";
echo "  Успешно:     {$success}\n";
echo "  Ошибок:      {$failed}\n";
if (!$dryRun) {
    echo "\n  Вердикты:\n";
    foreach ($verdicts as $v => $cnt) {
        echo "    " . str_pad($v, 20) . "{$cnt}\n";
    }
}
echo "{$bar}\n";
