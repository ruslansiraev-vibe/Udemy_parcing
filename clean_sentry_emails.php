<?php
/**
 * Очистка email_parcing: удаление sentry-адресов и исправление слипшихся записей.
 *
 * Что делает:
 *   1. Убирает адреса с доменами *sentry*, *wixpress*, *sentry.io
 *   2. Разделяет «слипшиеся» записи: email@gmail.comfirst → email@gmail.com
 *   3. Убирает шаблонные Wix-заглушки: example@mysite.com, info@mysite.com и т.п.
 *   4. Убирает явно невалидные «email» (без @, файлы .png/.jpg и т.д.)
 *   5. Если после очистки ничего не осталось — ставит NULL
 *
 * Режим --dry-run (по умолчанию): только показывает что изменится.
 * Режим --apply: реально обновляет базу.
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_TABLE', 'leads_copy');
define('DB_USER', 'root');
define('DB_PASS', '');

$dryRun = !in_array('--apply', $argv, true);

$dsn = "mysql:host=" . DB_HOST . ";port=" . DB_PORT . ";dbname=" . DB_NAME . ";charset=utf8mb4";
$pdo = new PDO($dsn, DB_USER, DB_PASS, [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
]);

$junkDomainPatterns = [
    'sentry-next.wixpress.com',
    'sentry.wixpress.com',
    'ingest.sentry.io',
    'mysite.com',
    'mysite.co.il',
    'yourcompany.example.com',
];

function isDomainJunk(string $domain): bool
{
    global $junkDomainPatterns;
    $domain = strtolower($domain);
    foreach ($junkDomainPatterns as $pattern) {
        if ($domain === $pattern || str_ends_with($domain, '.' . $pattern)) {
            return true;
        }
    }
    return false;
}

/**
 * Известные TLD для точного отсечения хвоста.
 */
function validTlds(): array
{
    static $set = null;
    if ($set === null) {
        $set = array_flip(explode(' ',
            'com net org io co edu gov info biz pro xyz app dev me us tv cc ' .
            'ai de fr es it nl be at ch se no dk fi pl cz sk hu ro bg hr si ' .
            'ru ua by kz br ar mx cl pe au nz jp kr cn in sg hk tw th my id ph vn ' .
            'za ng ke eg ma tn ae sa il tr eu uk cat la ly to fm im ' .
            'club online store shop site tech academy school training consulting ' .
            'solutions agency studio design earth mobi tel travel jobs aero coop museum'
        ));
    }
    return $set;
}

/**
 * Двухуровневые ccTLD.
 */
function twoLevelTlds(): array
{
    static $set = null;
    if ($set === null) {
        $set = array_flip(explode(' ',
            'co.uk co.nz co.za co.in co.il co.kr co.jp co.id ' .
            'com.br com.au com.tr com.mx com.ar com.sg com.hk com.ph com.ua ' .
            'org.uk org.au org.nz org.il ' .
            'net.au net.nz ac.uk ac.nz ac.il'
        ));
    }
    return $set;
}

/**
 * Извлекает чистый email из строки, отсекая слипшийся мусорный хвост.
 *
 * Стратегия: разбираем домен по точкам, ищем самый длинный валидный TLD
 * (сначала двухуровневый, потом одноуровневый), и обрезаем всё после него.
 */
function extractEmailFromString(string $s): ?string
{
    $s = trim($s);
    if ($s === '' || strpos($s, '@') === false) {
        return null;
    }

    // Извлекаем грубый email-кандидат: local@всё_остальное
    if (!preg_match('/([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}.*)/', $s, $m)) {
        return null;
    }

    $raw = strtolower($m[1]);
    $atPos = strpos($raw, '@');
    $local = substr($raw, 0, $atPos);
    $rest  = substr($raw, $atPos + 1);

    // Разбиваем доменную часть по точкам
    $parts = explode('.', $rest);
    if (count($parts) < 2) {
        return null;
    }

    $twoLevel = twoLevelTlds();
    $oneLevel = validTlds();

    // Ищем с конца: пробуем найти самый длинный валидный домен
    // Перебираем все возможные позиции TLD
    $bestEmail = null;

    for ($i = 1; $i < count($parts); $i++) {
        $seg = $parts[$i];

        // Проверяем двухуровневый TLD: parts[i].parts[i+1]
        if ($i + 1 < count($parts)) {
            $nextSeg = $parts[$i + 1];
            // Для двухуровневого: первая часть точная (co, com, org, net, ac),
            // вторая — начинается с валидного ccTLD (2-3 буквы)
            foreach (array_keys($twoLevel) as $twoKey) {
                [$first, $second] = explode('.', $twoKey);
                if ($seg === $first && str_starts_with($nextSeg, $second)) {
                    // Проверяем что после TLD идёт конец или мусор (не ещё одна точка+буквы)
                    $domain = implode('.', array_slice($parts, 0, $i)) . '.' . $twoKey;
                    $bestEmail = $local . '@' . $domain;
                    break;
                }
            }
        }

        // Проверяем одноуровневый TLD
        // Сегмент может быть "comshare" — нужно проверить, начинается ли он с валидного TLD
        foreach (array_keys($oneLevel) as $tld) {
            if ($seg === $tld || (str_starts_with($seg, $tld) && strlen($seg) > strlen($tld))) {
                $domain = implode('.', array_slice($parts, 0, $i)) . '.' . $tld;
                $candidate = $local . '@' . $domain;
                if ($bestEmail === null) {
                    $bestEmail = $candidate;
                }
                break;
            }
        }
    }

    // Предпочитаем двухуровневый TLD — пересканируем если нашли оба
    // (bestEmail уже содержит двухуровневый, если он был найден после одноуровневого)
    // Перезапуск: ищем двухуровневый отдельно
    $twoLevelEmail = null;
    for ($i = 1; $i < count($parts) - 1; $i++) {
        $seg = $parts[$i];
        $nextSeg = $parts[$i + 1];
        foreach (array_keys($twoLevel) as $twoKey) {
            [$first, $second] = explode('.', $twoKey);
            if ($seg === $first && str_starts_with($nextSeg, $second)) {
                $domain = implode('.', array_slice($parts, 0, $i)) . '.' . $twoKey;
                $twoLevelEmail = $local . '@' . $domain;
                break 2;
            }
        }
    }
    if ($twoLevelEmail !== null) {
        $bestEmail = $twoLevelEmail;
    }

    return $bestEmail;
}

/**
 * Из сырой строки с ; разделителями извлекает чистые email-адреса.
 */
function extractCleanEmails(string $raw): array
{
    $parts = explode(';', $raw);
    $result = [];

    foreach ($parts as $part) {
        $email = extractEmailFromString($part);
        if ($email === null) {
            continue;
        }

        $atPos = strpos($email, '@');
        $domain = substr($email, $atPos + 1);

        if (isDomainJunk($domain)) {
            continue;
        }

        if (preg_match('/\.(png|jpg|jpeg|gif|svg|webp|jp)$/i', $email)) {
            continue;
        }

        $junkEmails = ['sample@gmail.com', 'filler@godaddy.com', 'ejemplo@misitio.com'];
        if (in_array($email, $junkEmails, true)) {
            continue;
        }

        // Hex-hash local-part (Sentry DSN keys): 32-char hex @ anything
        $localPart = substr($email, 0, $atPos);
        if (preg_match('/^[0-9a-f]{20,}$/', $localPart)) {
            continue;
        }

        // Sentry ingest domains: xxx@o12345.ingest.sentry.io → parsed as xxx@o12345.in
        if (preg_match('/^o\d+\.in/', $domain)) {
            continue;
        }

        $result[] = $email;
    }

    // Удаляем дубликаты, у которых local-part = мусорный_префикс + local-part другого email
    $cleaned = [];
    $domains = [];
    foreach ($result as $e) {
        $at = strpos($e, '@');
        $domains[$e] = substr($e, $at + 1);
    }

    foreach ($result as $e) {
        $at = strpos($e, '@');
        $localPart = substr($e, 0, $at);
        $domain = $domains[$e];
        $dominated = false;

        foreach ($result as $other) {
            if ($other === $e) continue;
            $otherAt = strpos($other, '@');
            $otherLocal = substr($other, 0, $otherAt);
            $otherDomain = $domains[$other];
            // Если другой email имеет тот же домен и его local-part является суффиксом нашего
            if ($domain === $otherDomain && $localPart !== $otherLocal
                && str_ends_with($localPart, $otherLocal) && strlen($localPart) > strlen($otherLocal)) {
                $dominated = true;
                break;
            }
        }
        if (!$dominated) {
            $cleaned[] = $e;
        }
    }

    return array_values(array_unique($cleaned));
}

$rows = $pdo->query("
    SELECT `_rowid`, `email_parcing`
    FROM `" . DB_TABLE . "`
    WHERE `email_parcing` IS NOT NULL
      AND TRIM(`email_parcing`) != ''
      AND (
        `email_parcing` LIKE '%sentry%'
        OR `email_parcing` LIKE '%wixpress%'
        OR `email_parcing` LIKE '%@mysite.com%'
        OR `email_parcing` LIKE '%@mysite.co.il%'
      )
")->fetchAll();

echo ($dryRun ? "=== DRY RUN ===" : "=== APPLYING ===") . "\n";
echo "Найдено строк для обработки: " . count($rows) . "\n\n";

$updateStmt = $pdo->prepare("
    UPDATE `" . DB_TABLE . "`
    SET `email_parcing` = ?
    WHERE `_rowid` = ?
");

$changed  = 0;
$emptied  = 0;
$noChange = 0;

foreach ($rows as $row) {
    $rowid    = (int) $row['_rowid'];
    $original = $row['email_parcing'];
    $clean    = extractCleanEmails($original);
    $newValue = $clean ? implode(';', $clean) : null;

    if ($newValue === $original) {
        $noChange++;
        continue;
    }

    $changed++;
    if ($newValue === null) {
        $emptied++;
    }

    $origShort = mb_strimwidth($original, 0, 100, '…');
    $newShort  = $newValue !== null ? mb_strimwidth($newValue, 0, 100, '…') : 'NULL';
    echo "rowid=$rowid\n";
    echo "  OLD: $origShort\n";
    echo "  NEW: $newShort\n\n";

    if (!$dryRun) {
        $updateStmt->execute([$newValue, $rowid]);
    }
}

echo "---\n";
echo "Обработано строк: " . count($rows) . "\n";
echo "Изменено: $changed\n";
echo "Обнулено (стало NULL): $emptied\n";
echo "Без изменений: $noChange\n";

if ($dryRun) {
    echo "\nЭто был DRY RUN. Для применения запустите:\n";
    echo "  php clean_sentry_emails.php --apply\n";
}
