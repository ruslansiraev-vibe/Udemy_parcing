<?php

/**
 * Копия таблицы leads + дедупликация по UdemyProfile1_parcing.
 *
 * Создаёт таблицу leads_copy (структура как у leads), копирует все строки,
 * затем удаляет дубликаты: для одинакового нормализованного URL профиля Udemy
 * остаётся строка с минимальным _rowid.
 *
 * Не трогает служебные значения (SKIP, NOT_FOUND, HTTP_ERROR, RETRY:*, пусто) —
 * такие строки не считаются «профилем» и не схлопываются.
 *
 * Usage:
 *   php dedupe_leads_by_udemy_profile.php              # создать leads_copy и дедуплицировать
 *   php dedupe_leads_by_udemy_profile.php --dry-run    # только отчёт, без записи в БД
 *   php dedupe_leads_by_udemy_profile.php --force      # удалить существующую leads_copy и заново
 */

define('DB_HOST', '127.0.0.1');
define('DB_PORT', 3306);
define('DB_NAME', 'udemy_leads');
define('DB_USER', 'root');
define('DB_PASS', '');

define('TARGET_TABLE', 'leads_copy');
define('PROFILE_COL', 'UdemyProfile1_parcing');

$dryRun = in_array('--dry-run', $argv ?? [], true);
$force  = in_array('--force', $argv ?? [], true);

// ── DB ───────────────────────────────────────────────────────────────────────

function getDb(): PDO
{
    static $pdo = null;
    if ($pdo === null) {
        $dsn = 'mysql:host=' . DB_HOST . ';port=' . DB_PORT . ';dbname=' . DB_NAME . ';charset=utf8mb4';
        $pdo = new PDO($dsn, DB_USER, DB_PASS, [
            PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8mb4',
        ]);
    }
    return $pdo;
}

/**
 * Ключ для дедупа: один инструктор = один канонический URL профиля.
 * Возвращает null, если значение не похоже на URL профиля Udemy — такие строки не удаляем как дубли.
 */
function profileDedupeKey(?string $raw): ?string
{
    $raw = trim((string) $raw);
    if ($raw === '') {
        return null;
    }
    static $markers = ['SKIP', 'NOT_FOUND', 'HTTP_ERROR', 'RETRY:1', 'RETRY_NET'];
    if (in_array($raw, $markers, true) || str_starts_with($raw, 'RETRY:')) {
        return null;
    }
    if (!preg_match('#^https?://#i', $raw)) {
        return null;
    }
    if (!preg_match('#udemy\.com/user/#i', $raw)) {
        return null;
    }
    $parts = parse_url($raw);
    if ($parts === false || empty($parts['host'])) {
        return null;
    }
    $host = strtolower($parts['host']);
    $path = $parts['path'] ?? '';
    $path = rtrim($path, '/');

    return 'https://' . $host . $path;
}

/**
 * Имена столбцов без GENERATED — иначе INSERT … SELECT * падает (ошибка 3105).
 */
function getInsertableColumns(PDO $pdo, string $table): array
{
    $q = $pdo->prepare(
        'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
           AND (COALESCE(EXTRA, \'\') NOT LIKE \'%GENERATED%\')
         ORDER BY ORDINAL_POSITION'
    );
    $q->execute([$table]);
    $cols = $q->fetchAll(PDO::FETCH_COLUMN);
    if ($cols === []) {
        throw new RuntimeException('Не удалось прочитать столбцы таблицы ' . $table);
    }

    return $cols;
}

// ── main ─────────────────────────────────────────────────────────────────────

$pdo = getDb();

$srcExists = $pdo->query("SHOW TABLES LIKE 'leads'")->fetch();
if (!$srcExists) {
    fwrite(STDERR, "[ERROR] Таблица leads не найдена в БД " . DB_NAME . "\n");
    exit(1);
}

if ($dryRun) {
    echo "[DRY-RUN] Запись в БД отключена — только расчёт и отчёт.\n\n";
}

if (!$dryRun) {
    $exists = $pdo->query("SHOW TABLES LIKE '" . TARGET_TABLE . "'")->fetch();
    if ($exists && !$force) {
        fwrite(STDERR, "[ERROR] Таблица " . TARGET_TABLE . " уже существует. Удалите её вручную или запустите с --force\n");
        exit(1);
    }
    if ($exists && $force) {
        $pdo->exec('DROP TABLE `' . TARGET_TABLE . '`');
        echo "[OK] Удалена существующая таблица " . TARGET_TABLE . "\n";
    }

    $pdo->exec('CREATE TABLE `' . TARGET_TABLE . '` LIKE `leads`');
    echo "[OK] CREATE TABLE " . TARGET_TABLE . " LIKE leads\n";

    $cols     = getInsertableColumns($pdo, 'leads');
    $colList  = implode('`, `', array_map(static fn ($c) => str_replace('`', '``', $c), $cols));
    $sql      = 'INSERT INTO `' . TARGET_TABLE . '` (`' . $colList . '`) SELECT `' . $colList . '` FROM `leads`';
    $pdo->exec($sql);
    $inserted = (int) $pdo->query('SELECT COUNT(*) FROM `' . TARGET_TABLE . '`')->fetchColumn();
    echo "[OK] Скопировано строк: $inserted\n\n";
}

$tableForSelect = $dryRun ? 'leads' : TARGET_TABLE;
$stmt = $pdo->query('SELECT `_rowid`, `' . PROFILE_COL . '` AS v FROM `' . $tableForSelect . '` ORDER BY `_rowid` ASC');

$seen     = [];
$toDelete = [];
$dupPairs = 0;

while ($row = $stmt->fetch()) {
    $rowid = (int) $row['_rowid'];
    $key   = profileDedupeKey($row['v'] ?? null);
    if ($key === null) {
        continue;
    }
    if (isset($seen[$key])) {
        $toDelete[] = $rowid;
        $dupPairs++;
    } else {
        $seen[$key] = $rowid;
    }
}

$deleteCount = count($toDelete);
echo "Уникальных профилей (после нормализации URL): " . count($seen) . "\n";
echo "Строк-дубликатов к удалению: $deleteCount\n";

if ($deleteCount === 0) {
    echo "\n[OK] Дубликатов нет.\n";
    exit(0);
}

if ($dryRun) {
    echo "\n[DRY-RUN] Примеры _rowid к удалению (до 20): " . implode(', ', array_slice($toDelete, 0, 20))
        . ($deleteCount > 20 ? ' ...' : '') . "\n";
    exit(0);
}

$chunkSize = 500;
for ($i = 0; $i < $deleteCount; $i += $chunkSize) {
    $chunk   = array_slice($toDelete, $i, $chunkSize);
    $placeholders = implode(',', array_fill(0, count($chunk), '?'));
    $del = $pdo->prepare('DELETE FROM `' . TARGET_TABLE . '` WHERE `_rowid` IN (' . $placeholders . ')');
    $del->execute($chunk);
}

$left = (int) $pdo->query('SELECT COUNT(*) FROM `' . TARGET_TABLE . '`')->fetchColumn();
echo "\n[OK] Удалено строк: $deleteCount. Осталось в " . TARGET_TABLE . ": $left\n";
