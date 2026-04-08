<?php
/**
 * Создаёт недостающие столбцы validate_* в таблице лидов.
 * Подключается из validate_leads.php.
 *
 * @return list<string> имена добавленных столбцов
 */
function validate_ensure_columns(PDO $pdo, string $table): array
{
    static $done = false;
    if ($done) {
        return [];
    }

    $definitions = [
        'validate_email_match'     => "ENUM('yes','no','likely','unclear','not_applicable') DEFAULT NULL",
        'validate_email_reason'    => 'TEXT DEFAULT NULL',
        'validate_website_match'   => "ENUM('yes','no','likely','unclear','not_applicable') DEFAULT NULL",
        'validate_website_reason'  => 'TEXT DEFAULT NULL',
        'validate_instagram_match' => "ENUM('yes','no','likely','unclear','not_applicable') DEFAULT NULL",
        'validate_instagram_reason'=> 'TEXT DEFAULT NULL',
        'validate_linkedin_match'  => "ENUM('yes','no','likely','unclear','not_applicable') DEFAULT NULL",
        'validate_linkedin_reason' => 'TEXT DEFAULT NULL',
        'validate_confidence'      => 'DECIMAL(3,2) DEFAULT NULL',
        'validate_verdict'         => "ENUM('valid','suspicious','mismatch','insufficient_data') DEFAULT NULL",
        'validate_summary'         => 'TEXT DEFAULT NULL',
        'validate_raw_json'        => 'TEXT DEFAULT NULL',
        'validated_at'             => 'DATETIME DEFAULT NULL',
    ];

    if (!preg_match('/^[a-zA-Z0-9_]+$/', $table)) {
        throw new InvalidArgumentException('Invalid table name');
    }

    $existing = [];
    foreach ($pdo->query("SHOW COLUMNS FROM `{$table}`") as $col) {
        $existing[] = $col['Field'];
    }
    $added = [];
    foreach ($definitions as $name => $ddl) {
        if (!in_array($name, $existing, true)) {
            try {
                $pdo->exec("ALTER TABLE `{$table}` ADD COLUMN `{$name}` {$ddl}");
                $added[] = $name;
            } catch (PDOException $e) {
                if ($e->getCode() !== '42S21') {
                    throw $e;
                }
            }
        }
    }
    $done = true;

    return $added;
}
