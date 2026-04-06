<?php
/**
 * Создаёт недостающие столбцы xpoz_* в таблице лидов.
 * Подключается из xpoz_parser.php и xpoz_dashboard.php.
 *
 * @return list<string> имена добавленных столбцов (пусто при повторном вызове в том же запросе)
 */
function xpoz_ensure_columns(PDO $pdo, string $table): array
{
    static $done = false;
    if ($done) {
        return [];
    }

    $definitions = [
        'xpoz_scraped'               => 'TINYINT(1) DEFAULT NULL',
        'xpoz_follower_count'        => 'INT DEFAULT NULL',
        'xpoz_reels_performance'     => 'TINYINT(1) DEFAULT NULL',
        'xpoz_low_performing_reels'  => 'TINYINT(1) DEFAULT NULL',
        'xpoz_post_engagement'       => 'TINYINT(1) DEFAULT NULL',
        'xpoz_monetization'          => 'TINYINT(1) DEFAULT NULL',
        'xpoz_engagement_rate'       => 'DECIMAL(6,2) DEFAULT NULL',
        'xpoz_icp'                   => 'VARCHAR(10) DEFAULT NULL',
        'xpoz_offer_type'            => 'VARCHAR(40) DEFAULT NULL',
        'xpoz_funnel_type'           => 'VARCHAR(40) DEFAULT NULL',
        'xpoz_business_model'        => 'VARCHAR(40) DEFAULT NULL',
        'xpoz_audience_type'         => 'VARCHAR(40) DEFAULT NULL',
        'xpoz_monetization_strength' => 'VARCHAR(20) DEFAULT NULL',
        'xpoz_monetization_signals'  => 'TEXT DEFAULT NULL',
        'xpoz_monetization_reason'   => 'TEXT DEFAULT NULL',
        'xpoz_c4_reason'             => 'TEXT DEFAULT NULL',
        'xpoz_platform_mix'          => 'VARCHAR(40) DEFAULT NULL',
        'xpoz_primary_domain'        => 'VARCHAR(255) DEFAULT NULL',
        'xpoz_language'              => 'VARCHAR(10) DEFAULT NULL',
        'xpoz_geo_hint'              => 'VARCHAR(40) DEFAULT NULL',
        'xpoz_qualified'             => 'TINYINT(1) DEFAULT NULL',
        'xpoz_error'                 => 'TEXT DEFAULT NULL',
        'xpoz_analyzed_at'           => 'DATETIME DEFAULT NULL',
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
