<?php

/**
 * Splits social_links in instructor_social_links.csv into typed columns:
 * website, linkedin, youtube, facebook, twitter, instagram, tiktok, other
 *
 * Input:  instructor_social_links.csv
 * Output: instructor_social_links_expanded.csv
 */

define('INPUT_FILE',  __DIR__ . '/instructor_social_links.csv');
define('OUTPUT_FILE', __DIR__ . '/instructor_social_links_expanded.csv');

// ── Social network detection rules (order matters: checked top to bottom) ────

// Each rule: ['column_name', callable(string $url): bool]
$RULES = [
    'linkedin'  => fn($u) => str_contains($u, 'linkedin.com'),
    'youtube'   => fn($u) => str_contains($u, 'youtube.com') || str_contains($u, 'youtu.be'),
    'facebook'  => fn($u) => str_contains($u, 'facebook.com') || str_contains($u, 'fb.com'),
    'twitter'   => fn($u) => str_contains($u, 'twitter.com') || str_contains($u, 'x.com'),
    'instagram' => fn($u) => str_contains($u, 'instagram.com'),
    'tiktok'    => fn($u) => str_contains($u, 'tiktok.com'),
];

// Everything that doesn't match any social network goes to 'website'
// Multiple unrecognised links are stored as website_1, website_2, etc.

// ── helpers ───────────────────────────────────────────────────────────────────

/**
 * Classify a single URL.
 * Returns the column key (e.g. 'linkedin') or 'website' for personal sites.
 */
function classifyUrl(string $url, array $rules): string
{
    foreach ($rules as $name => $test) {
        if ($test($url)) {
            return $name;
        }
    }
    return 'website';
}

// ── Pass 1: find max number of 'website' links in one row ────────────────────

if (!file_exists(INPUT_FILE)) {
    fwrite(STDERR, "ERROR: Input file not found: " . INPUT_FILE . "\n");
    exit(1);
}

$fh     = fopen(INPUT_FILE, 'r');
$header = fgetcsv($fh, 0, ',', '"', '\\');
$colSocial = array_search('social_links', $header);

if ($colSocial === false) {
    fwrite(STDERR, "ERROR: Column 'social_links' not found\n");
    exit(1);
}

$maxWebsites = 0;
while (($row = fgetcsv($fh, 0, ',', '"', '\\')) !== false) {
    $raw   = $row[$colSocial] ?? '';
    $links = $raw !== '' ? array_values(array_filter(array_map('trim', explode(';', $raw)))) : [];
    $websiteCount = 0;
    foreach ($links as $link) {
        if (classifyUrl($link, $RULES) === 'website') {
            $websiteCount++;
        }
    }
    $maxWebsites = max($maxWebsites, $websiteCount);
}
fclose($fh);

// ── Build output header ───────────────────────────────────────────────────────

$socialCols = array_keys($RULES);  // linkedin, youtube, facebook, twitter, instagram, tiktok

$websiteCols = [];
if ($maxWebsites === 1) {
    $websiteCols = ['website'];
} else {
    for ($i = 1; $i <= $maxWebsites; $i++) {
        $websiteCols[] = "website_$i";
    }
}

$baseHeader   = array_values(array_filter($header, fn($c) => $c !== 'social_links'));
$outputHeader = array_merge($baseHeader, $websiteCols, $socialCols);

// ── Pass 2: write expanded CSV ────────────────────────────────────────────────

$inFh  = fopen(INPUT_FILE,  'r');
$outFh = fopen(OUTPUT_FILE, 'w');

fgetcsv($inFh, 0, ',', '"', '\\'); // skip header
fputcsv($outFh, $outputHeader, ',', '"', '\\');

$rowCount = 0;
while (($row = fgetcsv($inFh, 0, ',', '"', '\\')) !== false) {
    $raw   = $row[$colSocial] ?? '';
    $links = $raw !== '' ? array_values(array_filter(array_map('trim', explode(';', $raw)))) : [];

    // Classify each link
    $classified = array_fill_keys($socialCols, '');
    $websites   = [];

    foreach ($links as $link) {
        $type = classifyUrl($link, $RULES);
        if ($type === 'website') {
            $websites[] = $link;
        } else {
            // If somehow there are two linkedin links, keep the first
            if ($classified[$type] === '') {
                $classified[$type] = $link;
            }
        }
    }

    // Base columns (everything except social_links)
    $outRow = [];
    foreach ($header as $i => $col) {
        if ($col !== 'social_links') {
            $outRow[] = $row[$i] ?? '';
        }
    }

    // Website columns
    if ($maxWebsites === 1) {
        $outRow[] = $websites[0] ?? '';
    } else {
        for ($i = 0; $i < $maxWebsites; $i++) {
            $outRow[] = $websites[$i] ?? '';
        }
    }

    // Social network columns in fixed order
    foreach ($socialCols as $col) {
        $outRow[] = $classified[$col];
    }

    fputcsv($outFh, $outRow, ',', '"', '\\');
    $rowCount++;
}

fclose($inFh);
fclose($outFh);

echo "Rows processed : $rowCount\n";
echo "Output columns : " . implode(', ', $outputHeader) . "\n";
echo "Output saved   : " . OUTPUT_FILE . "\n";
