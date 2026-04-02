<?php

/**
 * Merges instructor_social_links_expanded.csv (base)
 * with instructor_emails.csv (emails)
 * joined on profile_url.
 *
 * Output: instructor_full.csv
 */

define('FILE_SOCIAL', __DIR__ . '/instructor_social_links_expanded.csv');
define('FILE_EMAILS', __DIR__ . '/instructor_emails.csv');
define('OUTPUT_FILE', __DIR__ . '/instructor_full.csv');

// ── helpers ───────────────────────────────────────────────────────────────────

/**
 * Remove junk emails produced by HTML Unicode escapes leaking into regex:
 * e.g. "u003eteam@example.com" → "team@example.com"
 *      "u002f@something"       → dropped (not a real email)
 */
function cleanEmails(string $raw): array
{
    // Skip marker values written by scrape_emails.php instead of real data
    $skipMarkers = ['NOT_FOUND', 'JS_REQUIRED', 'RETRY:1'];
    if ($raw === '' || str_starts_with($raw, 'ERROR:') || str_starts_with($raw, 'HTTP_') || in_array($raw, $skipMarkers, true)) {
        return [];
    }

    $emails = [];
    foreach (explode(';', $raw) as $email) {
        $email = trim($email);

        // Strip leading Unicode escape prefix (u003e, u003c, u002f, etc.)
        $email = preg_replace('/^u00[0-9a-fA-F]{2}/i', '', $email);
        $email = trim($email);

        // Must be a valid email after cleanup
        if ($email !== '' && filter_var($email, FILTER_VALIDATE_EMAIL)) {
            $emails[] = strtolower($email);
        }
    }

    return array_values(array_unique($emails));
}

// ── load emails index keyed by profile_url ────────────────────────────────────

$emailsIndex = []; // profile_url → emails string

if (!file_exists(FILE_EMAILS)) {
    fwrite(STDERR, "WARNING: " . FILE_EMAILS . " not found — emails column will be empty\n");
} else {
    $fh     = fopen(FILE_EMAILS, 'r');
    $header = fgetcsv($fh, 0, ',', '"', '\\');
    $colMap = array_flip($header);

    $cProfileUrl = $colMap['profile_url'] ?? 0;
    $cEmails     = $colMap['emails']      ?? 4;

    while (($row = fgetcsv($fh, 0, ',', '"', '\\')) !== false) {
        $profileUrl = trim($row[$cProfileUrl] ?? '');
        if ($profileUrl === '') {
            continue;
        }
        $cleanedEmails = cleanEmails($row[$cEmails] ?? '');
        $emailsIndex[$profileUrl] = implode(';', $cleanedEmails);
    }
    fclose($fh);
    echo "Loaded " . count($emailsIndex) . " email record(s) from " . basename(FILE_EMAILS) . "\n";
}

// ── read social links CSV and write merged output ─────────────────────────────

if (!file_exists(FILE_SOCIAL)) {
    fwrite(STDERR, "ERROR: " . FILE_SOCIAL . " not found\n");
    exit(1);
}

$inFh   = fopen(FILE_SOCIAL, 'r');
$outFh  = fopen(OUTPUT_FILE, 'w');

$socialHeader = fgetcsv($inFh, 0, ',', '"', '\\');
$colProfileUrl = array_search('profile_url', $socialHeader);

// Output header = all social columns + emails
$outputHeader = array_merge($socialHeader, ['emails']);
fputcsv($outFh, $outputHeader, ',', '"', '\\');

$total         = 0;
$withEmails    = 0;
$withoutEmails = 0;

while (($row = fgetcsv($inFh, 0, ',', '"', '\\')) !== false) {
    $profileUrl = trim($row[$colProfileUrl] ?? '');
    $emails     = $emailsIndex[$profileUrl] ?? '';

    $outRow = array_merge($row, [$emails]);

    fputcsv($outFh, $outRow, ',', '"', '\\');
    $total++;

    if ($emails !== '') {
        $withEmails++;
    } else {
        $withoutEmails++;
    }
}

fclose($inFh);
fclose($outFh);

echo "Total rows merged  : $total\n";
echo "With emails        : $withEmails\n";
echo "Without emails     : $withoutEmails\n";
echo "Output saved       : " . OUTPUT_FILE . "\n";
