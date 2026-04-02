<?php

/**
 * Pipeline Orchestrator
 *
 * Runs all scraping steps in order and produces instructor_full.csv
 *
 * Usage:
 *   php run_pipeline.php              — run all steps (skip already done)
 *   php run_pipeline.php --fresh      — delete all intermediate files and start from scratch
 *   php run_pipeline.php --from=3     — start from step 3
 *   php run_pipeline.php --only=2     — run only step 2
 *   php run_pipeline.php --fresh --from=2  — delete files from step 2 onward and re-run
 */

define('BASE_DIR',  __DIR__);
define('LOG_FILE',  BASE_DIR . '/pipeline.log');

// ── Pipeline steps definition ─────────────────────────────────────────────────

$steps = [
    1 => [
        'name'    => 'Scrape instructor profiles',
        'script'  => 'scrape_instructors.php',
        'output'  => 'instructor_profiles.csv',
    ],
    2 => [
        'name'    => 'Scrape social links from Udemy profiles',
        'script'  => 'scrape_social_links.php',
        'output'  => 'instructor_social_links.csv',
    ],
    3 => [
        'name'    => 'Split social links into typed columns',
        'script'  => 'split_social_links.php',
        'output'  => 'instructor_social_links_expanded.csv',
    ],
    4 => [
        'name'    => 'Scrape emails & social links from websites',
        'script'  => 'scrape_emails.php',
        'output'  => 'instructor_emails.csv',
    ],
    5 => [
        'name'    => 'Merge all data into final file',
        'script'  => 'merge_instructor_data.php',
        'output'  => 'instructor_full.csv',
    ],
];

// ── Parse CLI arguments ───────────────────────────────────────────────────────

$fresh   = in_array('--fresh', $argv);
$fromStep = 1;
$onlyStep = null;

foreach ($argv as $arg) {
    if (preg_match('/^--from=(\d+)$/', $arg, $m)) {
        $fromStep = (int) $m[1];
    }
    if (preg_match('/^--only=(\d+)$/', $arg, $m)) {
        $onlyStep = (int) $m[1];
    }
}

if ($onlyStep !== null) {
    $fromStep = $onlyStep;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function log_msg(string $msg, bool $newline = true): void
{
    $line = '[' . date('Y-m-d H:i:s') . '] ' . $msg . ($newline ? "\n" : '');
    echo $line;
    file_put_contents(LOG_FILE, $line, FILE_APPEND);
}

function hr(): void
{
    log_msg(str_repeat('─', 60));
}

function countCsvRows(string $file): int
{
    if (!file_exists($file)) {
        return 0;
    }
    $fh    = fopen($file, 'r');
    $count = -1; // subtract header
    while (fgetcsv($fh, 0, ',', '"', '\\') !== false) {
        $count++;
    }
    fclose($fh);
    return max(0, $count);
}

function deleteFile(string $file): void
{
    if (file_exists($file)) {
        unlink($file);
        log_msg("  Deleted: " . basename($file));
    }
}

// ── Banner ────────────────────────────────────────────────────────────────────

hr();
log_msg('  UDEMY INSTRUCTOR DATA PIPELINE');
hr();
log_msg("  Mode     : " . ($fresh ? '--fresh (clean start)' : 'resume (skip done)'));
log_msg("  From step: $fromStep" . ($onlyStep ? " (only step $onlyStep)" : ''));
log_msg("  Log file : " . LOG_FILE);
hr();

// ── Fresh mode: delete output files from $fromStep onward ────────────────────

if ($fresh) {
    log_msg("\n[FRESH] Removing output files from step $fromStep onward...");
    foreach ($steps as $num => $step) {
        if ($num >= $fromStep) {
            deleteFile(BASE_DIR . '/' . $step['output']);
        }
    }
    echo "\n";
}

// ── Run steps ─────────────────────────────────────────────────────────────────

$pipelineStart = microtime(true);
$results       = [];

foreach ($steps as $num => $step) {

    // Skip steps before --from
    if ($num < $fromStep) {
        log_msg("[ SKIP ] Step $num: {$step['name']} (before --from)");
        continue;
    }

    // If --only, stop after that step
    if ($onlyStep !== null && $num > $onlyStep) {
        break;
    }

    $outputFile = BASE_DIR . '/' . $step['output'];
    $scriptFile = BASE_DIR . '/' . $step['script'];

    hr();
    log_msg("[ STEP $num ] {$step['name']}");
    log_msg("  Script : {$step['script']}");
    log_msg("  Output : {$step['output']}");

    // Check script exists
    if (!file_exists($scriptFile)) {
        log_msg("  ERROR: Script not found: {$step['script']}");
        $results[$num] = 'error_no_script';
        break;
    }

    $stepStart = microtime(true);

    // Run the script, streaming output in real time
    $cmd        = 'php ' . escapeshellarg($scriptFile) . ' 2>&1';
    $descriptor = [['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w']];
    $proc       = proc_open($cmd, $descriptor, $pipes, BASE_DIR);

    if (!is_resource($proc)) {
        log_msg("  ERROR: Failed to start process");
        $results[$num] = 'error_proc';
        break;
    }

    fclose($pipes[0]);

    // Stream stdout+stderr line by line
    while (!feof($pipes[1])) {
        $line = fgets($pipes[1]);
        if ($line !== false) {
            $trimmed = rtrim($line);
            echo "  | $trimmed\n";
            file_put_contents(LOG_FILE, "  | $trimmed\n", FILE_APPEND);
        }
    }
    fclose($pipes[1]);
    fclose($pipes[2]);

    $exitCode = proc_close($proc);
    $elapsed  = round(microtime(true) - $stepStart, 1);

    // Verify output file was created
    $rowCount = countCsvRows($outputFile);

    if ($exitCode !== 0) {
        log_msg("  RESULT : FAILED (exit code $exitCode, {$elapsed}s)");
        $results[$num] = 'failed';
        break;
    }

    if (!file_exists($outputFile)) {
        log_msg("  RESULT : WARNING — output file not created");
        $results[$num] = 'no_output';
    } else {
        log_msg("  RESULT : OK — {$rowCount} rows in {$step['output']} ({$elapsed}s)");
        $results[$num] = 'ok';
    }
}

// ── Final summary ─────────────────────────────────────────────────────────────

$totalElapsed = round(microtime(true) - $pipelineStart, 1);

hr();
log_msg('  PIPELINE SUMMARY');
hr();

foreach ($steps as $num => $step) {
    $status = $results[$num] ?? ($num < $fromStep ? 'skipped' : 'not_run');
    $icon   = match($status) {
        'ok'            => '✓',
        'skipped'       => '·',
        'not_run'       => '·',
        default         => '✗',
    };
    $outputFile = BASE_DIR . '/' . $step['output'];
    $rows       = file_exists($outputFile) ? countCsvRows($outputFile) . ' rows' : 'no file';
    log_msg("  $icon Step $num: {$step['name']} [$rows]");
}

hr();
log_msg("  Total time: {$totalElapsed}s");

$finalFile = BASE_DIR . '/instructor_full.csv';
if (file_exists($finalFile)) {
    $finalRows = countCsvRows($finalFile);
    log_msg("  Final file : instructor_full.csv ({$finalRows} rows)");
    log_msg("  Location   : $finalFile");
} else {
    log_msg("  Final file : not yet created (run all steps to completion)");
}

hr();
log_msg("  Full log saved to: " . LOG_FILE);
hr();
