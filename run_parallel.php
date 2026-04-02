<?php

/**
 * run_parallel.php
 *
 * Runs scrape_instructors.php (or another script) in N parallel worker processes.
 *
 * Usage:
 *   php run_parallel.php [--workers=5] [--script=scrape_instructors.php]
 *
 * Each worker gets its own slice of rows (by _rowid % N == workerIndex).
 * Output from each worker is written to worker_0.log, worker_1.log, etc.
 * Progress of all workers is shown live in the terminal.
 */

$numWorkers = 5;
$script     = 'scrape_instructors.php';

foreach ($argv as $arg) {
    if (preg_match('/^--workers=(\d+)$/', $arg, $m))  $numWorkers = (int) $m[1];
    if (preg_match('/^--script=(.+)$/',   $arg, $m))  $script     = $m[1];
}

$scriptPath = __DIR__ . '/' . $script;
if (!file_exists($scriptPath)) {
    die("ERROR: Script not found: $scriptPath\n");
}

echo "Starting $numWorkers parallel workers for: $script\n";
echo str_repeat('-', 60) . "\n";

$processes = [];
$pipes     = [];
$logFiles  = [];

for ($i = 0; $i < $numWorkers; $i++) {
    $logFile    = __DIR__ . "/worker_{$i}.log";
    $logFiles[] = $logFile;

    // Clear old log
    file_put_contents($logFile, "");

    $cmd = "php " . escapeshellarg($scriptPath)
         . " --worker=$i"
         . " --total-workers=$numWorkers"
         . " >> " . escapeshellarg($logFile) . " 2>&1";

    $proc = proc_open($cmd, [], $p);
    if ($proc === false) {
        die("ERROR: Failed to start worker $i\n");
    }

    $processes[$i] = $proc;
    echo "[Worker $i] Started → log: worker_{$i}.log\n";
}

echo str_repeat('-', 60) . "\n";
echo "All workers running. Monitoring progress...\n\n";

// Track byte offset per log file — read only new bytes each iteration
$offsets   = array_fill(0, $numWorkers, 0);
$logFhs    = [];
$startTime = time();

for ($i = 0; $i < $numWorkers; $i++) {
    $logFhs[$i] = fopen($logFiles[$i], 'r');
}

while (true) {
    $allDone = true;

    for ($i = 0; $i < $numWorkers; $i++) {
        $status = proc_get_status($processes[$i]);
        if ($status['running']) {
            $allDone = false;
        }

        // Read only new bytes since last check
        fseek($logFhs[$i], $offsets[$i]);
        while (($line = fgets($logFhs[$i])) !== false) {
            $line = rtrim($line);
            if ($line !== '') {
                echo "[W$i] $line\n";
            }
        }
        $offsets[$i] = ftell($logFhs[$i]);
    }

    if ($allDone) {
        break;
    }

    sleep(2);
}

// Close log file handles
for ($i = 0; $i < $numWorkers; $i++) {
    fclose($logFhs[$i]);
    proc_close($processes[$i]);
}

$elapsed = time() - $startTime;
echo "\n" . str_repeat('=', 60) . "\n";
echo "All workers finished in {$elapsed}s\n";

// Print summary from each log — read only last 20 lines
for ($i = 0; $i < $numWorkers; $i++) {
    $fh      = fopen($logFiles[$i], 'r');
    $buffer  = [];
    while (($line = fgets($fh)) !== false) {
        $buffer[] = rtrim($line);
        if (count($buffer) > 20) {
            array_shift($buffer);
        }
    }
    fclose($fh);

    $summary   = [];
    $inSummary = false;
    foreach ($buffer as $line) {
        if (str_contains($line, '--- Done ---')) {
            $inSummary = true;
        }
        if ($inSummary && $line !== '') {
            $summary[] = $line;
        }
    }
    if ($summary) {
        echo "\n[Worker $i summary]\n" . implode("\n", $summary) . "\n";
    }
}
