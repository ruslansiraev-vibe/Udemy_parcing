<?php
/**
 * Нормализация URL профиля Instagram (как в clean_instagram.php).
 */

/**
 * Одна ссылка → https://www.instagram.com/username (нижний регистр, без query/#).
 */
function normalizeInstagramUrl(string $url): ?string
{
    $url = str_replace('\\', '', $url);

    if (preg_match('#instagram\.com/(?:https?://)?(?:www\.)?instagram\.com/(.*)#i', $url, $m)) {
        $url = 'https://www.instagram.com/' . $m[1];
    }

    if (!preg_match('#instagram\.com#i', $url)) {
        return null;
    }

    $parsed = parse_url($url);
    $path   = $parsed['path'] ?? '/';

    $path = preg_replace('#/profilecard/?$#i', '', $path);
    $path = rtrim($path, '/');

    $segments = array_values(array_filter(explode('/', $path)));
    if (empty($segments)) {
        return null;
    }

    $username = $segments[0];
    $username = ltrim($username, '@＠');
    $username = strtok($username, ' ');

    if ($username === '' || $username === 'p' || $username === 'reel' || $username === 'stories') {
        return null;
    }

    $username = strtok($username, '#');
    if ($username === '' || $username === false) {
        return null;
    }

    if (preg_match('/^[a-z0-9._-]+/i', $username, $validMatch)) {
        $username = $validMatch[0];
    } else {
        return null;
    }

    $username = strtolower($username);

    if (strlen($username) < 2 || strlen($username) > 30) {
        return null;
    }

    return 'https://www.instagram.com/' . $username;
}

/**
 * Значение ячейки (запятые, склеенные https://) → список нормализованных URL по порядку, без дублей.
 *
 * @return list<string>
 */
function normalizeInstagramFieldToUrls(string $raw): array
{
    $raw = trim($raw);
    if ($raw === '') {
        return [];
    }

    $out = [];
    $parts = preg_split('/\s*,\s*/', $raw);

    foreach ($parts as $chunk) {
        $urls = preg_split('#(?<=.)(?=https?://)#i', $chunk);

        foreach ($urls as $url) {
            $url = trim($url);
            if ($url === '') {
                continue;
            }
            $n = normalizeInstagramUrl($url);
            if ($n !== null) {
                $out[] = $n;
            }
        }
    }

    return array_values(array_unique($out));
}

/**
 * Значение ячейки → одна или несколько нормализованных ссылок через ", ".
 */
function normalizeInstagramField(string $raw): ?string
{
    $urls = normalizeInstagramFieldToUrls($raw);
    if ($urls === []) {
        return null;
    }

    return implode(', ', $urls);
}

/**
 * Нормализованный URL https://www.instagram.com/username → только username (нижний регистр).
 */
function instagramNormalizedUrlToUsername(string $url): string
{
    if (preg_match('~^https://www\.instagram\.com/([^/?#]+)/?$~i', $url, $m)) {
        return strtolower($m[1]);
    }

    return '';
}

/**
 * Сырая строка из БД → username, если возможно извлечь; иначе best-effort trim.
 */
function instagramRawToUsername(string $raw): string
{
    $n = normalizeInstagramUrl($raw);
    if ($n !== null) {
        return instagramNormalizedUrlToUsername($n);
    }
    if (preg_match('~(?:https?://)?(?:www\.)?instagram\.com/([^/?#\s,]+)~i', $raw, $m)) {
        return strtolower($m[1]);
    }

    return trim($raw);
}
