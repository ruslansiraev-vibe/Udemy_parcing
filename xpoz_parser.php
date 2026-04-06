<?php
/**
 * xpoz_parser.php — квалификация Instagram-аккаунтов через Xpoz API + Claude Haiku.
 *
 * Читает аккаунты из leads_copy (instagram_parcing), анализирует каждый по 5
 * критериям через Xpoz MCP API, классифицирует монетизацию через Claude Haiku
 * (с heuristic fallback), сохраняет результаты в xpoz_* столбцы leads_copy.
 *
 * Usage:
 *   php xpoz_parser.php                                   # все необработанные
 *   php xpoz_parser.php --worker=0 --total-workers=5      # параллельный воркер
 *   php xpoz_parser.php --username=whop                   # один аккаунт
 *   php xpoz_parser.php --stats                           # статистика
 *   php xpoz_parser.php --reanalyze                       # повторный анализ
 *   php xpoz_parser.php --limit=50                        # ограничить выборку
 *   php xpoz_parser.php --from=100 --to=500               # диапазон по порядковому номеру
 *   php xpoz_parser.php --require-email                   # только строки с email (сайт/YT/TW/bio)
 */

/** SQL fragment: row has at least one usable email (aligned with summary.php logic). */
const XPOZ_ELIGIBLE_HAS_EMAIL_SQL = <<<'SQL'
(
  (`email_parcing` IS NOT NULL AND TRIM(`email_parcing`) != ''
   AND `email_parcing` NOT IN ('NOT_FOUND','JS_REQUIRED','SOCIAL_URL','UNAVAILABLE')
   AND `email_parcing` NOT LIKE 'HTTP_%' AND `email_parcing` NOT LIKE 'ERROR:%' AND `email_parcing` NOT LIKE 'RETRY:%')
  OR (`youtube_parcing_email` IS NOT NULL AND TRIM(`youtube_parcing_email`) != ''
      AND `youtube_parcing_email` NOT IN ('NOT_FOUND','INVALID_URL'))
  OR (`twitter_parcing_email` IS NOT NULL AND TRIM(`twitter_parcing_email`) != ''
      AND `twitter_parcing_email` NOT IN ('NOT_FOUND','INVALID_URL'))
  OR (`instagram_bio_email` IS NOT NULL AND TRIM(`instagram_bio_email`) != ''
      AND `instagram_bio_email` NOT IN ('NOT_FOUND','INVALID_URL'))
)
SQL;

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

define('XPOZ_URL',       'https://mcp.xpoz.ai/mcp');
define('XPOZ_API_KEY',   'K3B1MjGOvXuuX7wiJE5EeEyHgEDhNXfhuDl0QdkSmaA3QJ25EAkpNfOWFAG51OPlBHE1XrE');
define('ANTHROPIC_KEY',  getenv('ANTHROPIC_KEY') ?: '');
define('MODEL_HAIKU',    'claude-haiku-4-5-20251001');

define('POSTS_LIMIT',      20);
define('DAYS_90',           90);
define('DELAY_MS',          500);
define('FETCH_BATCH_SIZE',  100);
define('REQUEST_TIMEOUT',   90);

// ── Enum sets ────────────────────────────────────────────────────────────────

const OFFER_TYPES = [
    'course','coaching','consulting','agency_service','community_membership',
    'webinar_workshop','newsletter','digital_product','affiliate','ecommerce','unknown',
];
const FUNNEL_TYPES = [
    'book_call','dm_to_buy','link_in_bio','lead_magnet','webinar_funnel',
    'waitlist','subscribe_join','direct_checkout','unknown',
];
const BUSINESS_MODELS = [
    'education','service_business','community_business','audience_monetization',
    'software_tool','commerce_brand','media_only','unknown',
];
const AUDIENCE_TYPES = ['b2b','consumer','creator_economy','local_business','mixed','unknown'];
const MONETIZATION_STRENGTHS = ['none','weak','moderate','strong'];
const PLATFORM_MIXES = [
    'instagram_only','instagram_youtube','instagram_twitter',
    'instagram_telegram','instagram_website','multi_channel',
];

const STOPWORDS = [
    'with','that','this','your','from','into','about','there','their','have',
    'will','want','help','them','they','instagram','creator','founder','coach',
    'agency','community','program','course','bio','link','join','free','best',
];

const KEYWORD_RULES = [
    'course'               => ['course','program','bootcamp','masterclass','academy'],
    'coaching'             => ['coach','coaching','mentor','mentorship','1:1','mastermind'],
    'consulting'           => ['consulting','consultant','strategy','audit'],
    'agency_service'       => ['agency','done for you','dfy','service','client'],
    'community_membership' => ['community','membership','discord','telegram','subscribe to join'],
    'webinar_workshop'     => ['webinar','workshop','training','live session'],
    'newsletter'           => ['newsletter','substack','weekly email'],
    'digital_product'      => ['template','toolkit','ebook','digital product','download'],
    'affiliate'            => ['affiliate','commission','promo code','discount code'],
    'ecommerce'            => ['shop','store','merch','etsy','product'],
];

const MONETIZATION_SYSTEM_PROMPT = <<<'PROMPT'
You are a senior Instagram marketing analyst specializing in creator economy.

Your task: determine whether an Instagram account belongs to an EXPERT who monetizes their own expertise and knowledge — NOT through brand deals, sponsorships, or affiliate programs, but through their OWN products and services built on personal authority.

We are looking for: a blogger / content creator who has built an audience around a specific niche and sells their OWN expertise.

POSITIVE signals (expert monetization):
- Own online courses, programs, bootcamps, masterclasses, academies
- Own coaching, mentoring, consulting, 1:1 sessions, mastermind groups
- Own paid communities (Discord, Telegram, Skool, Circle, membership)
- Own webinars, workshops, live trainings (paid or as lead magnets)
- Own digital products: templates, toolkits, ebooks, guides, frameworks
- Own newsletters / paid newsletters (Substack, Beehiiv, etc.)
- Own SaaS / software tools built on their expertise
- Lead magnets funneling into paid expert offerings (free guide → course, free webinar → coaching)
- Clear niche authority: the person teaches, advises, or consults in a specific domain

NEGATIVE signals (NOT what we are looking for):
- Brand deals, sponsored posts, paid partnerships (#ad, #sponsored, "thanks to @brand")
- Affiliate marketing, promo codes, discount codes for other companies' products
- Promoting other people's products or services for commission
- Influencer-style product placements (unboxing, reviews for brands)
- Reselling or dropshipping (no personal expertise involved)
- Pure ecommerce / merch without expertise component
- "Link in bio" leading only to free YouTube / TikTok content with no paid offering

Key distinction: the person must be THE expert, not a promoter of someone else's expertise.

Analyze the provided bio and recent post captions carefully. Return strict JSON only:
{
  "is_expert": true,
  "has_own_product": true,
  "expertise_niche": "specific niche in 2-5 words",
  "signals_found": ["exact phrases from bio/captions proving expert monetization"],
  "red_flags": ["any brand deal / affiliate / non-expert signals found"],
  "reasoning": "2-3 sentences explaining why this is or isn't an expert monetizing own knowledge",
  "offer_type": "course|coaching|consulting|agency_service|community_membership|webinar_workshop|newsletter|digital_product|software_tool|unknown",
  "funnel_type": "book_call|dm_to_buy|link_in_bio|lead_magnet|webinar_funnel|waitlist|subscribe_join|direct_checkout|unknown",
  "business_model": "education|service_business|community_business|audience_monetization|software_tool|unknown",
  "audience_type": "b2b|consumer|creator_economy|local_business|mixed|unknown",
  "monetization_strength": "none|weak|moderate|strong",
  "cta_keywords": ["cta keywords from bio/captions"],
  "bio_keywords": ["niche/expertise keywords"],
  "confidence": 0.0,
  "icp": "ICP1|ICP2|ICP3|ICP4|ICP5|unknown",
  "c4_reason": "Explain in 2-4 sentences WHY this account qualifies or does not qualify for C4 criterion: does the person monetize their OWN expertise (not brand deals / affiliate)? Mention specific evidence from bio or captions."
}

Confidence guide:
- 0.8-1.0: clear expert with own paid products/services
- 0.6-0.8: likely expert, some signals but not fully explicit
- 0.4-0.6: ambiguous — could be expert or could be affiliate/brand promoter
- 0.2-0.4: mostly brand deals / affiliate, minimal own expertise signals
- 0.0-0.2: no expert monetization at all
PROMPT;

// ── DB ───────────────────────────────────────────────────────────────────────

require_once __DIR__ . '/xpoz_migrate.php';

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
    $added = xpoz_ensure_columns(getDb(), DB_TABLE);
    if (PHP_SAPI === 'cli') {
        foreach ($added as $name) {
            echo "  + Added column: {$name}\n";
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function safeInt($v, int $default = 0): int
{
    if ($v === null || $v === '' || $v === 'null' || $v === 'None') return $default;
    return is_numeric($v) ? (int)$v : $default;
}

function parseDate(?string $s): ?int
{
    if (!$s || in_array($s, ['null', '0', 'None'], true)) return null;
    $ts = strtotime($s);
    return $ts !== false ? $ts : null;
}

function pickEnum($raw, array $allowed, string $default = 'unknown'): string
{
    $text = strtolower(trim((string)($raw ?? '')));
    return in_array($text, $allowed, true) ? $text : $default;
}

function cleanList($raw, int $limit = 8): array
{
    if (is_array($raw)) {
        $items = $raw;
    } elseif (is_string($raw)) {
        $items = preg_split('/[,;\n]/', $raw);
    } else {
        $items = [];
    }
    $seen = [];
    $cleaned = [];
    foreach ($items as $item) {
        $text = trim(trim((string)($item ?? ''), '"\''));
        if ($text === '') continue;
        $key = strtolower($text);
        if (isset($seen[$key])) continue;
        $seen[$key] = true;
        $cleaned[] = mb_substr($text, 0, 80);
        if (count($cleaned) >= $limit) break;
    }
    return $cleaned;
}

function extractKeywords(string $text, int $limit = 8): array
{
    preg_match_all('/[A-Za-z][A-Za-z0-9+&.\-]{3,}/', strtolower($text), $matches);
    $seen = [];
    $keywords = [];
    foreach ($matches[0] as $token) {
        if (in_array($token, STOPWORDS, true) || ctype_digit($token)) continue;
        if (isset($seen[$token])) continue;
        $seen[$token] = true;
        $keywords[] = $token;
        if (count($keywords) >= $limit) break;
    }
    return $keywords;
}

function extractCtaKeywords(string $text, int $limit = 8): array
{
    $candidates = [
        'book a call','dm me','link in bio','free guide','free training',
        'free webinar','join community','join newsletter','shop now',
        'apply now','consulting','coaching','course','masterclass','template',
    ];
    $lowered = strtolower($text);
    $found = [];
    foreach ($candidates as $c) {
        if (str_contains($lowered, $c)) $found[] = $c;
    }
    return cleanList($found, $limit);
}

function extractPrimaryDomain(string $externalUrl, array $otherSocials): string
{
    $urls = [];
    if ($externalUrl) $urls[] = $externalUrl;
    foreach ($otherSocials as $val) {
        if (is_array($val) && !empty($val['url'])) {
            $urls[] = $val['url'];
        } elseif (is_string($val)) {
            $urls[] = $val;
        }
    }
    $socialHosts = ['instagram.com','youtube.com','youtu.be','twitter.com',
                    'x.com','tiktok.com','linkedin.com','t.me','telegram.me'];
    foreach ($urls as $raw) {
        $raw = trim($raw);
        if (!$raw || !str_contains($raw, '.')) continue;
        $parsed = parse_url(str_contains($raw, '://') ? $raw : "https://{$raw}");
        $host = strtolower(trim($parsed['host'] ?? $parsed['path'] ?? ''));
        if (str_starts_with($host, 'www.')) $host = substr($host, 4);
        if (!$host) continue;
        $skip = false;
        foreach ($socialHosts as $sh) {
            if (str_contains($host, $sh)) { $skip = true; break; }
        }
        if (!$skip) return $host;
    }
    return '';
}

// ── Inference functions ──────────────────────────────────────────────────────

function inferPlatformMix(array $otherSocials, string $externalUrl): string
{
    $hasYoutube  = !empty($otherSocials['youtube']);
    $hasTwitter  = !empty($otherSocials['twitter']) || !empty($otherSocials['twitter_bio']);
    $hasTelegram = !empty($otherSocials['telegram']);
    $hasSite     = extractPrimaryDomain($externalUrl, $otherSocials) !== '';
    $count = (int)$hasYoutube + (int)$hasTwitter + (int)$hasTelegram + (int)$hasSite;
    if ($count >= 2) return 'multi_channel';
    if ($hasYoutube)  return 'instagram_youtube';
    if ($hasTwitter)  return 'instagram_twitter';
    if ($hasTelegram) return 'instagram_telegram';
    if ($hasSite)     return 'instagram_website';
    return 'instagram_only';
}

function inferLanguage(string $text): string
{
    $sample = mb_strtolower($text);
    if (trim($sample) === '') return 'unknown';
    if (preg_match('/[а-яё]/u', $sample)) return 'ru';
    $esWords = ['hola','espa','latam','mexico','madrid','barcelona'];
    foreach ($esWords as $w) { if (str_contains($sample, $w)) return 'es'; }
    $ptWords = ['ola','brasil','portugal','lisboa','sao paulo'];
    foreach ($ptWords as $w) { if (str_contains($sample, $w)) return 'pt'; }
    return 'en';
}

function inferGeoHint(string $text, string $externalUrl): string
{
    $lowered = strtolower("{$text} {$externalUrl}");
    $geoMap = [
        'usa'       => ['usa','united states','new york','miami','los angeles','california'],
        'uk'        => ['london','uk','united kingdom','england'],
        'uae'       => ['dubai','abu dhabi','uae','emirates'],
        'canada'    => ['canada','toronto','vancouver'],
        'australia' => ['australia','sydney','melbourne'],
        'germany'   => ['germany','berlin','.de'],
        'france'    => ['france','paris','.fr'],
        'spain'     => ['spain','madrid','barcelona','.es'],
        'brazil'    => ['brazil','brasil','sao paulo','.br'],
        'russia'    => ['moscow','russia','россия','.ru'],
    ];
    foreach ($geoMap as $geo => $markers) {
        foreach ($markers as $m) {
            if (str_contains($lowered, $m)) return $geo;
        }
    }
    return 'unknown';
}

function inferBusinessModel(string $offerType, array $signals): string
{
    $lowered = strtolower(implode(' ', $signals));
    if (in_array($offerType, ['course','coaching','webinar_workshop'])) return 'education';
    if (in_array($offerType, ['consulting','agency_service']))         return 'service_business';
    if (in_array($offerType, ['community_membership','newsletter']))   return 'community_business';
    if ($offerType === 'digital_product')                              return 'audience_monetization';
    if (str_contains($lowered, 'software') || str_contains($lowered, 'saas') || str_contains($lowered, 'app'))
        return 'software_tool';
    if (in_array($offerType, ['ecommerce','affiliate']))               return 'commerce_brand';
    return $signals ? 'media_only' : 'unknown';
}

function inferAudienceType(string $text): string
{
    $lowered = strtolower($text);
    $b2b = ['founder','agency','brand','business','b2b','ceo','consultant','freelancer'];
    $consumer = ['mom','fitness','beauty','recipe','fashion','lifestyle','travel','wellness'];
    $hasB2B = false;
    $hasCons = false;
    foreach ($b2b as $m)      { if (str_contains($lowered, $m)) { $hasB2B = true; break; } }
    foreach ($consumer as $m) { if (str_contains($lowered, $m)) { $hasCons = true; break; } }
    if ($hasB2B && $hasCons) return 'mixed';
    if ($hasB2B)  return 'b2b';
    if ($hasCons) return 'consumer';
    if (str_contains($lowered, 'creator')) return 'creator_economy';
    $local = ['local','studio','clinic','salon','restaurant'];
    foreach ($local as $m) { if (str_contains($lowered, $m)) return 'local_business'; }
    return 'unknown';
}

function inferIcp(string $offerType, string $businessModel, string $audienceType): string
{
    if (in_array($offerType, ['course','coaching','webinar_workshop']))     return 'ICP1';
    if (in_array($offerType, ['consulting','agency_service']) || $audienceType === 'b2b') return 'ICP2';
    if (in_array($offerType, ['community_membership','newsletter']))       return 'ICP3';
    if (in_array($businessModel, ['commerce_brand','software_tool']) ||
        in_array($offerType, ['ecommerce','affiliate']))                   return 'ICP4';
    if (in_array($offerType, ['digital_product','unknown']))               return 'ICP5';
    return 'unknown';
}

// ── Heuristic taxonomy (keyword-based fallback for C4) ───────────────────────

function heuristicTaxonomy(string $text): array
{
    $lowered = strtolower($text);
    $signals = [];
    $offerType = 'unknown';
    foreach (KEYWORD_RULES as $candidate => $patterns) {
        $matched = false;
        foreach ($patterns as $pattern) {
            if (str_contains($lowered, $pattern)) {
                $signals[] = $pattern;
                $matched = true;
            }
        }
        if ($matched) { $offerType = $candidate; break; }
    }

    $funnelType = 'unknown';
    $funnelMap = [
        'book_call'       => ['book a call','book call','strategy call','apply now','application'],
        'dm_to_buy'       => ['dm me','message me','send dm','reply dm'],
        'lead_magnet'     => ['free guide','free checklist','free training','lead magnet','freebie'],
        'webinar_funnel'  => ['webinar','workshop','masterclass'],
        'waitlist'        => ['waitlist','join waitlist'],
        'subscribe_join'  => ['join','subscribe','membership','community'],
        'direct_checkout' => ['buy now','shop now','checkout'],
        'link_in_bio'     => ['link in bio'],
    ];
    foreach ($funnelMap as $ft => $patterns) {
        foreach ($patterns as $p) {
            if (str_contains($lowered, $p)) { $funnelType = $ft; break 2; }
        }
    }

    if (!$signals) $signals = extractCtaKeywords($text);
    $signals = cleanList($signals);
    $businessModel = inferBusinessModel($offerType, $signals);
    $audienceType  = inferAudienceType($text);
    $cnt = count($signals);
    $strength = $cnt >= 3 ? 'strong' : ($cnt >= 2 ? 'moderate' : ($cnt > 0 ? 'weak' : 'none'));
    $icp = inferIcp($offerType, $businessModel, $audienceType);

    return [
        'has_signals'           => (bool)$signals,
        'signals_found'         => $signals,
        'reasoning'             => 'Heuristic fallback based on bio/caption keywords.',
        'offer_type'            => $offerType,
        'funnel_type'           => $funnelType,
        'business_model'        => $businessModel,
        'audience_type'         => $audienceType,
        'monetization_strength' => $strength,
        'cta_keywords'          => extractCtaKeywords($text),
        'bio_keywords'          => extractKeywords($text),
        'confidence'            => $signals ? 0.45 : 0.2,
        'icp'                   => $icp,
    ];
}

function normalizeTaxonomyResult(
    array $rawResult,
    string $biography,
    string $externalUrl,
    array $otherSocials,
    string $captionsText,
): array {
    $fallback = heuristicTaxonomy("{$biography}\n{$captionsText}\n{$externalUrl}");
    $merged = $fallback;
    foreach ($rawResult as $k => $v) {
        if ($v !== null && $v !== '') $merged[$k] = $v;
    }

    $signals     = cleanList($merged['signals_found'] ?? $fallback['signals_found']);
    $ctaKeywords = cleanList($merged['cta_keywords'] ?? extractCtaKeywords("{$biography}\n{$captionsText}"));
    $bioKeywords = cleanList($merged['bio_keywords'] ?? extractKeywords($biography));
    $offerType   = pickEnum($merged['offer_type'] ?? null, OFFER_TYPES);
    $funnelType  = pickEnum($merged['funnel_type'] ?? null, FUNNEL_TYPES);
    $businessModel = pickEnum(
        $merged['business_model'] ?? null, BUSINESS_MODELS,
        inferBusinessModel($offerType, $signals)
    );
    $audienceType = pickEnum(
        $merged['audience_type'] ?? null, AUDIENCE_TYPES,
        inferAudienceType("{$biography}\n{$captionsText}")
    );
    $strength = pickEnum(
        $merged['monetization_strength'] ?? null, MONETIZATION_STRENGTHS,
        $fallback['monetization_strength']
    );
    $conf = $merged['confidence'] ?? $fallback['confidence'];
    $confidence = max(0.0, min(1.0, is_numeric($conf) ? (float)$conf : (float)$fallback['confidence']));
    $primaryDomain = extractPrimaryDomain($externalUrl, $otherSocials);
    $platformMix   = inferPlatformMix($otherSocials, $externalUrl);
    $language      = inferLanguage($biography);
    $geoHint       = inferGeoHint($biography, $externalUrl);
    $icp = strtoupper(trim((string)($merged['icp'] ?? '')));
    if (!in_array($icp, ['ICP1','ICP2','ICP3','ICP4','ICP5'])) {
        $icp = inferIcp($offerType, $businessModel, $audienceType);
    }

    $c4Reason = trim((string)($merged['c4_reason'] ?? ''));
    if ($c4Reason === '') {
        $c4Reason = trim((string)($merged['reasoning'] ?? $fallback['reasoning'] ?? ''));
    }

    return [
        'has_signals'           => (bool)($merged['has_signals'] ?? (bool)$signals),
        'signals_found'         => $signals,
        'reasoning'             => trim((string)($merged['reasoning'] ?? $fallback['reasoning'])),
        'offer_type'            => $offerType,
        'funnel_type'           => $funnelType,
        'business_model'        => $businessModel,
        'audience_type'         => $audienceType,
        'monetization_strength' => $strength,
        'cta_keywords'          => $ctaKeywords,
        'bio_keywords'          => $bioKeywords,
        'confidence'            => round($confidence, 3),
        'platform_mix'          => in_array($platformMix, PLATFORM_MIXES) ? $platformMix : 'instagram_only',
        'primary_domain'        => $primaryDomain,
        'language'              => $language,
        'geo_hint'              => $geoHint,
        'icp'                   => $icp,
        'c4_reason'             => $c4Reason,
    ];
}

// ── Xpoz MCP HTTP client ────────────────────────────────────────────────────

function xpozCall(string $tool, array $args, int $maxPoll = 10): array
{
    $headers = [
        'Authorization: Bearer ' . XPOZ_API_KEY,
        'Content-Type: application/json',
        'Accept: application/json, text/event-stream',
    ];
    $payload = json_encode([
        'jsonrpc' => '2.0',
        'id'      => 1,
        'method'  => 'tools/call',
        'params'  => ['name' => $tool, 'arguments' => $args],
    ], JSON_UNESCAPED_UNICODE);

    for ($attempt = 0; $attempt < 3; $attempt++) {
        $body = curlPost(XPOZ_URL, $payload, $headers);
        if ($body === false) {
            sleep(3);
            continue;
        }
        if (str_contains(strtolower($body), 'quota') ||
            str_contains($body, 'USAGE_LIMIT_EXCEEDED') ||
            str_contains($body, '"429"')) {
            echo "  ⚠ Xpoz quota/429, waiting 30s...\n";
            sleep(30);
            continue;
        }

        $data = parseSse($body);

        $opId = $data['operationId'] ?? null;
        if ($opId && ($data['status'] ?? '') === 'running') {
            for ($i = 0; $i < $maxPoll; $i++) {
                sleep(8);
                $pollPayload = json_encode([
                    'jsonrpc' => '2.0', 'id' => 1, 'method' => 'tools/call',
                    'params' => ['name' => 'checkOperationStatus',
                                 'arguments' => ['operationId' => $opId]],
                ], JSON_UNESCAPED_UNICODE);
                $pollBody = curlPost(XPOZ_URL, $pollPayload, $headers);
                if ($pollBody === false) continue;
                $data = parseSse($pollBody);
                if (!empty($data['success']) || str_contains(json_encode($data), 'results')) break;
                if (($data['status'] ?? 'running') !== 'running') break;
            }
        }
        return $data;
    }
    return ['error' => 'Xpoz request failed after retries'];
}

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
    if ($err) return false;
    return $result;
}

function parseSse(string $text): array
{
    foreach (explode("\n", $text) as $line) {
        if (!str_starts_with($line, 'data:')) continue;
        $jsonStr = trim(substr($line, 5));
        $outer = @json_decode($jsonStr, true);
        if (!is_array($outer)) continue;

        $content = $outer['result']['content'] ?? [];
        if ($content && ($content[0]['type'] ?? '') === 'text') {
            return parseYamlIsh($content[0]['text']);
        }
        if (isset($outer['error'])) {
            return ['error' => $outer['error']['message'] ?? json_encode($outer['error'])];
        }
    }
    return [];
}

function parseYamlIsh(string $text): array
{
    $trimmed = trim($text);
    if (str_starts_with($trimmed, '{')) {
        $decoded = @json_decode($trimmed, true);
        if (is_array($decoded)) return $decoded;
    }

    $result = [];
    $inResults = false;
    $rows = [];
    $fieldsOrder = [];

    foreach (explode("\n", $text) as $line) {
        $stripped = trim($line);
        if ($stripped === '') continue;

        if (str_contains($stripped, ':') && !str_starts_with($stripped, '-') && !$inResults) {
            $colonPos = strpos($stripped, ':');
            $key = trim(substr($stripped, 0, $colonPos));
            $val = trim(substr($stripped, $colonPos + 1));

            if (str_starts_with($key, 'results[') && str_contains($key, '{')) {
                $inResults = true;
                $fieldsPart = substr($key, strpos($key, '{') + 1, strpos($key, '}') - strpos($key, '{') - 1);
                $fieldsOrder = array_map('trim', explode(',', $fieldsPart));
                $result['results'] = [];
                continue;
            }

            if ($key === 'success') {
                $result[$key] = strtolower($val) === 'true';
            } elseif (ctype_digit($val)) {
                $result[$key] = (int)$val;
            } elseif (str_starts_with($val, '"') && str_ends_with($val, '"')) {
                $result[$key] = substr($val, 1, -1);
            } else {
                $result[$key] = $val;
            }
        } elseif ($inResults && $stripped) {
            $rowValues = str_getcsv($stripped, ',', '"', '');
            if (count($rowValues) >= count($fieldsOrder)) {
                $row = [];
                foreach ($fieldsOrder as $i => $f) {
                    $row[$f] = $rowValues[$i] ?? null;
                }
                $rows[] = $row;
            }
        }
    }

    if ($inResults) $result['results'] = $rows;
    return $result;
}

// ── Username extraction ──────────────────────────────────────────────────────

function extractUsername(string $raw): string
{
    $raw = trim($raw);
    $raw = preg_replace('/[?#].*$/', '', $raw);
    $raw = rtrim($raw, '/');
    if (preg_match('#instagram\.com/([a-zA-Z0-9_.]{1,30})$#i', $raw, $m)) {
        return strtolower($m[1]);
    }
    $raw = ltrim($raw, '@');
    if (preg_match('/^[a-zA-Z0-9_.]{1,30}$/', $raw)) {
        return strtolower($raw);
    }
    return '';
}

// ── Data fetching ────────────────────────────────────────────────────────────

function fetchProfile(string $username): array
{
    $fields = ['id','username','fullName','biography',
               'followerCount','followingCount','mediaCount',
               'isVerified','isPrivate','externalUrl'];

    $data = xpozCall('getInstagramUser', [
        'identifier'     => $username,
        'identifierType' => 'username',
        'fields'         => $fields,
    ]);
    $nested = $data['data'] ?? null;
    $profile = (is_array($nested) && !empty($nested['username'])) ? $nested : $data;

    $fol = safeInt($profile['followerCount'] ?? null);
    if ($fol === 0 && empty($profile['error'])) {
        $existingId = trim((string)($profile['id'] ?? ''));
        if (!$existingId) {
            $existingId = searchInstagramId($username);
        }
        if ($existingId) {
            $rtData = xpozCall('getInstagramUser', [
                'identifier'     => $existingId,
                'identifierType' => 'id',
                'fields'         => $fields,
            ]);
            $rtNested = $rtData['data'] ?? null;
            $rtProfile = (is_array($rtNested) && !empty($rtNested['username'])) ? $rtNested : $rtData;
            if (safeInt($rtProfile['followerCount'] ?? null) > 0) {
                $rtProfile['_realtime'] = true;
                $rtProfile['_ig_id'] = $existingId;
                return $rtProfile;
            }
        }
    }
    return $profile;
}

function searchInstagramId(string $username): string
{
    $data = xpozCall('searchInstagramUsers', ['name' => $username, 'limit' => 5]);
    $rows = $data['results'] ?? $data['data'] ?? [];
    if (!is_array($rows)) return '';
    foreach ($rows as $row) {
        if (is_array($row) && strtolower($row['username'] ?? '') === strtolower($username)) {
            return (string)($row['id'] ?? '');
        }
    }
    return '';
}

function fetchPosts(string $username, ?string $igId = null): array
{
    $fields = ['id','postType','mediaType','caption',
               'createdAtDate','likeCount','commentCount',
               'reshareCount','videoPlayCount'];
    $params = [
        'identifier'     => $igId ?: $username,
        'identifierType' => $igId ? 'id' : 'username',
        'limit'          => POSTS_LIMIT,
        'fields'         => $fields,
    ];
    $data = xpozCall('getInstagramPostsByUser', $params);
    return $data['results'] ?? [];
}

function fetchTwitter(string $username): ?array
{
    $data = xpozCall('getTwitterUser', [
        'identifier'     => $username,
        'identifierType' => 'username',
        'fields'         => ['id','username','name','description',
                             'followersCount','tweetCount','isVerified'],
    ]);
    if (!is_array($data) || !empty($data['error']) || ($data['success'] ?? true) === false) return null;
    $nested = $data['data'] ?? $data;
    return is_array($nested) ? $nested : null;
}

function isReel(array $post): bool
{
    $media = strtolower((string)($post['mediaType'] ?? ''));
    $ptype = strtolower((string)($post['postType'] ?? ''));
    return $media === 'video' || str_contains($ptype, 'reel');
}

// ── Criteria ─────────────────────────────────────────────────────────────────

function criterion1ReelsPerformance(array $posts, int $followerCount): array
{
    $threshold = $followerCount * 1.5;
    $cutoff = time() - DAYS_90 * 86400;

    $hasDates = false;
    foreach ($posts as $p) {
        if (parseDate($p['createdAtDate'] ?? null) !== null) { $hasDates = true; break; }
    }

    $qualifying = [];
    $totalReels = 0;
    foreach ($posts as $p) {
        if (!isReel($p)) continue;
        $dt = parseDate($p['createdAtDate'] ?? null);
        if ($hasDates && $dt !== null && $dt < $cutoff) continue;
        $totalReels++;
        $views = safeInt($p['videoPlayCount'] ?? null);
        if ($views >= $threshold) {
            $qualifying[] = [
                'views'   => $views,
                'caption' => mb_substr((string)($p['caption'] ?? ''), 0, 60),
            ];
        }
    }

    $passed = count($qualifying) >= 5;
    return [$passed, $totalReels, count($qualifying)];
}

function criterion2LowPerforming(array $posts, int $followerCount): array
{
    $threshold = $followerCount * 0.15;
    $reels = [];
    foreach ($posts as $p) {
        if (isReel($p) && ($p['videoPlayCount'] ?? null) !== null) {
            $reels[] = $p;
        }
    }
    if (count($reels) < 10) return [false, 0.0];

    $last15 = array_slice($reels, 0, 15);
    $views = [];
    foreach ($last15 as $p) $views[] = safeInt($p['videoPlayCount'] ?? null);
    sort($views);
    $bottom10 = array_slice($views, 0, 10);
    $avg = array_sum($bottom10) / 10;

    return [$avg > $threshold, round($avg, 1)];
}

function criterion3Engagement(array $posts, int $followerCount): array
{
    $withData = [];
    foreach ($posts as $p) {
        if (($p['likeCount'] ?? null) !== null) $withData[] = $p;
    }
    $last15 = array_slice($withData, 0, 15);
    if (!$last15) return [false, 0.0, 0];

    $totalLikes    = 0;
    $totalComments = 0;
    $totalReshares = 0;
    foreach ($last15 as $p) {
        $totalLikes    += safeInt($p['likeCount'] ?? null);
        $totalComments += safeInt($p['commentCount'] ?? null);
        $totalReshares += safeInt($p['reshareCount'] ?? null);
    }
    $total = $totalLikes + $totalComments + $totalReshares;
    $avgPerPost = $total / count($last15);
    $rate = $followerCount > 0 ? $avgPerPost / $followerCount : 0;

    return [$rate >= 0.015, round($rate * 100, 2), $total];
}

function criterion4Monetization(array $posts, string $biography, string $externalUrl): array
{
    $captions = [];
    foreach ($posts as $p) {
        $cap = (string)($p['caption'] ?? '');
        if ($cap !== '') $captions[] = mb_substr($cap, 0, 200);
    }
    $captionsText = implode("\n---\n", array_slice($captions, 0, 20));
    $contentText = "Bio: {$biography}\nExternal URL: {$externalUrl}\n\nCaptions:\n{$captionsText}";
    $fallback = heuristicTaxonomy($contentText);

    if (ANTHROPIC_KEY === '') {
        $fallback['error'] = 'ANTHROPIC_API_KEY empty, heuristic fallback used';
        return [$fallback['has_signals'], $fallback];
    }

    $apiResult = callClaudeHaiku($contentText);
    if ($apiResult === null) {
        return [$fallback['has_signals'], $fallback];
    }
    return [$apiResult['has_signals'] ?? $fallback['has_signals'], $apiResult];
}

function callClaudeHaiku(string $contentText): ?array
{
    $payload = json_encode([
        'model'      => MODEL_HAIKU,
        'max_tokens' => 1024,
        'system'     => [[
            'type'          => 'text',
            'text'          => MONETIZATION_SYSTEM_PROMPT,
            'cache_control' => ['type' => 'ephemeral'],
        ]],
        'messages' => [[
            'role'    => 'user',
            'content' => $contentText,
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

function criterion5OtherSocials(string $username, string $biography, string $externalUrl): array
{
    $found = [];

    $ytSignals = [];
    if ($externalUrl && (str_contains($externalUrl, 'youtube.com') || str_contains($externalUrl, 'youtu.be'))) {
        $ytSignals[] = $externalUrl;
    }

    foreach (explode(' ', $biography) as $raw) {
        $w = trim($raw, '.,!() ');
        if (str_contains($w, 'youtube.com') || str_contains($w, 'youtu.be'))    $ytSignals[] = $w;
        if (str_contains($w, 'tiktok.com'))                                       $found['tiktok'] = $w;
        if (str_contains($w, 'twitter.com') || str_contains($w, 'x.com'))        $found['twitter_bio'] = $w;
        if (str_contains($w, 'linkedin.com'))                                     $found['linkedin'] = $w;
        if (str_contains($w, 't.me') || str_contains(strtolower($w), 'telegram'))$found['telegram'] = $w;
    }
    if ($ytSignals) $found['youtube'] = $ytSignals[0];

    $twitterData = fetchTwitter($username);
    if ($twitterData && empty($twitterData['error'])) {
        $found['twitter'] = [
            'url'       => 'https://twitter.com/' . ($twitterData['username'] ?? $username),
            'followers' => safeInt($twitterData['followersCount'] ?? null),
            'verified'  => (bool)($twitterData['isVerified'] ?? false),
        ];
    }
    return $found;
}

// ── Main analysis ────────────────────────────────────────────────────────────

function analyzeAccount(string $username, bool $verbose = false): array
{
    $result = [
        'follower_count'        => 0,
        'posts_analyzed'        => 0,
        'reels_performance'     => false,
        'reels_90d_count'       => 0,
        'reels_above_150pct'    => 0,
        'low_performing_reels'  => false,
        'bottom10_avg_views'    => 0.0,
        'post_engagement'       => false,
        'engagement_rate'       => 0.0,
        'total_interactions'    => 0,
        'monetization'          => false,
        'monetization_signals'  => [],
        'monetization_reason'   => '',
        'offer_type'            => 'unknown',
        'funnel_type'           => 'unknown',
        'business_model'        => 'unknown',
        'audience_type'         => 'unknown',
        'monetization_strength' => 'none',
        'platform_mix'          => 'instagram_only',
        'primary_domain'        => '',
        'language'              => 'unknown',
        'geo_hint'              => 'unknown',
        'icp'                   => 'unknown',
        'c4_reason'             => '',
        'qualified'             => false,
        'error'                 => '',
    ];
    $username = ltrim($username, '@');

    if ($verbose) echo "\n" . str_repeat('═', 56) . "\n  Анализ: @{$username}\n" . str_repeat('═', 56) . "\n";

    // Step 1: profile
    if ($verbose) echo "  [1/3] Загружаю профиль... ";
    $profile = fetchProfile($username);

    if (!empty($profile['error']) || empty($profile['username'])) {
        $result['error'] = 'Профиль не найден: ' . ($profile['error'] ?? 'unknown');
        if ($verbose) echo "❌ {$result['error']}\n";
        return $result;
    }

    $followerCount = safeInt($profile['followerCount'] ?? null);
    $biography     = (string)($profile['biography'] ?? '');
    $externalUrl   = (string)($profile['externalUrl'] ?? '');
    $igId          = $profile['_ig_id'] ?? $profile['id'] ?? null;
    $realtimeMode  = !empty($profile['_realtime']);
    $result['follower_count'] = $followerCount;

    if ($verbose) {
        $rt = $realtimeMode ? ' [real-time]' : '';
        echo "✓ " . ($profile['fullName'] ?? '') . " | " . number_format($followerCount) . " подписчиков{$rt}\n";
    }

    if ($followerCount === 0) {
        $result['error'] = 'followerCount = 0, аккаунт приватный или не найден';
        if ($verbose) echo "  ⚠ {$result['error']}\n";
        return $result;
    }

    // Step 2: posts
    if ($verbose) echo "  [2/3] Загружаю последние " . POSTS_LIMIT . " постов... ";
    $posts = fetchPosts($username, $realtimeMode ? (string)$igId : null);
    $result['posts_analyzed'] = count($posts);
    if ($verbose) echo "✓ " . count($posts) . " постов получено\n";

    // Criteria 1-3
    [$c1, $reels90d, $reelsAbove] = criterion1ReelsPerformance($posts, $followerCount);
    $result['reels_performance']  = $c1;
    $result['reels_90d_count']    = $reels90d;
    $result['reels_above_150pct'] = $reelsAbove;

    [$c2, $bottom10Avg] = criterion2LowPerforming($posts, $followerCount);
    $result['low_performing_reels'] = $c2;
    $result['bottom10_avg_views']   = $bottom10Avg;

    [$c3, $engRate, $totalInteractions] = criterion3Engagement($posts, $followerCount);
    $result['post_engagement']    = $c3;
    $result['engagement_rate']    = $engRate;
    $result['total_interactions'] = $totalInteractions;

    // Criterion 4
    if ($verbose) echo "  [3/3] Анализ монетизации... ";
    [$c4, $c4details] = criterion4Monetization($posts, $biography, $externalUrl);
    if ($verbose) echo "✓\n";

    // Criterion 5
    $otherSocials = criterion5OtherSocials($username, $biography, $externalUrl);

    $captionsText = '';
    $capArr = [];
    foreach ($posts as $p) {
        $cap = (string)($p['caption'] ?? '');
        if ($cap !== '') $capArr[] = mb_substr($cap, 0, 200);
    }
    $captionsText = implode("\n---\n", $capArr);

    $taxonomy = normalizeTaxonomyResult($c4details, $biography, $externalUrl, $otherSocials, $captionsText);

    $result['monetization']          = $c4 || $taxonomy['has_signals'];
    $result['monetization_signals']  = $taxonomy['signals_found'];
    $result['monetization_reason']   = $taxonomy['reasoning'];
    $result['offer_type']            = $taxonomy['offer_type'];
    $result['funnel_type']           = $taxonomy['funnel_type'];
    $result['business_model']        = $taxonomy['business_model'];
    $result['audience_type']         = $taxonomy['audience_type'];
    $result['monetization_strength'] = $taxonomy['monetization_strength'];
    $result['platform_mix']          = $taxonomy['platform_mix'];
    $result['primary_domain']        = $taxonomy['primary_domain'];
    $result['language']              = $taxonomy['language'];
    $result['geo_hint']              = $taxonomy['geo_hint'];
    $result['icp']                   = $taxonomy['icp'];
    $result['c4_reason']             = $taxonomy['c4_reason'] ?? '';
    $result['qualified']             = $c1 && $c2 && $c3 && $result['monetization'] && $result['error'] === '';

    if ($verbose) printResults($result, $followerCount);
    return $result;
}

function printResults(array $r, int $fc): void
{
    $yn = fn($v) => $v ? 'TRUE  ✅' : 'FALSE ❌';
    $sep = str_repeat('─', 56);

    echo "\n{$sep}\n  РЕЗУЛЬТАТЫ — " . number_format($fc) . " подписчиков\n{$sep}\n";

    $thr = (int)($fc * 1.5);
    echo "\n  1. Reels performance (90д ≥150% views): {$yn($r['reels_performance'])}\n";
    echo "     Порог: " . number_format($thr) . " просмотров\n";
    echo "     Reels в 90д: {$r['reels_90d_count']}, прошли порог: {$r['reels_above_150pct']}/5\n";

    $thr2 = (int)($fc * 0.15);
    echo "\n  2. Low-performing Reels (bottom-10 avg):  {$yn($r['low_performing_reels'])}\n";
    echo "     Avg bottom-10: " . number_format($r['bottom10_avg_views']) . "  |  порог 15%: " . number_format($thr2) . "\n";

    echo "\n  3. Post engagement (last 15):             {$yn($r['post_engagement'])}\n";
    echo "     Rate: {$r['engagement_rate']}%  |  порог: 1.50%\n";

    echo "\n  4. Monetization signals:                  {$yn($r['monetization'])}\n";
    if ($r['monetization_signals']) {
        foreach (array_slice($r['monetization_signals'], 0, 5) as $s) echo "     • {$s}\n";
    }
    if ($r['monetization_reason']) echo "     {$r['monetization_reason']}\n";
    if ($r['c4_reason'] ?? '') echo "     C4 reason: {$r['c4_reason']}\n";
    echo "     offer={$r['offer_type']}  funnel={$r['funnel_type']}  strength={$r['monetization_strength']}\n";
    echo "     audience={$r['audience_type']}  business={$r['business_model']}  icp={$r['icp']}\n";
    if ($r['primary_domain']) echo "     domain={$r['primary_domain']}  platform_mix={$r['platform_mix']}\n";

    echo "\n{$sep}\n";
    echo $r['qualified'] ? "  ✅ КВАЛИФИЦИРОВАН\n" : "  ❌ НЕ КВАЛИФИЦИРОВАН\n";
    echo "{$sep}\n";
}

// ── DB save ──────────────────────────────────────────────────────────────────

function saveResult(int $rowid, array $result): void
{
    $pdo = getDb();
    $pdo->prepare("
        UPDATE `" . DB_TABLE . "` SET
            `xpoz_scraped`               = 1,
            `xpoz_follower_count`        = ?,
            `xpoz_reels_performance`     = ?,
            `xpoz_low_performing_reels`  = ?,
            `xpoz_post_engagement`       = ?,
            `xpoz_monetization`          = ?,
            `xpoz_engagement_rate`       = ?,
            `xpoz_icp`                   = ?,
            `xpoz_offer_type`            = ?,
            `xpoz_funnel_type`           = ?,
            `xpoz_business_model`        = ?,
            `xpoz_audience_type`         = ?,
            `xpoz_monetization_strength` = ?,
            `xpoz_monetization_signals`  = ?,
            `xpoz_monetization_reason`   = ?,
            `xpoz_c4_reason`             = ?,
            `xpoz_platform_mix`          = ?,
            `xpoz_primary_domain`        = ?,
            `xpoz_language`              = ?,
            `xpoz_geo_hint`              = ?,
            `xpoz_qualified`             = ?,
            `xpoz_error`                 = ?,
            `xpoz_analyzed_at`           = NOW()
        WHERE `_rowid` = ?
    ")->execute([
        $result['follower_count'],
        (int)$result['reels_performance'],
        (int)$result['low_performing_reels'],
        (int)$result['post_engagement'],
        (int)$result['monetization'],
        $result['engagement_rate'],
        $result['icp'] ?: 'unknown',
        $result['offer_type'] ?: 'unknown',
        $result['funnel_type'] ?: 'unknown',
        $result['business_model'] ?: 'unknown',
        $result['audience_type'] ?: 'unknown',
        $result['monetization_strength'] ?: 'none',
        $result['monetization_signals'] ? json_encode($result['monetization_signals'], JSON_UNESCAPED_UNICODE) : null,
        $result['monetization_reason'] ?: null,
        $result['c4_reason'] ?: null,
        $result['platform_mix'] ?: 'instagram_only',
        $result['primary_domain'] ?: null,
        $result['language'] ?: 'unknown',
        $result['geo_hint'] ?: 'unknown',
        (int)$result['qualified'],
        $result['error'] ?: null,
        $rowid,
    ]);
}

// ── Stats ────────────────────────────────────────────────────────────────────

function showStats(): void
{
    $pdo = getDb();
    $total = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `instagram_parcing` IS NOT NULL AND `instagram_parcing` != ''")->fetchColumn();
    $scraped = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `xpoz_scraped` = 1")->fetchColumn();
    $qualified = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `xpoz_qualified` = 1")->fetchColumn();
    $withErrors = (int)$pdo->query("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE `xpoz_scraped` = 1 AND `xpoz_error` IS NOT NULL AND TRIM(`xpoz_error`) != ''")->fetchColumn();

    $bar = str_repeat('═', 50);
    echo "\n{$bar}\n  СТАТИСТИКА xpoz парсера\n{$bar}\n";
    echo "  Всего аккаунтов с IG:   " . number_format($total) . "\n";
    echo "  Проанализировано:        " . number_format($scraped) . "\n";
    echo "  Квалифицированных:       " . number_format($qualified) . "\n";
    echo "  С ошибками:              " . number_format($withErrors) . "\n";
    echo "  Осталось:                " . number_format(max(0, $total - $scraped)) . "\n";

    $icpRows = $pdo->query("
        SELECT `xpoz_icp`, COUNT(*) AS cnt
        FROM `" . DB_TABLE . "`
        WHERE `xpoz_qualified` = 1
        GROUP BY `xpoz_icp`
        ORDER BY cnt DESC
    ")->fetchAll();
    if ($icpRows) {
        echo "\n  ICP-сегменты (квалифицированные):\n";
        foreach ($icpRows as $row) {
            echo "    " . str_pad($row['xpoz_icp'] ?? '?', 8) . str_pad($row['cnt'], 5, ' ', STR_PAD_LEFT) . "\n";
        }
    }

    $offerRows = $pdo->query("
        SELECT `xpoz_offer_type`, COUNT(*) AS cnt
        FROM `" . DB_TABLE . "`
        WHERE `xpoz_monetization` = 1
        GROUP BY `xpoz_offer_type`
        ORDER BY cnt DESC
    ")->fetchAll();
    if ($offerRows) {
        echo "\n  Типы офферов (с монетизацией):\n";
        foreach ($offerRows as $row) {
            echo "    " . str_pad($row['xpoz_offer_type'] ?? '?', 24) . str_pad($row['cnt'], 5, ' ', STR_PAD_LEFT) . "\n";
        }
    }
    echo "\n  DB: " . DB_NAME . " › " . DB_TABLE . "\n{$bar}\n\n";
}

// ── CLI ──────────────────────────────────────────────────────────────────────

$workerIdx    = 0;
$totalWorkers = 1;
$limit        = 0;
$singleUser   = '';
$showStatsOnly = false;
$reanalyze    = false;
$requireEmail = false;
$rangeFrom    = 0;
$rangeTo      = 0;

foreach ($argv as $arg) {
    if (preg_match('/^--worker=(\d+)$/',        $arg, $m)) $workerIdx    = (int)$m[1];
    if (preg_match('/^--total-workers=(\d+)$/',  $arg, $m)) $totalWorkers = (int)$m[1];
    if (preg_match('/^--limit=(\d+)$/',          $arg, $m)) $limit        = (int)$m[1];
    if (preg_match('/^--username=(.+)$/',        $arg, $m)) $singleUser   = trim($m[1]);
    if (preg_match('/^--from=(\d+)$/',           $arg, $m)) $rangeFrom    = (int)$m[1];
    if (preg_match('/^--to=(\d+)$/',             $arg, $m)) $rangeTo      = (int)$m[1];
    if ($arg === '--stats')        $showStatsOnly = true;
    if ($arg === '--reanalyze')    $reanalyze      = true;
    if ($arg === '--require-email') $requireEmail  = true;
}

$workerLabel = $totalWorkers > 1 ? "[W{$workerIdx}/{$totalWorkers}] " : "";

// Ensure DB columns exist
$pdo = getDb();
ensureColumns();

if ($showStatsOnly) {
    showStats();
    exit(0);
}

// Single account mode
if ($singleUser !== '') {
    $username = extractUsername($singleUser);
    if ($username === '') {
        echo "  ❌ Невалидный username: {$singleUser}\n";
        exit(1);
    }
    $result = analyzeAccount($username, true);

    $h = strtolower($username);
    $row = $pdo->prepare("
        SELECT `_rowid` FROM `" . DB_TABLE . "`
        WHERE `instagram_parcing` IS NOT NULL AND TRIM(`instagram_parcing`) != ''
          AND (
            LOCATE(?, LOWER(`instagram_parcing`)) > 0
            OR LOCATE(?, LOWER(`instagram_parcing`)) > 0
            OR LOCATE(?, LOWER(`instagram_parcing`)) > 0
          )
        ORDER BY `_rowid` ASC
        LIMIT 1
    ");
    $row->execute([
        'instagram.com/' . $h,
        'instagram.com/@' . $h,
        '@' . $h,
    ]);
    $found = $row->fetch();
    if ($found) {
        saveResult((int)$found['_rowid'], $result);
        echo "  ✓ Сохранено в " . DB_TABLE . " (rowid={$found['_rowid']})\n";
    } else {
        echo "  ⚠ Аккаунт @{$username} не найден в " . DB_TABLE . ", результат не сохранён\n";
    }
    exit($result['error'] ? 1 : 0);
}

// ── Batch mode ───────────────────────────────────────────────────────────────

$eligibleCondition = "
    `instagram_parcing` IS NOT NULL
    AND TRIM(`instagram_parcing`) != ''
";
if (!$reanalyze) {
    $eligibleCondition .= " AND `xpoz_scraped` IS NULL";
}
if ($requireEmail) {
    $eligibleCondition .= ' AND ' . trim(XPOZ_ELIGIBLE_HAS_EMAIL_SQL);
}
$eligibleCondition .= " AND (`_rowid` % :tw) = :wi";

// --from / --to: resolve ordinal positions to _rowid boundaries
$rangeRowidFrom = 0;
$rangeRowidTo   = 0;
if ($rangeFrom > 0 || $rangeTo > 0) {
    $allIds = $pdo->prepare("
        SELECT `_rowid` FROM `" . DB_TABLE . "`
        WHERE {$eligibleCondition}
        ORDER BY `_rowid` ASC
    ");
    $allIds->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
    $allRowIds = $allIds->fetchAll(PDO::FETCH_COLUMN);
    $cnt = count($allRowIds);

    $idxFrom = max(0, $rangeFrom - 1);
    $idxTo   = $rangeTo > 0 ? min($cnt - 1, $rangeTo - 1) : $cnt - 1;

    if ($idxFrom < $cnt) {
        $rangeRowidFrom = (int)$allRowIds[$idxFrom];
        $rangeRowidTo   = (int)$allRowIds[$idxTo];
    }
    $eligibleCondition .= " AND `_rowid` >= {$rangeRowidFrom} AND `_rowid` <= {$rangeRowidTo}";
    echo "{$workerLabel}Диапазон: аккаунт #{$rangeFrom}–#{$rangeTo} (_rowid {$rangeRowidFrom}..{$rangeRowidTo})\n";
}

$totalStmt = $pdo->prepare("SELECT COUNT(*) FROM `" . DB_TABLE . "` WHERE {$eligibleCondition}");
$totalStmt->execute([':tw' => $totalWorkers, ':wi' => $workerIdx]);
$total = (int)$totalStmt->fetchColumn();

if ($limit > 0 && $total > $limit) $total = $limit;

echo "{$workerLabel}xpoz_parser.php — Instagram qualifier via Xpoz API\n";
if ($requireEmail) {
    echo "{$workerLabel}Фильтр: только строки с email (сайт / YouTube / Twitter / Instagram bio)\n";
}
echo "{$workerLabel}Total accounts to process: {$total}\n";
echo "{$workerLabel}Estimated time: ~" . ceil($total * 18 / 60) . " min\n\n";

if ($total === 0) {
    echo "{$workerLabel}Нет аккаунтов для анализа.\n";
    if (!$reanalyze) echo "{$workerLabel}Используй --reanalyze для повторного запуска.\n";
    exit(0);
}

$fetchBatchStmt = $pdo->prepare("
    SELECT `_rowid`, `instructor`, `instagram_parcing`
    FROM `" . DB_TABLE . "`
    WHERE {$eligibleCondition}
      AND `_rowid` > :lastRowId
    ORDER BY `_rowid` ASC
    LIMIT " . FETCH_BATCH_SIZE . "
");

$processed  = 0;
$okCount    = 0;
$errCount   = 0;
$qualCount  = 0;
$lastRowId  = 0;
$startTime  = microtime(true);

while ($processed < $total) {
    $fetchBatchStmt->execute([
        ':tw'        => $totalWorkers,
        ':wi'        => $workerIdx,
        ':lastRowId' => $lastRowId,
    ]);
    $rows = $fetchBatchStmt->fetchAll();
    if (!$rows) break;

    foreach ($rows as $row) {
        if ($limit > 0 && $processed >= $limit) break 2;

        $rowid      = (int)$row['_rowid'];
        $lastRowId  = $rowid;
        $instructor = $row['instructor'] ?? '';
        $igRaw      = $row['instagram_parcing'] ?? '';

        $username = extractUsername($igRaw);
        if ($username === '') {
            $pdo->prepare("UPDATE `" . DB_TABLE . "` SET `xpoz_scraped` = 1, `xpoz_error` = 'INVALID_USERNAME' WHERE `_rowid` = ?")
                ->execute([$rowid]);
            continue;
        }

        $processed++;
        $elapsed = microtime(true) - $startTime;
        $rate = $processed / max($elapsed, 1);
        $remaining = ($total - $processed) / max($rate, 0.001);
        $eta = gmdate('H:i:s', (int)$remaining);
        $pct = $total > 0 ? round($processed / $total * 100, 1) : 0;

        $result = analyzeAccount($username);
        saveResult($rowid, $result);

        $isOk = $result['error'] === '';
        if ($isOk) $okCount++; else $errCount++;
        if ($result['qualified']) $qualCount++;

        $c = fn($v) => $v ? 'T' : 'F';
        $icon = $isOk ? '✅' : '⚠';
        $qTag = $result['qualified'] ? ' QUALIFIED' : '';
        echo "{$workerLabel}  {$icon} @" . str_pad($username, 30) .
            " C1={$c($result['reels_performance'])}" .
            " C2={$c($result['low_performing_reels'])}" .
            " C3={$c($result['post_engagement'])}" .
            " C4={$c($result['monetization'])}" .
            "  fol=" . number_format($result['follower_count']) .
            "  icp={$result['icp']}{$qTag}\n";
        echo "{$workerLabel}     [{$processed}/{$total}] {$pct}%  ok={$okCount} err={$errCount} qual={$qualCount}  ETA={$eta}\n";

        usleep(DELAY_MS * 1000);
    }
}

$elapsedTotal = (int)(microtime(true) - $startTime);
$h = str_pad((string)intdiv($elapsedTotal, 3600), 2, '0', STR_PAD_LEFT);
$m = str_pad((string)intdiv($elapsedTotal % 3600, 60), 2, '0', STR_PAD_LEFT);
$s = str_pad((string)($elapsedTotal % 60), 2, '0', STR_PAD_LEFT);

echo "\n" . str_repeat('─', 64) . "\n";
echo "{$workerLabel}Готово за {$h}:{$m}:{$s}\n";
echo "{$workerLabel}Успешно: {$okCount}  |  Ошибок: {$errCount}  |  Квалифицированных: {$qualCount}\n";
echo str_repeat('─', 64) . "\n\n";
