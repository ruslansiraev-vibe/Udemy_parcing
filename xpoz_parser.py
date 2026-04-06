#!/usr/bin/env python3
"""
xpoz_parser.py — квалификация Instagram-аккаунтов из instagram_email_parcing.csv
через систему xpoz (Xpoz API + Claude Haiku).

Читает CSV (login, email), анализирует каждый аккаунт по 5 критериям,
сохраняет результаты в SQLite, экспортирует итоговый CSV с оценками.

Usage:
    python xpoz_parser.py                          # Анализ всех не-обработанных
    python xpoz_parser.py --limit 50               # Первые 50 аккаунтов
    python xpoz_parser.py --workers 10             # 10 параллельных потоков
    python xpoz_parser.py --username whop          # Один аккаунт
    python xpoz_parser.py --export                 # Экспорт результатов в CSV
    python xpoz_parser.py --stats                  # Статистика
    python xpoz_parser.py --reanalyze              # Перезапуск всех
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCRIPT_DIR = Path(__file__).resolve().parent
XPOZ_PROJECT = Path(os.environ.get(
    "XPOZ_PROJECT_DIR",
    "/Users/cursor3/pareser/business-development-tool-multi/xpoz",
))
DEPLOY_DIR = XPOZ_PROJECT / "deploy"

if str(DEPLOY_DIR) not in sys.path:
    sys.path.insert(0, str(DEPLOY_DIR))
if str(XPOZ_PROJECT) not in sys.path:
    sys.path.insert(0, str(XPOZ_PROJECT))

os.environ.setdefault("ANTHROPIC_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
os.environ.setdefault("XPOZ_API_KEY",
    "K3Bq78DmO0xoOhM2AXjw6S4VxhWJIKTkG9rhvrtgLbZNkl9BnnjxlSofgCTlhcrFjWbMim7")

from analyze_account import analyze, CriteriaResult

INPUT_CSV  = SCRIPT_DIR / "instagram_email_parcing.csv"
DB_PATH    = SCRIPT_DIR / "xpoz_results.db"
OUTPUT_CSV = SCRIPT_DIR / "xpoz_qualified.csv"

OUTPUT_COLUMNS = [
    "username", "email", "follower_count",
    "reels_performance", "low_performing_reels", "post_engagement", "monetization",
    "icp", "offer_type", "funnel_type", "business_model", "audience_type",
    "monetization_strength", "engagement_rate_pct",
    "monetization_reason", "primary_domain", "platform_mix",
    "language", "geo_hint",
    "reels_90d_count", "reels_above_150pct", "bottom10_avg_views",
    "total_interactions",
    "cta_keywords", "bio_keywords", "monetization_signals",
    "youtube_url", "twitter_url", "twitter_followers",
    "analyzed_at", "error",
    "qualified",
]


# ── SQLite ────────────────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            login TEXT PRIMARY KEY,
            email TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT,
            analyzed_at TEXT,
            follower_count INTEGER,
            posts_analyzed INTEGER,
            reels_performance INTEGER,
            reels_90d_count INTEGER,
            reels_above_150pct INTEGER,
            low_performing_reels INTEGER,
            bottom10_avg_views REAL,
            post_engagement INTEGER,
            engagement_rate_pct REAL,
            total_interactions INTEGER,
            monetization INTEGER,
            monetization_signals TEXT,
            monetization_reason TEXT,
            offer_type TEXT,
            offer_type_confidence REAL,
            funnel_type TEXT,
            business_model TEXT,
            audience_type TEXT,
            monetization_strength TEXT,
            platform_mix TEXT,
            primary_domain TEXT,
            bio_keywords TEXT,
            cta_keywords TEXT,
            language TEXT,
            geo_hint TEXT,
            icp TEXT,
            youtube_url TEXT,
            twitter_url TEXT,
            twitter_followers INTEGER,
            other_socials TEXT,
            error TEXT,
            qualified INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_res_username ON results(username)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_res_qualified ON results(qualified)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_res_icp ON results(icp)")
    conn.commit()


def _sync_csv(conn: sqlite3.Connection, csv_path: Path) -> int:
    """Импорт username + email из CSV в таблицу accounts."""
    conn.execute("DELETE FROM accounts")
    count = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            login = (row.get("login") or row.get("instagram") or row.get("username") or "").strip().lstrip("@")
            email = (row.get("email") or row.get("email_parcing") or "").strip()
            if not login:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO accounts (login, email) VALUES (?, ?)",
                (login, email),
            )
            count += 1
    conn.commit()
    return count


def _get_analyzed_set(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT username FROM results WHERE error IS NULL").fetchall()
    return {str(row["username"]) for row in rows}


def _get_email(conn: sqlite3.Connection, username: str) -> str:
    row = conn.execute("SELECT email FROM accounts WHERE login = ?", (username,)).fetchone()
    return str(row["email"]) if row else ""


def _get_all_usernames(conn: sqlite3.Connection, limit: int) -> list[str]:
    rows = conn.execute(
        "SELECT login FROM accounts WHERE TRIM(COALESCE(login, '')) != '' ORDER BY rowid LIMIT ?",
        (limit,),
    ).fetchall()
    return [str(row["login"]) for row in rows]


def _save_result(conn: sqlite3.Connection, username: str, email: str,
                 res: CriteriaResult) -> None:
    socials = res.other_socials or {}
    twitter = socials.get("twitter", {})
    qualified = int(
        bool(res.reels_performance)
        and bool(res.low_performing_reels)
        and bool(res.post_engagement)
        and bool(res.monetization)
        and not res.error
    )
    conn.execute("DELETE FROM results WHERE username = ?", (username,))
    conn.execute("""
        INSERT INTO results (
            username, email, analyzed_at, follower_count, posts_analyzed,
            reels_performance, reels_90d_count, reels_above_150pct,
            low_performing_reels, bottom10_avg_views,
            post_engagement, engagement_rate_pct, total_interactions,
            monetization, monetization_signals, monetization_reason,
            offer_type, offer_type_confidence, funnel_type,
            business_model, audience_type, monetization_strength,
            platform_mix, primary_domain, bio_keywords, cta_keywords,
            language, geo_hint, icp,
            youtube_url, twitter_url, twitter_followers,
            other_socials, error, qualified
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
    """, (
        username, email,
        res.analyzed_at or datetime.utcnow().isoformat(),
        res.follower_count, res.posts_analyzed,
        int(bool(res.reels_performance)), res.reels_90d_count, res.reels_above_150pct,
        int(bool(res.low_performing_reels)), res.bottom10_avg_views,
        int(bool(res.post_engagement)), res.engagement_rate_pct, res.total_interactions,
        int(bool(res.monetization)),
        json.dumps(res.monetization_signals, ensure_ascii=False) if res.monetization_signals else None,
        res.monetization_reason or None,
        res.offer_type or "unknown", res.offer_type_confidence or 0.0,
        res.funnel_type or "unknown",
        res.business_model or "unknown", res.audience_type or "unknown",
        res.monetization_strength or "none",
        res.platform_mix or "instagram_only", res.primary_domain or None,
        json.dumps(res.bio_keywords, ensure_ascii=False) if res.bio_keywords else None,
        json.dumps(res.cta_keywords, ensure_ascii=False) if res.cta_keywords else None,
        res.language or "unknown", res.geo_hint or "unknown",
        res.icp or "unknown",
        socials.get("youtube"),
        twitter.get("url") if isinstance(twitter, dict) else None,
        twitter.get("followers") if isinstance(twitter, dict) else None,
        json.dumps(socials, ensure_ascii=False) if socials else None,
        res.error or None,
        qualified,
    ))
    conn.commit()


# ── Прогресс ──────────────────────────────────────────────────────────────────

class Progress:
    def __init__(self, total: int):
        self._lock = threading.Lock()
        self.total = total
        self.done = 0
        self.ok = 0
        self.errors = 0
        self.qualified = 0
        self._start = time.time()

    def update(self, *, success: bool, is_qualified: bool):
        with self._lock:
            self.done += 1
            if success:
                self.ok += 1
            else:
                self.errors += 1
            if is_qualified:
                self.qualified += 1

    def line(self) -> str:
        elapsed = time.time() - self._start
        rate = self.done / elapsed if elapsed > 0 else 0
        remaining = (self.total - self.done) / rate if rate > 0 else 0
        m, s = divmod(int(remaining), 60)
        h, m = divmod(m, 60)
        pct = self.done / self.total * 100 if self.total else 0
        return (f"[{self.done:>5}/{self.total}] {pct:5.1f}%  "
                f"ok={self.ok} err={self.errors} qualified={self.qualified}  "
                f"ETA={h:02d}:{m:02d}:{s:02d}")


# ── Воркер ────────────────────────────────────────────────────────────────────

_db_lock = threading.Lock()


def _worker(username: str, email: str, progress: Progress, verbose: bool) -> dict:
    try:
        result = analyze(username, verbose=verbose)

        qualified = (
            bool(result.reels_performance)
            and bool(result.low_performing_reels)
            and bool(result.post_engagement)
            and bool(result.monetization)
            and not result.error
        )

        with _db_lock:
            conn = _connect()
            try:
                _save_result(conn, username, email, result)
            finally:
                conn.close()

        is_ok = not result.error
        progress.update(success=is_ok, is_qualified=qualified)

        c = lambda v: "T" if v else "F"
        tag = "QUALIFIED" if qualified else ""
        icon = "✅" if is_ok else "⚠"
        print(
            f"  {icon} @{username:<30} C1={c(result.reels_performance)} "
            f"C2={c(result.low_performing_reels)} C3={c(result.post_engagement)} "
            f"C4={c(result.monetization)}  fol={result.follower_count:,}  "
            f"icp={result.icp}  {tag}",
            flush=True,
        )
        print(f"     {progress.line()}", flush=True)

        return {"username": username, "ok": is_ok}
    except Exception as e:
        progress.update(success=False, is_qualified=False)
        print(f"  ❌ @{username}: {e}", flush=True)
        print(f"     {progress.line()}", flush=True)
        return {"username": username, "ok": False, "error": str(e)}


# ── Batch ─────────────────────────────────────────────────────────────────────

def run_batch(usernames: list[str], emails: dict[str, str],
              workers: int = 20, verbose: bool = False) -> dict:
    total = len(usernames)
    progress = Progress(total)

    print(f"\n{'═' * 64}")
    print(f"  xpoz парсер: {total} аккаунтов  |  workers={workers}")
    print(f"  Ожидаемое время: ~{total * 18 // workers // 60} мин")
    print(f"  База: {DB_PATH}")
    print(f"{'═' * 64}\n")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_worker, uname, emails.get(uname, ""), progress, verbose): uname
            for uname in usernames
        }
        try:
            for fut in as_completed(futures):
                fut.result()
        except KeyboardInterrupt:
            print("\n  ⏹ Прерван. Ожидаю завершения активных задач...")
            pool.shutdown(wait=False, cancel_futures=True)

    elapsed = int(time.time() - progress._start)
    h, r = divmod(elapsed, 3600)
    m, s = divmod(r, 60)

    print(f"\n{'─' * 64}")
    print(f"  Готово за {h:02d}:{m:02d}:{s:02d}")
    print(f"  Успешно: {progress.ok}  |  Ошибок: {progress.errors}")
    print(f"  Квалифицированных (C1+C2+C3+C4): {progress.qualified}")
    print(f"  Результаты: {DB_PATH}")
    print(f"{'─' * 64}\n")

    return {
        "ok": progress.ok,
        "errors": progress.errors,
        "qualified": progress.qualified,
        "elapsed": elapsed,
    }


# ── Экспорт ───────────────────────────────────────────────────────────────────

def export_csv(output_path: Path = OUTPUT_CSV) -> int:
    conn = _connect()
    try:
        _init_db(conn)
        rows = conn.execute("""
            SELECT r.*, a.email AS a_email
            FROM results r
            LEFT JOIN accounts a ON a.login = r.username
            ORDER BY r.qualified DESC, r.follower_count DESC
        """).fetchall()
    finally:
        conn.close()

    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            d = dict(row)
            email = d.get("email") or d.get("a_email") or ""
            for col in ("monetization_signals", "bio_keywords", "cta_keywords"):
                val = d.get(col)
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        d[col] = "; ".join(str(x) for x in parsed) if isinstance(parsed, list) else val
                    except json.JSONDecodeError:
                        pass

            socials = d.get("other_socials")
            twitter = {}
            if socials and isinstance(socials, str):
                try:
                    socials_parsed = json.loads(socials)
                    twitter = socials_parsed.get("twitter", {})
                    d["youtube_url"] = socials_parsed.get("youtube", d.get("youtube_url", ""))
                except json.JSONDecodeError:
                    pass

            out = {}
            for col in OUTPUT_COLUMNS:
                if col == "email":
                    out[col] = email
                elif col == "twitter_url":
                    out[col] = twitter.get("url", d.get("twitter_url", "")) if isinstance(twitter, dict) else ""
                elif col == "twitter_followers":
                    out[col] = twitter.get("followers", d.get("twitter_followers", "")) if isinstance(twitter, dict) else ""
                elif col == "qualified":
                    out[col] = "YES" if d.get("qualified") else "NO"
                elif col in ("reels_performance", "low_performing_reels", "post_engagement", "monetization"):
                    out[col] = "TRUE" if d.get(col) else "FALSE"
                else:
                    out[col] = d.get(col, "")
            writer.writerow(out)

    return len(rows)


def show_stats() -> None:
    conn = _connect()
    try:
        _init_db(conn)
        total = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        qualified = conn.execute("SELECT COUNT(*) FROM results WHERE qualified = 1").fetchone()[0]
        with_errors = conn.execute("SELECT COUNT(*) FROM results WHERE error IS NOT NULL AND TRIM(error) != ''").fetchone()[0]
        accounts = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]

        icp_rows = conn.execute("""
            SELECT icp, COUNT(*) as cnt
            FROM results
            WHERE qualified = 1
            GROUP BY icp
            ORDER BY cnt DESC
        """).fetchall()

        offer_rows = conn.execute("""
            SELECT offer_type, COUNT(*) as cnt
            FROM results
            WHERE monetization = 1
            GROUP BY offer_type
            ORDER BY cnt DESC
        """).fetchall()
    finally:
        conn.close()

    print(f"\n{'═' * 50}")
    print(f"  СТАТИСТИКА xpoz парсера")
    print(f"{'═' * 50}")
    print(f"  Аккаунтов в CSV:        {accounts:,}")
    print(f"  Проанализировано:        {total:,}")
    print(f"  Квалифицированных:       {qualified:,}")
    print(f"  С ошибками:              {with_errors:,}")
    print(f"  Осталось:                {max(0, accounts - total):,}")

    if icp_rows:
        print(f"\n  ICP-сегменты (квалифицированные):")
        for row in icp_rows:
            print(f"    {row['icp']:<8} {row['cnt']:>5}")

    if offer_rows:
        print(f"\n  Типы офферов (с монетизацией):")
        for row in offer_rows:
            print(f"    {row['offer_type']:<24} {row['cnt']:>5}")

    print(f"\n  SQLite: {DB_PATH}")
    print(f"{'═' * 50}\n")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="xpoz парсер — квалификация Instagram-аккаунтов из CSV",
    )
    parser.add_argument("--csv", default=str(INPUT_CSV),
                        help=f"Путь к CSV (default: {INPUT_CSV.name})")
    parser.add_argument("--username", help="Анализ одного аккаунта")
    parser.add_argument("--limit", type=int, default=0,
                        help="Макс. кол-во аккаунтов (0 = все)")
    parser.add_argument("--workers", type=int, default=20,
                        help="Параллельных потоков (default: 20)")
    parser.add_argument("--reanalyze", action="store_true",
                        help="Перезапустить анализ (включая уже обработанные)")
    parser.add_argument("--export", action="store_true",
                        help="Экспорт результатов в CSV")
    parser.add_argument("--stats", action="store_true",
                        help="Показать статистику")
    parser.add_argument("--verbose", action="store_true",
                        help="Подробный вывод для каждого аккаунта")
    args = parser.parse_args()

    conn = _connect()
    try:
        _init_db(conn)
    finally:
        conn.close()

    if args.stats:
        show_stats()
        return

    if args.export:
        count = export_csv()
        print(f"  ✓ Экспортировано {count} записей → {OUTPUT_CSV}")
        return

    if args.username:
        email = ""
        conn = _connect()
        try:
            email = _get_email(conn, args.username)
        finally:
            conn.close()

        result = analyze(args.username, verbose=True)
        conn = _connect()
        try:
            _save_result(conn, args.username, email, result)
        finally:
            conn.close()

        qualified = (
            bool(result.reels_performance)
            and bool(result.low_performing_reels)
            and bool(result.post_engagement)
            and bool(result.monetization)
            and not result.error
        )
        print(f"\n  {'✅ КВАЛИФИЦИРОВАН' if qualified else '❌ НЕ КВАЛИФИЦИРОВАН'}")
        print(f"  ICP: {result.icp}  |  offer: {result.offer_type}  |  strength: {result.monetization_strength}")
        print(f"  ✓ Сохранено в {DB_PATH}")
        return

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"  ❌ CSV файл не найден: {csv_path}", file=sys.stderr)
        sys.exit(1)

    conn = _connect()
    try:
        imported = _sync_csv(conn, csv_path)
        print(f"  Импортировано из CSV: {imported:,} аккаунтов")

        all_usernames = _get_all_usernames(conn, limit=args.limit if args.limit > 0 else 999999)
        emails_map = {}
        for u in all_usernames:
            emails_map[u] = _get_email(conn, u)

        if not args.reanalyze:
            done = _get_analyzed_set(conn)
            before = len(all_usernames)
            all_usernames = [u for u in all_usernames if u not in done]
            skipped = before - len(all_usernames)
            if skipped:
                print(f"  Пропущено уже проанализированных: {skipped:,}")
    finally:
        conn.close()

    if not all_usernames:
        print("  Нет аккаунтов для анализа (все обработаны или список пуст)")
        print("  Используй --reanalyze для повторного запуска")
        return

    print(f"  Аккаунтов для анализа: {len(all_usernames):,}")
    run_batch(all_usernames, emails_map, workers=args.workers, verbose=args.verbose)

    count = export_csv()
    print(f"  ✓ Автоэкспорт: {count} записей → {OUTPUT_CSV}")
    show_stats()


if __name__ == "__main__":
    main()
