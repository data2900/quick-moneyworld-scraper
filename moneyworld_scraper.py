# moneyworld_scraper.py
# 1本で「全件取得(all) / 未取得のみ(missing)」に対応する Playwright 版
# - 未指定時は consensus_url の最新 target_date を自動採用
# - 礼儀最優先: QPS制御・並列上限・画像/フォント遮断・堅牢待機・指数バックオフ
# - 高速化: WAL + バッチコミット (--batch)
# - liquidity は dd/span[3] に修正済み

import asyncio
import datetime
import sqlite3
import time
import os
import argparse
from typing import Dict, Any, List, Tuple, Iterable, Optional
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright, TimeoutError as PwTimeout

DB_PATH = "/market_data.db"

# ====== ポライト設定（サイト負荷優先）======
DEFAULT_MAX_CONCURRENCY = 4   # 4〜6推奨
DEFAULT_TARGET_QPS = 0.7      # 全体QPS上限（0.6〜0.9推奨）
DEFAULT_BATCH_COMMIT = 100    # DBコミット間隔
NAV_TIMEOUT_MS = 25000        # ナビゲーション/待機タイムアウト
RETRIES = 3                   # 最大リトライ回数

# 必須セレクタ（レンダ完了目安）
SEL_STOCK_PAGE = "#stock-page"
SEL_READY = 'xpath=//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[1]/dd/span[3]'

# 取得XPaths（liquidity は dd/span[3]）
XPATHS: Dict[str, str] = {
    "rating":      'string(//*[@id="stock-page"]/div[1]/div[3]/div[1]/section/div[1]/div[2]/div[3]/div[1]/span)',
    "sales":       'string(//*[@id="stock-page"]/div[1]/div[3]/div[1]/section/div[1]/div[2]/div[3]/div[2]/div[2]/div[1]/span)',
    "profit":      'string(//*[@id="stock-page"]/div[1]/div[3]/div[1]/section/div[1]/div[2]/div[3]/div[2]/div[2]/div[2]/span)',
    "scale":       'string(//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[1]/dd/span[3])',
    "cheap":       'string(//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[2]/dd/span[3])',
    "growth":      'string(//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[3]/dd/span[3])',
    "profitab":    'string(//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[4]/dd/span[3])',
    "safety":      'string(//*[@id="sec3_quick_stock_info_0"]/div[2]/div/div/dl[5]/dd/span[3])',
    "risk":        'string(//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[1]/dd/span[3])',
    "return_rate": 'string(//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[2]/dd/span[3])',
    "liquidity":   'string(//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[3]/dd/span[3])',
    "trend":       'string(//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[4]/dd/span[3])',
    "forex":       'string(//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[5]/dd/span[3])',
    "technical":   'string(//*[@id="sec3_quick_stock_info_0"]/div[3]/div/div/dl[6]/dd/span[3])',
}

def _pct(s: str) -> str:
    s = (s or "").strip()
    if s and not s.endswith("%"):
        return s + "%"
    return s or ""

class TokenBucket:
    """単一プロセス内の簡易トークンバケットで全体QPSを制御"""
    def __init__(self, qps: float):
        self.interval = 1.0 / max(qps, 0.0001)
        self._lock = asyncio.Lock()
        self._next_time = time.monotonic()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)
            self._next_time = max(now, self._next_time) + self.interval

@asynccontextmanager
async def browser_context(play, headless=True):
    browser = await play.chromium.launch(
        headless=headless,
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
    )
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        java_script_enabled=True,
        bypass_csp=True,
        viewport={"width": 1366, "height": 768}
    )
    # 軽量化: 画像/フォント/メディア遮断（CSSとXHRは許可）
    async def route_handler(route, request):
        if request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", route_handler)
    try:
        yield context
    finally:
        await context.close()
        await browser.close()

async def fetch_one(page, code: str, url: str) -> Tuple[str, Dict[str, Any]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_selector(SEL_STOCK_PAGE, timeout=NAV_TIMEOUT_MS)
    await page.wait_for_selector(SEL_READY, timeout=NAV_TIMEOUT_MS)
    data = await page.evaluate(
        """(xps) => {
            const evalStr = (xp) => {
              try { return document.evaluate(xp, document, null, XPathResult.STRING_TYPE, null).stringValue.trim(); }
              catch(e){ return ""; }
            };
            const out = {};
            for (const [k, xp] of Object.entries(xps)) out[k] = evalStr(xp);
            return out;
        }""",
        XPATHS
    )
    data["sales"]  = _pct(data.get("sales", ""))
    data["profit"] = _pct(data.get("profit", ""))
    return code, data

async def worker(name: int,
                 ctx,
                 jobs: asyncio.Queue,
                 bucket: TokenBucket,
                 results: asyncio.Queue):
    page = await ctx.new_page()
    try:
        while True:
            item = await jobs.get()
            if item is None:
                break
            code, url = item
            await bucket.acquire()  # 全体QPS制御
            delay = 0.8
            last_err = None
            for attempt in range(RETRIES):
                try:
                    c, data = await fetch_one(page, code, url)
                    await results.put((c, data, None))
                    break
                except (PwTimeout, Exception) as e:
                    last_err = e
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(delay)
                        delay *= 1.8
                    else:
                        await results.put((code, None, last_err))
            jobs.task_done()
    finally:
        await page.close()

def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    # 書き込み高速化（WAL＋synchronous=NORMAL）
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    # moneyworld_reports
    cur.execute("""
        CREATE TABLE IF NOT EXISTS moneyworld_reports (
            target_date TEXT,
            code TEXT,
            rating TEXT,
            sales TEXT,
            profit TEXT,
            scale TEXT,
            cheap TEXT,
            growth TEXT,
            profitab TEXT,
            safety TEXT,
            risk TEXT,
            return_rate TEXT,
            liquidity TEXT,
            trend TEXT,
            forex TEXT,
            technical TEXT,
            PRIMARY KEY (target_date, code)
        )
    """)
    conn.commit()

def resolve_target_date(conn: sqlite3.Connection, explicit: Optional[str]) -> Optional[str]:
    """明示が無ければ consensus_url の最新日付を採用"""
    if explicit:
        # フォーマット検証
        datetime.datetime.strptime(explicit, "%Y%m%d")
        return explicit
    cur = conn.cursor()
    cur.execute("SELECT MAX(target_date) FROM consensus_url")
    row = cur.fetchone()
    td = row[0] if row else None
    if td:
        try:
            datetime.datetime.strptime(td, "%Y%m%d")
            return td
        except Exception:
            return None
    return None

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str) -> List[Tuple[str,str]]:
    cur = conn.cursor()
    if mode == "all":
        cur.execute("""
            SELECT code, quickurl
            FROM consensus_url
            WHERE target_date = ?
        """, (target_date,))
        rows = [(c, u) for c, u in cur.fetchall() if u]
        return rows

    # mode == "missing": nikkei_reports を母集合にして moneyworld_reports 未保存を抽出
    cur.execute("""
        SELECT code FROM nikkei_reports WHERE target_date = ?
        EXCEPT
        SELECT code FROM moneyworld_reports WHERE target_date = ?
    """, (target_date, target_date))
    codes = [r[0] for r in cur.fetchall()]
    if not codes:
        return []
    placeholders = ",".join(["?"] * len(codes))
    cur.execute(f"""
        SELECT code, quickurl FROM consensus_url
        WHERE target_date = ? AND code IN ({placeholders})
    """, [target_date] + codes)
    return [(c, u) for c, u in cur.fetchall() if u]

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--target_date", help="YYYYMMDD（未指定なら consensus_url の最新日付）")
    p.add_argument("--mode", choices=["missing", "all"], default="missing",
                   help="missing: nikkei基準で未取得のみ / all: consensus_url全件")
    p.add_argument("--concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY, help="同時タブ数（4〜6推奨）")
    p.add_argument("--qps", type=float, default=DEFAULT_TARGET_QPS, help="全体QPS上限（0.6〜0.9推奨）")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH_COMMIT, help="DBコミット間隔")
    p.add_argument("--headful", action="store_true", help="ヘッドフルで起動（デバッグ用）")
    args = p.parse_args()

    # DB 準備
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    cur = conn.cursor()

    # 対象日付の決定
    target_date = resolve_target_date(conn, args.target_date)
    if not target_date:
        print("❌ target_date を決定できません（-a YYYYMMDD を指定するか、consensus_url にデータが必要）")
        conn.close()
        return
    print(f"▶ target_date = {target_date} / mode = {args.mode}")

    # 対象URLのロード
    targets = load_targets(conn, target_date, args.mode)
    if not targets:
        if args.mode == "missing":
            print(f"✅ {target_date} 未取得はありません（mode=missing）")
        else:
            print(f"⚠️ {target_date} 対象URLが見つかりません（mode=all）")
        conn.close()
        return

    # キュー
    jobs: asyncio.Queue = asyncio.Queue()
    results: asyncio.Queue = asyncio.Queue()
    for item in targets:
        await jobs.put(item)
    total = len(targets)

    bucket = TokenBucket(args.qps)
    done = 0
    ok = 0
    ng = 0
    buf: List[Tuple] = []

    async with async_playwright() as play:
        async with browser_context(play, headless=not args.headful) as ctx:
            workers = [
                asyncio.create_task(worker(i+1, ctx, jobs, bucket, results))
                for i in range(max(1, min(args.concurrency, 12)))
            ]

            async def stop_workers():
                for _ in workers:
                    await jobs.put(None)

            try:
                while done < total:
                    code, data, err = await results.get()
                    done += 1
                    if err is None and data:
                        row = (
                            target_date, code,
                            data.get("rating",""), data.get("sales",""), data.get("profit",""),
                            data.get("scale",""), data.get("cheap",""), data.get("growth",""), data.get("profitab",""),
                            data.get("safety",""), data.get("risk",""), data.get("return_rate",""), data.get("liquidity",""),
                            data.get("trend",""), data.get("forex",""), data.get("technical","")
                        )
                        buf.append(row)
                        ok += 1
                        if len(buf) >= args.batch:
                            cur.executemany("""
                                INSERT OR REPLACE INTO moneyworld_reports (
                                    target_date, code, rating, sales, profit, scale, cheap, growth, profitab,
                                    safety, risk, return_rate, liquidity, trend, forex, technical
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, buf)
                            conn.commit()
                            buf.clear()
                        if done % 50 == 0 or done == total:
                            print(f"✅ {done}/{total} / OK:{ok} NG:{ng}")
                    else:
                        ng += 1
                        print(f"❌ {done}/{total} code:{code} err:{repr(err)[:120]}")
            finally:
                if buf:
                    cur.executemany("""
                        INSERT OR REPLACE INTO moneyworld_reports (
                            target_date, code, rating, sales, profit, scale, cheap, growth, profitab,
                            safety, risk, return_rate, liquidity, trend, forex, technical
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, buf)
                    conn.commit()
                await stop_workers()
                await asyncio.gather(*workers, return_exceptions=True)
                conn.close()
                print(f"🏁 完了 / OK:{ok} NG:{ng} / 対象:{total} / mode={args.mode} / date={target_date}")

if __name__ == "__main__":
    asyncio.run(main())
