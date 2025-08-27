# dcinside_incremental.py
# -*- coding: utf-8 -*-
import time, random, re, sqlite3, urllib.parse, argparse, sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

BASE = "https://gall.dcinside.com"
TZ = ZoneInfo("Asia/Seoul")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
}

def with_retry_get(url, session, max_try=3, sleep_base=1.2):
    for i in range(max_try):
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r
        except requests.RequestException:
            pass
        time.sleep(sleep_base * (i + 1) + random.random())
    return None

def ensure_db(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts(
        post_no INTEGER,
        gallery_id TEXT,
        url TEXT UNIQUE,
        title TEXT,
        author TEXT,
        author_ip TEXT,
        created_at TEXT,
        views INTEGER,
        upvotes INTEGER,
        downvotes INTEGER,
        comments_count INTEGER,
        content TEXT,
        images_json TEXT,
        crawled_at TEXT,
        PRIMARY KEY (gallery_id, post_no)
    )
    """)
    # optional: 크롤 상태 저장
    cur.execute("""
    CREATE TABLE IF NOT EXISTS crawl_state(
        gallery_id TEXT PRIMARY KEY,
        last_max_post_no INTEGER,
        updated_at TEXT
    )
    """)
    conn.commit()
    return conn

def get_last_max_post_no(conn, gallery_id):
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(post_no), 0) FROM posts WHERE gallery_id=?", (gallery_id,))
    row = cur.fetchone()
    return int(row[0] or 0)

def update_crawl_state(conn, gallery_id, last_max):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO crawl_state(gallery_id, last_max_post_no, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(gallery_id) DO UPDATE SET last_max_post_no=excluded.last_max_post_no,
                                         updated_at=excluded.updated_at
    """, (gallery_id, last_max, datetime.now(TZ).isoformat()))
    conn.commit()

def parse_korean_list_datetime(s: str, now_dt=None) -> datetime:
    s = s.strip()
    now_dt = now_dt or datetime.now(TZ)
    # yyyy.mm.dd hh:mm(:ss)
    m = re.match(r"(\d{4})[./-](\d{2})[./-](\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$", s)
    if m:
        y, mo, d = map(int, m.group(1,2,3))
        hh = int(m.group(4)) if m.group(4) else 0
        mm = int(m.group(5)) if m.group(5) else 0
        ss = int(m.group(6)) if m.group(6) else 0
        return datetime(y, mo, d, hh, mm, ss, tzinfo=TZ)
    # mm.dd
    m = re.match(r"(\d{2})[./-](\d{2})$", s)
    if m:
        mo, d = map(int, m.groups())
        return datetime(now_dt.year, mo, d, 0, 0, 0, tzinfo=TZ)
    # hh:mm
    m = re.match(r"(\d{2}):(\d{2})$", s)
    if m:
        hh, mm = map(int, m.groups())
        return datetime(now_dt.year, now_dt.month, now_dt.day, hh, mm, 0, tzinfo=TZ)
    return now_dt

def parse_list_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tr in soup.select("table.gall_list tbody tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        # '공지' 건너뜀
        if '공지' in tr.get_text(" ", strip=True):
            continue

        a = tr.select_one("td.gall_tit a[href]")
        if not a:
            a = tr.find("a", href=True)
        if not a:
            continue
        href = urllib.parse.urljoin(BASE, a["href"])
        if "no=" not in href:
            continue
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(href).query or "")
        try:
            post_no = int(qs.get("no", ["0"])[0])
        except:
            continue

        title = a.get_text(strip=True)
        author_cell = tr.select_one("td.gall_writer")
        author = author_cell.get_text(" ", strip=True) if author_cell else None

        date_cell = tr.select_one("td.gall_date")
        created_at = parse_korean_list_datetime(date_cell.get_text(strip=True)) if date_cell else None

        views = up = None
        if len(tds) >= 6:
            try:
                views = int(re.sub(r"\D", "", tds[-2].get_text()))
            except: pass
            try:
                up = int(re.sub(r"\D", "", tds[-1].get_text()))
            except: pass

        rows.append({
            "post_no": post_no,
            "title": title,
            "url": href,
            "author": author,
            "created_at": created_at,
            "views": views,
            "upvotes": up,
        })
    # 최신글이 위로 오는 형태(보통 내림차순). 안전하게 post_no 내림차순 정렬.
    rows.sort(key=lambda r: r["post_no"], reverse=True)
    return rows

def extract_article_text_and_images(soup: BeautifulSoup):
    candidates = []
    for sel in ["div.write_div", "div.view_content_wrap", "div#dgn_gallery_detail",
                "div#content", "article", "div.inner.clear"]:
        for el in soup.select(sel):
            txt = " ".join(el.stripped_strings)
            candidates.append((len(txt), el, txt))
    if not candidates:
        body_txt = " ".join(list(soup.body.stripped_strings))[:5000] if soup.body else ""
        imgs = [img.get("src") for img in soup.find_all("img", src=True)]
        return body_txt, imgs
    _, best_el, best_txt = max(candidates, key=lambda x: x[0])
    imgs = []
    for img in best_el.select("img[src]"):
        src = img.get("src")
        if src:
            imgs.append(urllib.parse.urljoin(BASE, src))
    return best_txt, imgs

def parse_post_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    title = None
    for sel in ["h3.title", "h2.title", "div.title", "h3", "h2"]:
        h = soup.select_one(sel)
        if h and h.get_text(strip=True):
            title = h.get_text(strip=True); break

    text = soup.get_text("\n", strip=True)
    # 작성시각
    dt = None
    m = re.search(r"(\d{4})[./-](\d{2})[./-](\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?", text)
    if m:
        y, mo, d, hh, mm, ss = map(int, [m.group(1), m.group(2), m.group(3),
                                          m.group(4), m.group(5), m.group(6) or 0])
        dt = datetime(y, mo, d, hh, mm, ss, tzinfo=TZ)

    views = up = down = None
    mv = re.search(r"조회\s*([0-9,]+)", text);       views = int(mv.group(1).replace(",", "")) if mv else None
    mu = re.search(r"추천\s*([0-9,]+)", text);       up    = int(mu.group(1).replace(",", "")) if mu else None
    md = re.search(r"비추천\s*([0-9,]+)", text);     down  = int(md.group(1).replace(",", "")) if md else None

    author = author_ip = None
    ma = re.search(r"\n([^\n]+)\s*\(\d{1,3}(?:\.\d{1,3}){3}\)", text)
    if ma:
        raw = ma.group(0)
        m_ip = re.search(r"(\d{1,3}(?:\.\d{1,3}){3})", raw)
        if m_ip: author_ip = m_ip.group(1)
        author = raw.split("(")[0].strip()

    content, images = extract_article_text_and_images(soup)

    ccount = 0
    mc = re.search(r"댓글\s*([0-9,]+)\)", text) or re.search(r"전체 댓글\s*([0-9,]+)\s*개", text)
    if mc:
        try: ccount = int(mc.group(1).replace(",", ""))
        except: pass

    return {
        "title": title, "author": author, "author_ip": author_ip,
        "created_at": dt, "views": views, "upvotes": up, "downvotes": down,
        "comments_count": ccount, "content": content, "images": images
    }

def save_post(conn, gallery_id, row, detail):
    cur = conn.cursor()
    cur.execute("""
    INSERT OR IGNORE INTO posts
      (post_no, gallery_id, url, title, author, author_ip, created_at, views,
       upvotes, downvotes, comments_count, content, images_json, crawled_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        row["post_no"], gallery_id, row["url"],
        detail.get("title") or row.get("title"),
        detail.get("author"), detail.get("author_ip"),
        (detail.get("created_at") or row.get("created_at") or datetime.now(TZ)).isoformat(),
        detail.get("views") if detail.get("views") is not None else row.get("views"),
        detail.get("upvotes"), detail.get("downvotes"),
        detail.get("comments_count"), detail.get("content"),
        str(detail.get("images") or []), datetime.now(TZ).isoformat()
    ))
    conn.commit()

def crawl_incremental(db_path, gallery_id="ecoin", max_pages=5, max_new=50,
                      existing_break=20, sleep_min=0.8, sleep_max=1.6,
                      mode="incremental", floor_post=0, log_verbose=False):
    """existing_break: 연속으로 기존 글을 이만큼 만나면 조기 종료"""
    list_url_base = f"{BASE}/mgallery/board/lists/?id={gallery_id}&page="
    conn = ensure_db(db_path)
    s = requests.Session()

    last_max_no = get_last_max_post_no(conn, gallery_id)
    new_count = 0
    consecutive_existing = 0
    observed_max_no_this_run = last_max_no

    for p in range(1, max_pages + 1):
        list_url = list_url_base + str(p)
        r = with_retry_get(list_url, s)
        if not r:
            print(f"[WARN] list fetch failed: {list_url}"); continue
        rows = parse_list_page(r.text)
        if not rows:
            print(f"[INFO] no rows on page {p}"); break

        for row in rows:
            post_no = row["post_no"]
            if log_verbose:
                print(f"[DEBUG] seen post_no={post_no} (last_max_no={last_max_no})")

            observed_max_no_this_run = max(observed_max_no_this_run, post_no)

            if mode == "incremental":
                if post_no <= last_max_no:
                    consecutive_existing += 1
                    if consecutive_existing >= existing_break:
                        print(f"[INFO] hit {existing_break} existing posts in a row → early stop.")
                        update_crawl_state(conn, gallery_id, observed_max_no_this_run)
                        conn.close()
                        return
                    continue
                else:
                    consecutive_existing = 0
            else:  # backfill
                if floor_post and post_no <= floor_post:
                    print(f"[INFO] reached floor_post={floor_post} → stop backfill.")
                    update_crawl_state(conn, gallery_id, observed_max_no_this_run)
                    conn.close()
                    return
                # backfill은 기존글이어도 계속 진행 (INSERT OR IGNORE로 중복 방지)
                # 단, 너무 오래 긁지 않도록 max_new는 그대로 적용

            # 상세 페이지 수집
            r2 = with_retry_get(row["url"], s)
            if not r2:
                print(f"[WARN] view fetch failed: {row['url']}");
                continue
            detail = parse_post_page(r2.text)
            save_post(conn, gallery_id, row, detail)
            new_count += 1

            if new_count >= max_new:
                print(f"[INFO] reached max_new={max_new} → stop this run.")
                update_crawl_state(conn, gallery_id, observed_max_no_this_run)
                conn.close()
                return

            time.sleep(random.uniform(sleep_min, sleep_max))
        time.sleep(random.uniform(sleep_min + 0.3, sleep_max + 0.8))

    update_crawl_state(conn, gallery_id, observed_max_no_this_run)
    conn.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="dcinside_ecoin.sqlite3")
    ap.add_argument("--gallery-id", default="ecoin")
    ap.add_argument("--max-pages", type=int, default=5)
    ap.add_argument("--max-new", type=int, default=50, help="이번 실행에서 최대 신규 수집 건수")
    ap.add_argument("--existing-break", type=int, default=20, help="연속 기존글 N개 만나면 조기 종료")
    ap.add_argument("--mode", choices=["incremental", "backfill"], default="incremental",
                    help="incremental: 새 글만 확인(연속 기존글 만나면 종료) / backfill: 조기 종료 없이 과거까지 채우기")
    ap.add_argument("--floor-post", type=int, default=0,
                    help="backfill 모드에서 여기에 도달하면 종료(예: 1000000). 0이면 무시")
    ap.add_argument("--log-verbose", action="store_true", help="상세 로그 출력")

    args = ap.parse_args()

    crawl_incremental(
        db_path=args.db, gallery_id=args.gallery_id,
        max_pages=args.max_pages, max_new=args.max_new,
        existing_break=args.existing_break,
        mode=args.mode, floor_post=args.floor_post,
        log_verbose=args.log_verbose
    )

if __name__ == "__main__":
    main()
