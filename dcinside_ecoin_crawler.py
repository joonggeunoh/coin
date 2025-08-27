# dcinside_ecoin_crawler.py
# -*- coding: utf-8 -*-
import time, random, re, sqlite3, urllib.parse
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

BASE = "https://gall.dcinside.com"
GALLERY_ID = "ecoin"
LIST_URL = f"{BASE}/mgallery/board/lists/?id={GALLERY_ID}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36",
    "Referer": LIST_URL,
}
TZ = ZoneInfo("Asia/Seoul")

DB_PATH = "dcinside_ecoin.sqlite3"

def with_retry_get(url, session, max_try=3, sleep_base=1.2):
    for i in range(max_try):
        try:
            r = session.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r
            # 403/429/5xx 등은 약간 더 휴식
            time.sleep(sleep_base * (i + 1) + random.random())
        except requests.RequestException:
            time.sleep(sleep_base * (i + 1) + random.random())
    return None

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_no INTEGER,
        gallery_id TEXT,
        comment_id INTEGER,
        parent_id INTEGER,
        author TEXT,
        author_ip TEXT,
        created_at TEXT,
        content TEXT,
        upvotes INTEGER,
        downvotes INTEGER
    )
    """)
    conn.commit()
    return conn

def parse_korean_list_datetime(s: str) -> datetime:
    """
    리스트의 작성일 표기:
      - '08.24' 형태: 올해의 월.일로 간주해 00:00
      - '18:47' 형태: 오늘의 시:분으로 간주
      - '2025.08.24 18:47:19' 전체 포맷도 안전 처리
    """
    s = s.strip()
    today = datetime.now(TZ).date()
    # yyyy.mm.dd hh:mm(:ss)
    m = re.match(r"(\d{4})[./-](\d{2})[./-](\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?", s)
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
        return datetime(today.year, mo, d, 0, 0, 0, tzinfo=TZ)

    # hh:mm
    m = re.match(r"(\d{2}):(\d{2})$", s)
    if m:
        hh, mm = map(int, m.groups())
        return datetime(today.year, today.month, today.day, hh, mm, 0, tzinfo=TZ)

    # fallback: 지금
    return datetime.now(TZ)

def parse_list_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    # DCInside 리스트는 보통 table.gall_list > tbody > tr 로 구성,
    # '공지' 행을 건너뛰고, 제목 td 안 <a>의 href에 'no='가 있음
    for tr in soup.select("table.gall_list tbody tr"):
        tds = tr.find_all("td")
        if not tds or '공지' in tr.get_text(strip=True):
            continue

        # 제목/링크
        a = tr.select_one("td.gall_tit a")
        if not a or not a.get("href"):
            # 클래스 이름이 다를 때를 대비한 보조 탐색
            a = tr.find("a", href=True)
        if not a:
            continue
        href = urllib.parse.urljoin(BASE, a["href"])
        if "no=" not in href:
            continue
        q = urllib.parse.urlparse(href).query
        qs = urllib.parse.parse_qs(q)
        try:
            post_no = int(qs.get("no", ["0"])[0])
        except:
            continue
        title = a.get_text(strip=True)

        # 작성자
        author_cell = tr.select_one("td.gall_writer") or (tds[2] if len(tds) >= 3 else None)
        author = author_cell.get_text(" ", strip=True) if author_cell else None

        # 작성일 (보통 4번째 컬럼)
        date_cell = tr.select_one("td.gall_date") or (tds[3] if len(tds) >= 4 else None)
        created_at = parse_korean_list_datetime(date_cell.get_text(strip=True)) if date_cell else None

        # 조회/추천 (마지막 2칸 가정)
        views, rec = None, None
        if len(tds) >= 6:
            try:
                views = int(re.sub(r"\D", "", tds[-2].get_text()))
            except:
                pass
            try:
                rec = int(re.sub(r"\D", "", tds[-1].get_text()))
            except:
                pass

        rows.append({
            "post_no": post_no,
            "title": title,
            "url": href,
            "author": author,
            "created_at": created_at,
            "views": views,
            "upvotes": rec,
        })
    return rows

def extract_article_text_and_images(soup: BeautifulSoup):
    # 본문 컨테이너가 바뀌어도 대응하도록 여러 후보를 검사
    candidates = []
    for sel in [
        "div.write_div", "div.view_content_wrap", "div#dgn_gallery_detail",
        "div#content", "div#container", "article", "div.inner.clear"
    ]:
        for el in soup.select(sel):
            txt = " ".join(list(el.stripped_strings))
            candidates.append((len(txt), el, txt))
    if not candidates:
        body_txt = " ".join(list(soup.body.stripped_strings))[:5000] if soup.body else ""
        imgs = [img.get("src") for img in soup.find_all("img", src=True)]
        return body_txt, imgs

    # 가장 텍스트가 많은 블록을 본문으로 간주
    _, best_el, best_txt = max(candidates, key=lambda x: x[0])
    imgs = []
    for img in best_el.select("img[src]"):
        src = img.get("src")
        if src:
            imgs.append(urllib.parse.urljoin(BASE, src))
    return best_txt, imgs

def parse_post_page(html: str):
    soup = BeautifulSoup(html, "lxml")

    # 제목
    title = None
    for sel in ["h3.title", "h2.title", "div.title", "h3", "h2"]:
        h = soup.select_one(sel)
        if h and h.get_text(strip=True):
            title = h.get_text(strip=True)
            break

    # 메타(작성자/IP/작성시각/추천/조회)
    # 페이지 구조 다양성 때문에 안전하게 텍스트 기반 정규식 추출
    text = soup.get_text("\n", strip=True)

    # 작성 시각 (YYYY.MM.DD HH:MM:SS)
    dt = None
    m = re.search(r"(\d{4})[./-](\d{2})[./-](\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?", text)
    if m:
        y, mo, d, hh, mm, ss = map(int, [m.group(1), m.group(2), m.group(3),
                                          m.group(4), m.group(5), m.group(6) or 0])
        dt = datetime(y, mo, d, hh, mm, ss, tzinfo=TZ)

    # 조회/추천/비추천
    views = None
    up, down = None, None
    mv = re.search(r"조회\s*([0-9,]+)", text)
    if mv: views = int(mv.group(1).replace(",", ""))
    mu = re.search(r"추천\s*([0-9,]+)", text)
    if mu: up = int(mu.group(1).replace(",", ""))
    md = re.search(r"비추천\s*([0-9,]+)", text)
    if md: down = int(md.group(1).replace(",", ""))

    # 작성자/IP
    author, author_ip = None, None
    ma = re.search(r"\n([^\n]+)\s*\(\d{1,3}(?:\.\d{1,3}){3}\)", text)  # 닉네임(1.2.3.4)
    if ma:
        raw = ma.group(0)
        m_ip = re.search(r"(\d{1,3}(?:\.\d{1,3}){3})", raw)
        if m_ip:
            author_ip = m_ip.group(1)
        # 닉네임은 괄호 앞부분 추정
        nick = raw.split("(")[0].strip()
        author = nick

    content, images = extract_article_text_and_images(soup)

    # 댓글 수(텍스트 기반)
    ccount = 0
    mc = re.search(r"댓글\s*([0-9,]+)\)", text)  # '댓글 0)' 같은 패턴
    if mc:
        try:
            ccount = int(mc.group(1).replace(",", ""))
        except:
            pass
    else:
        mc2 = re.search(r"전체 댓글\s*([0-9,]+)\s*개", text)
        if mc2:
            ccount = int(mc2.group(1).replace(",", ""))

    return {
        "title": title,
        "author": author,
        "author_ip": author_ip,
        "created_at": dt,
        "views": views,
        "upvotes": up,
        "downvotes": down,
        "comments_count": ccount,
        "content": content,
        "images": images,
    }

def save_post(conn, row, detail):
    cur = conn.cursor()
    cur.execute("""
    INSERT OR IGNORE INTO posts
      (post_no, gallery_id, url, title, author, author_ip, created_at, views,
       upvotes, downvotes, comments_count, content, images_json, crawled_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        row["post_no"], GALLERY_ID, row["url"],
        detail.get("title") or row.get("title"),
        detail.get("author"),
        detail.get("author_ip"),
        (detail.get("created_at") or row.get("created_at") or datetime.now(TZ)).isoformat(),
        detail.get("views") if detail.get("views") is not None else row.get("views"),
        detail.get("upvotes"),
        detail.get("downvotes"),
        detail.get("comments_count"),
        detail.get("content"),
        # 간단히 JSON 문자열 형태로 저장
        str(detail.get("images") or []),
        datetime.now(TZ).isoformat()
    ))
    conn.commit()

def crawl(pages=3, sleep_min=1.0, sleep_max=2.0):
    conn = ensure_db()
    s = requests.Session()
    for p in range(1, pages + 1):
        url = f"{LIST_URL}&page={p}"
        r = with_retry_get(url, s)
        if not r:
            print(f"[WARN] list fetch failed: {url}")
            continue
        rows = parse_list_page(r.text)
        if not rows:
            print(f"[INFO] no rows on page {p}")
            continue

        for row in rows:
            # 글 상세
            r2 = with_retry_get(row["url"], s)
            if not r2:
                print(f"[WARN] view fetch failed: {row['url']}")
                continue
            detail = parse_post_page(r2.text)
            save_post(conn, row, detail)
            # 예의상 딜레이
            time.sleep(random.uniform(sleep_min, sleep_max))

        # 페이지 간 딜레이
        time.sleep(random.uniform(sleep_min + 0.5, sleep_max + 1.0))

    conn.close()

if __name__ == "__main__":
    # 예: 처음 5페이지 수집
    crawl(pages=5)
