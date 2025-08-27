# fmkorea_coin_crawler.py
# -*- coding: utf-8 -*-
import re, time, random, sqlite3, urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

BASE = "https://www.fmkorea.com"
START = f"{BASE}/coin"
TZ = ZoneInfo("Asia/Seoul")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"),
    "Referer": START,
    "Accept-Language": "ko,en;q=0.9",
}

DB_PATH = "fmkorea_coin.sqlite3"

def with_retry_get(url, session, max_try=3, base_sleep=1.2):
    for i in range(max_try):
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r
            # 4xx/5xx: 점진적 대기
            time.sleep(base_sleep * (i + 1) + random.random())
        except requests.RequestException:
            time.sleep(base_sleep * (i + 1) + random.random())
    return None

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        doc_id INTEGER PRIMARY KEY,
        url TEXT UNIQUE,
        title TEXT,
        author TEXT,
        created_at TEXT,
        content TEXT,
        images_json TEXT,
        crawled_at TEXT
    )
    """)
    conn.commit()
    return conn

DOC_HREF_RE = re.compile(r"^/(?:\d{6,12})(?:[/?#].*)?$")  # /1234567890 형태

def extract_list_links(html: str):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    # 1) 본문 영역 안의 a[href] 중 숫자형 문서 경로 수집
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        # 절대/상대 모두 처리
        parsed = urllib.parse.urlparse(urllib.parse.urljoin(BASE, href))
        if parsed.netloc not in ("www.fmkorea.com", "fmkorea.com"):
            continue
        if DOC_HREF_RE.match(parsed.path):
            links.add(urllib.parse.urlunparse(parsed._replace(fragment="")))
    return sorted(links)

def text_candidates(soup: BeautifulSoup):
    # 텍스트가 많은 후보를 찾아 가장 긴 것을 본문으로 사용
    candidates = []
    for sel in [
        "div.read_body", "article", "div[document_root]", "section", "div#content",
        "div.bd", "div.rx-content", "div.xe_content", "div.content", "div#bo_v_con"
    ]:
        for el in soup.select(sel):
            text = " ".join(el.stripped_strings)
            if len(text) > 100:
                candidates.append((len(text), el, text))
    if candidates:
        return max(candidates, key=lambda x: x[0])[1]
    return soup.body or soup  # 최후 수단

DT_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}):(\d{2})")

def parse_post(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")

    # 제목: og:title > h1 > title 우선순위
    title = None
    og = soup.select_one("meta[property='og:title']")
    if og and og.get("content"):
        title = og["content"].strip()
    if not title:
        h1 = soup.select_one("h1, h2.post-title, .np_18px, .hx")
        if h1:
            title = h1.get_text(" ", strip=True)
    if not title and soup.title:
        title = soup.title.get_text(strip=True)

    # 작성자(가능한 경우)
    author = None
    for sel in [".author", ".member", ".side.fr .m_no", ".nick", ".wr_name", "meta[name='author']"]:
        el = soup.select_one(sel)
        if el:
            author = (el.get("content") if el.name == "meta" else el.get_text(" ", strip=True))
            if author:
                break

    # 작성시각 (텍스트 전체에서 패턴 추출: 2025.08.27 14:55)
    created_at = None
    m = DT_RE.search(soup.get_text("\n", strip=True))
    if m:
        y, mo, d, hh, mm = map(int, m.groups())
        try:
            created_at = datetime(y, mo, d, hh, mm, tzinfo=TZ).isoformat()
        except ValueError:
            created_at = None

    # 본문/이미지
    body_el = text_candidates(soup)
    content_text = " ".join(body_el.stripped_strings)[:100000] if body_el else ""
    images = []
    for img in body_el.select("img[src]") if body_el else soup.select("img[src]"):
        src = img.get("src")
        if not src:
            continue
        abs_src = urllib.parse.urljoin(BASE, src)
        # 썸네일/스티커 등은 길이 제한적으로 필터링 가능 (원하면 조건 추가)
        images.append(abs_src)

    # 문서 ID
    parsed = urllib.parse.urlparse(url)
    doc_id = None
    m2 = re.match(r"^/(\d{6,12})$", parsed.path)
    if m2:
        doc_id = int(m2.group(1))
    else:
        # path가 /1234567890?mid=... 처럼 섞인 경우
        m3 = re.match(r"^/(\d{6,12})/", parsed.path)
        if m3:
            doc_id = int(m3.group(1))

    return {
        "doc_id": doc_id,
        "title": title,
        "author": author,
        "created_at": created_at,
        "content": content_text,
        "images": images,
    }

def save_post(conn, url: str, data: dict):
    if not data.get("doc_id"):
        return
    cur = conn.cursor()
    cur.execute("""
    INSERT OR IGNORE INTO posts (doc_id, url, title, author, created_at, content, images_json, crawled_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["doc_id"],
        url,
        data.get("title"),
        data.get("author"),
        data.get("created_at"),
        data.get("content"),
        str(data.get("images") or []),
        datetime.now(TZ).isoformat()
    ))
    conn.commit()

def crawl(max_pages=3, sleep_min=1.0, sleep_max=2.0):
    conn = ensure_db()
    s = requests.Session()

    for page in range(1, max_pages + 1):
        list_url = START if page == 1 else f"{START}?page={page}"
        r = with_retry_get(list_url, s)
        if not r:
            print(f"[WARN] list fetch failed: {list_url}")
            continue

        post_links = extract_list_links(r.text)
        if not post_links:
            print(f"[INFO] no post links found on {list_url}")
            time.sleep(random.uniform(sleep_min, sleep_max))
            continue

        print(f"[INFO] page {page}: {len(post_links)} links")
        for url in post_links:
            rr = with_retry_get(url, s)
            if not rr:
                print(f"[WARN] view fetch failed: {url}")
                continue
            data = parse_post(rr.text, url)
            save_post(conn, url, data)
            time.sleep(random.uniform(sleep_min, sleep_max))

        time.sleep(random.uniform(sleep_min + 0.5, sleep_max + 1.0))

    conn.close()

if __name__ == "__main__":
    # 예: 처음 5페이지 수집
    crawl(max_pages=5)
