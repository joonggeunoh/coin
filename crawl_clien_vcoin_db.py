#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clien '가상화폐당' (cm_vcoin) 크롤러 - DB 저장 버전
- 리스트: https://www.clien.net/service/board/cm_vcoin
- 수집 필드: title, url(UNIQUE), author, date_text, date_parsed, views, likes, comments, body_text
- DB: SQLite (기본), SQLAlchemy ORM 사용 → 다른 DB로 교체 쉬움
주의:
  1) robots.txt/약관 준수, 과도한 트래픽 금지(딜레이 유지)
  2) 사이트 구조 변경 시 selector 조정 필요
"""

import re
import time
import sys
import argparse
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
from dateutil import parser as dtparser

from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker

# ------------- 크롤 설정 -------------
BASE = "https://www.clien.net"
BOARD_PATH = "/service/board/cm_vcoin"
BOARD_URL = urljoin(BASE, BOARD_PATH)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE,
}

# ------------- DB 모델 -------------
Base = declarative_base()

class ClienPost(Base):
    __tablename__ = "clien_vcoin_posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(512), nullable=False)                 # UNIQUE
    title = Column(String(512))
    author = Column(String(128))
    date_text = Column(String(128))
    date_parsed = Column(DateTime)
    views = Column(Integer)
    likes = Column(Integer)
    comments = Column(Integer)
    body_text = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("url", name="uq_clien_url"),
        Index("ix_date_parsed", "date_parsed"),
        Index("ix_created_at", "created_at"),
    )

# ------------- HTTP 유틸 -------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 3))
def get(url: str, session: requests.Session, **kwargs) -> requests.Response:
    resp = session.get(url, timeout=15, headers=DEFAULT_HEADERS, **kwargs)
    resp.raise_for_status()
    return resp

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip() if s else ""

def to_int(s: str):
    if s is None:
        return None
    s = s.strip().replace(",", "")
    m = re.search(r"-?\d+", s)
    return int(m.group()) if m else None

def safe_parse_date(s: str):
    s = clean_text(s)
    if not s:
        return None
    try:
        return dtparser.parse(s, fuzzy=True)
    except Exception:
        return None

def build_url_with_params(base_url: str, **params) -> str:
    u = urlparse(base_url)
    q = parse_qs(u.query)
    for k, v in params.items():
        q[k] = [str(v)]
    from urllib.parse import urlencode
    new_query = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

# ------------- 파서 -------------
def parse_list_items(html: str):
    soup = BeautifulSoup(html, "html5lib")

    candidates = []
    candidates.extend(soup.select("li.list_item"))
    candidates.extend(soup.select("div.list_item"))
    if not candidates:
        candidates.extend(soup.select("div.list_content > ul > li"))
        candidates.extend(soup.select("table.list_table tr"))

    items = []
    for node in candidates:
        a = node.select_one("a.list_subject, a.subject_fixed, a[href*='/service/board/']")
        if not a:
            a = node.find("a", href=True)
        title = clean_text(a.get_text()) if a else None
        href = urljoin(BASE, a["href"]) if a and a.has_attr("href") else None

        author = None
        for sel in ["span.nickname", "span.author", "span.list_author", "a.nickname"]:
            t = node.select_one(sel)
            if t:
                author = clean_text(t.get_text())
                break

        date_text = None
        for sel in ["span.timestamp", "span.list_time", "time", "span.regdate"]:
            t = node.select_one(sel)
            if t:
                date_text = clean_text(t.get_text())
                break

        views = None
        for sel in ["span.view_count", "span.hit", "span.list_hit", "span.view"]:
            t = node.select_one(sel)
            if t and (v := to_int(t.get_text())) is not None:
                views = v
                break

        likes = None
        for sel in ["span.symph", "span.recommend", "span.like", "span.sum"]:
            t = node.select_one(sel)
            if t and (v := to_int(t.get_text())) is not None:
                likes = v
                break

        comments = None
        if a and a.find("span", class_=re.compile(r"reply|rSymph|comment")):
            t = a.find("span", class_=re.compile(r"reply|comment"))
            if t and (v := to_int(t.get_text())) is not None:
                comments = v
        if comments is None:
            m = re.search(r"\[(\d+)\]", node.get_text(" "))
            comments = int(m.group(1)) if m else None

        if title and href:
            items.append({
                "title": title,
                "url": href,
                "author": author,
                "date_text": date_text,
                "views": views,
                "likes": likes,
                "comments": comments,
            })
    return items

def parse_detail(html: str):
    soup = BeautifulSoup(html, "html5lib")
    body = None
    for sel in [
        "div.post_article",
        "div.article_view",
        "div.post_content",
        "div.view_content",
        "article.post_view",
    ]:
        node = soup.select_one(sel)
        if node:
            body = clean_text(node.get_text(" "))
            break
    if not body:
        body = clean_text(soup.get_text(" "))
    return body

def guess_pagination_mode(html: str):
    if "po=" in html or re.search(r"[?&]po=\d+", html):
        return "offset"
    if re.search(r"[?&](page|p)=\d+", html):
        return "page"
    return "offset"

def next_page_url(current_url: str, mode: str, page_index: int, step: int = 20) -> str:
    if mode == "offset":
        return build_url_with_params(current_url, po=page_index * step)
    else:
        return build_url_with_params(current_url, page=page_index + 1)

# ------------- 크롤 + DB upsert -------------
def upsert_post(db, data: dict):
    """
    url UNIQUE 기준으로 upsert
    """
    session = db()
    try:
        obj = session.query(ClienPost).filter(ClienPost.url == data["url"]).one_or_none()
        if obj is None:
            obj = ClienPost(**data)
            session.add(obj)
        else:
            # 갱신(본문/조회수 등 업데이트)
            for k, v in data.items():
                setattr(obj, k, v)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def crawl_to_db(pages: int, delay: float, step: int, include_body: bool, db_sessionmaker):
    http = requests.Session()
    http.headers.update(DEFAULT_HEADERS)

    first = get(BOARD_URL, http)
    mode = guess_pagination_mode(first.text)

    total_saved = 0
    for i in range(pages):
        list_url = BOARD_URL if i == 0 else next_page_url(BOARD_URL, mode, i, step)
        resp = get(list_url, http)
        items = parse_list_items(resp.text)
        print(f"[Page {i+1}/{pages}] {list_url} → items: {len(items)}")

        for it in items:
            row = {**it}
            row["date_parsed"] = safe_parse_date(it.get("date_text"))

            if include_body:
                try:
                    time.sleep(delay)
                    d = get(it["url"], http)
                    row["body_text"] = parse_detail(d.text)
                except Exception as e:
                    row["body_text"] = f"(detail_fetch_error: {e})"

            upsert_post(db_sessionmaker, row)
            total_saved += 1

        time.sleep(delay)

    print(f"Done. processed rows: {total_saved}")

def main():
    ap = argparse.ArgumentParser(description="Clien cm_vcoin → DB crawler")
    ap.add_argument("--pages", type=int, default=3, help="수집할 페이지(대략)")
    ap.add_argument("--delay", type=float, default=2.0, help="요청 간 딜레이(초)")
    ap.add_argument("--step", type=int, default=20, help="offset 증가 단위(po 모드)")
    ap.add_argument("--db", type=str, default="clien_vcoin.sqlite", help="DB 파일 경로(또는 SQLAlchemy URL)")
    ap.add_argument("--no-body", action="store_true", help="본문 수집 생략")
    args = ap.parse_args()

    # DB 엔진 초기화 (SQLite 파일 or URL)
    # 예) PostgreSQL: postgresql+psycopg2://user:pass@host:5432/dbname
    #     MySQL: mysql+pymysql://user:pass@host:3306/dbname?charset=utf8mb4
    if args.db.endswith(".sqlite") or "://" not in args.db:
        engine_url = f"sqlite:///{args.db}"
    else:
        engine_url = args.db

    engine = create_engine(engine_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    try:
        crawl_to_db(
            pages=args.pages,
            delay=args.delay,
            step=args.step,
            include_body=not args.no_body,
            db_sessionmaker=SessionLocal
        )
    except requests.HTTPError as e:
        print(f"HTTPError: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("중단됨.", file=sys.stderr)
        sys.exit(130)

if __name__ == "__main__":
    main()
