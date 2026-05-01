"""
Optimized scraper for Microsoft Logic Apps documentation.
"""

import asyncio
import logging
import re
from typing import Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 50

MICROSOFT_LEARN_URLS = [
    "https://learn.microsoft.com/en-us/azure/logic-apps/view-workflow-status-run-history",
    "https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-diagnosing-failures",
    "https://learn.microsoft.com/en-us/azure/logic-apps/handle-throttling-problems-429-errors",
    "https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-limits-and-config",
    "https://learn.microsoft.com/en-us/azure/logic-apps/error-exception-handling",
    "https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-content-type",
    "https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-handle-large-messages",
    "https://learn.microsoft.com/en-us/azure/logic-apps/handle-long-running-stored-procedures-sql-connector",
    "https://learn.microsoft.com/en-us/azure/logic-apps/logic-apps-custom-api-authentication",
    "https://learn.microsoft.com/en-us/azure/logic-apps/support-non-unicode-character-encoding",
]

SLOW_URLS = {"authenticate-with-managed-identity"}


def chunk_text(text: str) -> List[str]:
    if not text:
        return []
    if len(text) > 500000:
        text = text[:500000]

    chunks: List[str] = []
    start = 0
    text_len = len(text)
    max_chunks = 500
    while start < text_len and len(chunks) < max_chunks:
        end = min(start + CHUNK_SIZE, text_len)
        if end < text_len:
            for sep in [". ", "! ", "? ", "\n\n", "\n", " "]:
                pos = text.rfind(sep, start, end)
                if pos != -1:
                    end = pos + len(sep)
                    break
        chunk = text[start:end].strip()
        if chunk and len(chunk) > 50:
            chunks.append(chunk)
        new_start = end - CHUNK_OVERLAP
        if new_start <= start:
            new_start = start + CHUNK_SIZE // 2
        start = new_start
    return chunks


def extract_category(text: str) -> str:
    text_lower = text.lower()
    if "retry" in text_lower:
        return "RETRY_POLICY"
    if "throttl" in text_lower or "429" in text:
        return "THROTTLING"
    if "timeout" in text_lower or "408" in text or "504" in text:
        return "TIMEOUT"
    if "401" in text or "unauthorized" in text_lower:
        return "AUTH_ERROR"
    if "403" in text or "forbidden" in text_lower:
        return "PERMISSION_ERROR"
    if "404" in text or "not found" in text_lower:
        return "NOT_FOUND"
    if "500" in text or "502" in text or "503" in text:
        return "BACKEND_ERROR"
    if "400" in text or "bad request" in text_lower:
        return "BAD_REQUEST"
    return "GENERAL_ERROR"


async def scrape_page(client: httpx.AsyncClient, url: str, timeout: float = 45.0) -> Optional[Dict]:
    try:
        resp = await client.get(url, timeout=timeout)
        if resp.status_code != 200:
            return None
        if len(resp.text) > 2_000_000:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        title_el = soup.find("h1") or soup.find("title")
        title = title_el.get_text(strip=True)[:200] if title_el else "Microsoft Learn"
        content_el = soup.find("main") or soup.find("article")
        if content_el:
            for unwanted in content_el.find_all(["nav", "footer", "script", "style", "aside"]):
                unwanted.decompose()
            text = content_el.get_text(separator=" ", strip=True)
        else:
            text = soup.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text).strip()
        error_keywords = ["error", "fail", "timeout", "retry", "throttl", "401", "403", "404", "408", "429", "500"]
        if not any(kw in text.lower() for kw in error_keywords):
            return None
        if len(text) > 100:
            return {"title": title, "url": url, "text": text, "category": extract_category(text)}
    except asyncio.TimeoutError:
        logger.warning("Timeout scraping %s", url)
        return None
    except Exception as e:
        logger.warning("Error scraping %s: %s", url, e)
        return None
    return None


async def scrape_all_async(batch_size: int = 3, timeout_per_url: float = 45.0, skip_slow_urls: bool = True) -> List[Dict]:
    chunks: List[Dict] = []
    urls_to_scrape = MICROSOFT_LEARN_URLS
    if skip_slow_urls:
        urls_to_scrape = [url for url in urls_to_scrape if not any(slow in url for slow in SLOW_URLS)]
    logger.info("\n  Scraping %s URLs with batch size %s", len(urls_to_scrape), batch_size)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        follow_redirects=True,
    ) as client:
        for batch_idx in range(0, len(urls_to_scrape), batch_size):
            batch = urls_to_scrape[batch_idx : batch_idx + batch_size]
            tasks = [scrape_page(client, url, timeout_per_url) for url in batch]
            results = await asyncio.gather(*tasks)
            for url, result in zip(batch, results):
                if result:
                    text_chunks = chunk_text(result["text"])
                    for chunk_text_content in text_chunks:
                        chunks.append(
                            {
                                "text": chunk_text_content,
                                "meta": {
                                    "title": result["title"],
                                    "url": result["url"],
                                    "category": result["category"],
                                    "source": "Microsoft Learn",
                                    "product": "Azure Logic Apps",
                                },
                            }
                        )
                    logger.info("   %s: %s chunks", url.split("/")[-1][:50], len(text_chunks))
                else:
                    logger.info("   %s: No error content", url.split("/")[-1][:50])
            if batch_idx + batch_size < len(urls_to_scrape):
                await asyncio.sleep(0.5)
    return chunks


def get_knowledge_chunks_async(skip_slow_urls: bool = True, batch_size: int = 3, timeout_per_url: float = 45.0):
    return scrape_all_async(
        batch_size=batch_size,
        timeout_per_url=timeout_per_url,
        skip_slow_urls=skip_slow_urls,
    )
