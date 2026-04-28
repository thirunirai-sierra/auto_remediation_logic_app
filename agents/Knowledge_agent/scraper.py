"""
Optimized scraper for Microsoft Logic Apps documentation.

This module provides async web scraping of Microsoft Learn documentation
with features:
- Concurrent batch processing (3 URLs simultaneously)
- Timeout protection (45 seconds per URL)
- Memory protection (500KB limit per document)
- Error content filtering (only keeps error-related text)
- Smart chunking (1200 chars with 50 char overlap)

The scraper targets specific Azure Logic Apps error documentation pages
and filters content using error keywords.

Example:
    chunks = asyncio.run(scrape_all_async(batch_size=3, timeout_per_url=45))
    for chunk in chunks:
        print(chunk["text"][:100])
"""

import asyncio
import logging
import re
from typing import List, Dict, Optional
import httpx
from bs4 import BeautifulSoup
from yaspin import yaspin

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 50

# Error-specific Microsoft Learn URLs
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

# URLs known to be slow or problematic - can be skipped
SLOW_URLS = {
    "authenticate-with-managed-identity",  # Takes 60+ minutes
}


def chunk_text(text: str) -> List[str]:
    """
    Split text into overlapping chunks with memory protection.
    
    Args:
        text: Input text to split.
    
    Returns:
        List of text chunks, each up to CHUNK_SIZE chars with CHUNK_OVERLAP overlap.
    
    Features:
        - Limits text to 500KB to prevent MemoryError
        - Maximum 500 chunks per document
        - Breaks at sentence boundaries when possible
        - Minimum chunk size 50 chars (filters tiny chunks)
    """
    if not text:
        return []
    
    # Limit text size to prevent MemoryError
    if len(text) > 500000:
        text = text[:500000]
    
    chunks = []
    start = 0
    text_len = len(text)
    max_chunks = 500
    
    while start < text_len and len(chunks) < max_chunks:
        end = min(start + CHUNK_SIZE, text_len)
        
        if end < text_len:
            for sep in ['. ', '! ', '? ', '\n\n', '\n', ' ']:
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
    """
    Determine category based on content keywords.
    
    Args:
        text: Content to analyze.
    
    Returns:
        Category string: RETRY_POLICY, THROTTLING, TIMEOUT, AUTH_ERROR,
        PERMISSION_ERROR, NOT_FOUND, BACKEND_ERROR, BAD_REQUEST, or GENERAL_ERROR.
    """
    text_lower = text.lower()
    
    if "retry" in text_lower:
        return "RETRY_POLICY"
    elif "throttl" in text_lower or "429" in text:
        return "THROTTLING"
    elif "timeout" in text_lower or "408" in text or "504" in text:
        return "TIMEOUT"
    elif "401" in text or "unauthorized" in text_lower:
        return "AUTH_ERROR"
    elif "403" in text or "forbidden" in text_lower:
        return "PERMISSION_ERROR"
    elif "404" in text or "not found" in text_lower:
        return "NOT_FOUND"
    elif "500" in text or "502" in text or "503" in text:
        return "BACKEND_ERROR"
    elif "400" in text or "bad request" in text_lower:
        return "BAD_REQUEST"
    else:
        return "GENERAL_ERROR"


async def scrape_page(
    client: httpx.AsyncClient,
    url: str,
    timeout: float = 45.0
) -> Optional[Dict]:
    """
    Scrape a single Microsoft Learn page with timeout protection.
    
    Args:
        client: Async HTTP client.
        url: Page URL to scrape.
        timeout: Request timeout in seconds.
    
    Returns:
        Dictionary with keys: title, url, text, category.
        Returns None if:
        - Page returns non-200 status
        - Page size exceeds 2MB
        - No error-related content found
        - Timeout occurs
    """
    try:
        resp = await client.get(url, timeout=timeout)
        if resp.status_code != 200:
            return None
        
        # Skip if page is too large
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
        
        # Filter for error-related content
        error_keywords = ["error", "fail", "timeout", "retry", "throttl", "401", "403", "404", "408", "429", "500"]
        if not any(kw in text.lower() for kw in error_keywords):
            return None
        
        if len(text) > 100:
            return {
                "title": title,
                "url": url,
                "text": text,
                "category": extract_category(text),
            }
    except asyncio.TimeoutError:
        logger.warning(f"Timeout scraping {url}")
        return None
    except Exception as e:
        logger.warning(f"Error scraping {url}: {e}")
        return None
    
    return None


async def scrape_all_async(
    batch_size: int = 3,
    timeout_per_url: float = 45.0,
    skip_slow_urls: bool = True
) -> List[Dict]:
    """
    Scrape all URLs concurrently with batch control.
    
    Args:
        batch_size: Number of concurrent requests (default 3).
        timeout_per_url: Timeout per URL in seconds (default 45).
        skip_slow_urls: Skip known problematic URLs (default True).
    
    Returns:
        List of chunk dictionaries, each with 'text' and 'meta' keys.
    
    Processing:
        1. Filters URLs based on skip_slow_urls
        2. Processes in batches to avoid overwhelming the server
        3. Converts each page to chunks with metadata
        4. Adds delay between batches to be polite
    
    Example:
        >>> chunks = await scrape_all_async(batch_size=2, timeout_per_url=30)
        >>> len(chunks) > 0
        True
    """
    chunks = []
    
    # Filter URLs
    urls_to_scrape = MICROSOFT_LEARN_URLS
    if skip_slow_urls:
        urls_to_scrape = [
            url for url in urls_to_scrape
            if not any(slow in url for slow in SLOW_URLS)
        ]
    
    print(f"\n  Scraping {len(urls_to_scrape)} URLs with batch size {batch_size}")
    
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        follow_redirects=True
    ) as client:
        for batch_idx in range(0, len(urls_to_scrape), batch_size):
            batch = urls_to_scrape[batch_idx:batch_idx + batch_size]
            
            tasks = [scrape_page(client, url, timeout_per_url) for url in batch]
            results = await asyncio.gather(*tasks)
            
            for url, result in zip(batch, results):
                if result:
                    text_chunks = chunk_text(result["text"])
                    for chunk_text_content in text_chunks:
                        chunks.append({
                            "text": chunk_text_content,
                            "meta": {
                                "title": result["title"],
                                "url": result["url"],
                                "category": result["category"],
                                "source": "Microsoft Learn",
                                "product": "Azure Logic Apps",
                            },
                        })
                    print(f"  ✔ {url.split('/')[-1][:50]}: {len(text_chunks)} chunks")
                else:
                    print(f"  – {url.split('/')[-1][:50]}: No error content")
            
            if batch_idx + batch_size < len(urls_to_scrape):
                await asyncio.sleep(0.5)
    
    return chunks


def get_knowledge_chunks_async(
    skip_slow_urls: bool = True,
    batch_size: int = 3,
    timeout_per_url: float = 45.0
):
    """
    Get all scraped knowledge chunks (async wrapper).
    
    Args:
        skip_slow_urls: Skip known problematic URLs.
        batch_size: Number of concurrent requests.
        timeout_per_url: Timeout per URL in seconds.
    
    Returns:
        Coroutine that returns list of chunks when awaited.
    
    Example:
        >>> chunks = await get_knowledge_chunks_async(batch_size=2)
        >>> # or in synchronous code:
        >>> chunks = asyncio.run(get_knowledge_chunks_async())
    """
    return scrape_all_async(
        batch_size=batch_size,
        timeout_per_url=timeout_per_url,
        skip_slow_urls=skip_slow_urls
    )