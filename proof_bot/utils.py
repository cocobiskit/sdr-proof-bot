# -- coding: utf-8 --
"""
utils.py

A robust set of helpers for Playwright-based automation + parsing and metrics.
"""

import asyncio
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urljoin, urlparse

import numpy as np
from bs4 import BeautifulSoup, Tag
from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    Request,
    Response,
    Route,
    TimeoutError as PwTimeoutError,
)
# Removed: from playwright_stealth import stealth_async  (handled globally in scraper.py)

__all__ = [
    "USER_AGENTS", "choose_user_agent", "SessionProfile", "random_session_profile",
    "new_context_with_profile", "handle_consent", "ensure_consent", "wait_for_any",
    "find_element_with_fallbacks", "human_delay", "polite_goto", "DomainRateLimiter",
]

# --- User Agents (expanded, deduped, realistic mix) ---
USER_AGENTS = [
    # Chrome desktop (Win/Mac/Linux)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
]

class _NullLogger:
    def debug(self, *args, **kwargs): pass
    def info(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass

def _get_logger(logger: Optional[Any]) -> Any:
    return logger if logger is not None else _NullLogger()

async def human_delay(min_ms: int = 80, max_ms: int = 220) -> None:
    await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000.0)

def choose_user_agent(seed: Optional[Union[int, str]] = None) -> str:
    rng = random.Random()
    if seed is not None:
        rng.seed(seed)
    return rng.choice(USER_AGENTS)

@dataclass
class SessionProfile:
    user_agent: str

def random_session_profile(seed: Optional[Union[int, str]] = None) -> SessionProfile:
    ua = choose_user_agent(seed=seed)
    return SessionProfile(user_agent=ua)

async def new_context_with_profile(browser: Browser, profile: Optional[SessionProfile] = None, **kwargs) -> BrowserContext:
    p = profile or random_session_profile()
    context_args = {
        'user_agent': p.user_agent,
        'viewport': {'width': 1920, 'height': 1080},
    }
    context_args.update(kwargs) # Allow overriding, e.g., with proxy
    ctx = await browser.new_context(**context_args)
    return ctx

class DomainRateLimiter:
    def __init__(self, min_delay_s: float = 1.8):
        self.min_delay = float(min_delay_s)
        self._last_at: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _domain(self, url: str) -> str:
        return urlparse(url).netloc or ""

    async def acquire(self, url: str):
        domain = self._domain(url)
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        
        lock = self._locks[domain]
        await lock.acquire()
        
        now = time.monotonic()
        last = self._last_at.get(domain, 0.0)
        gap = now - last
        wait_s = max(0.0, self.min_delay - gap)
        if wait_s > 0:
            await asyncio.sleep(wait_s)
            
    def release(self, url: str):
        domain = self._domain(url)
        self._last_at[domain] = time.monotonic()
        if domain in self._locks:
            self._locks[domain].release()

    # PATCH: Modified `wait` method to correctly handle rate-limiting.
    async def wait(self, url: Optional[str] = None):
        """
        Waits to ensure the minimum delay between requests to the same domain.
        This method fully encapsulates the blocking logic for a single operation.
        """
        if not url:
            # If no URL is provided, apply a general delay as a fallback.
            await asyncio.sleep(self.min_delay)
            return

        domain = self._domain(url)
        
        # Ensure a lock exists for this domain
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()

        # Use an async context manager for the lock to ensure it's released properly
        async with self._locks[domain]:
            now = time.monotonic()
            last_activity_time = self._last_at.get(domain, 0.0) 

            # Calculate how much more time needs to pass before `min_delay` is met
            elapsed_since_last = now - last_activity_time
            time_to_wait = max(0.0, self.min_delay - elapsed_since_last)

            if time_to_wait > 0:
                await asyncio.sleep(time_to_wait)
            
            # Update `_last_at` to the current time, marking that a request *is now being initiated*.
            # This ensures the next request to the same domain factors in this one's start time.
            self._last_at[domain] = time.monotonic() 
            # The lock is implicitly released when exiting the 'async with' block.


async def polite_goto(page: Page, url: str, logger: Optional[Any] = None, limiter: Optional[DomainRateLimiter] = None):
    log = _get_logger(logger)
    try:
        # Use the improved .wait() method
        if limiter:
            await limiter.wait(url)
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await ensure_consent(page, logger=log)
    except Exception as e:
        log.error(f"Polite goto failed for {url}: {e}")
        raise # Re-raise to propagate the error if navigation truly failed

async def accept_onetrust(page: Page, logger: Optional[Any] = None) -> bool:
    log = _get_logger(logger)
    try:
        btn = page.locator('#onetrust-accept-btn-handler')
        if await btn.is_visible(timeout=10000):
            await btn.click()
            log.info("[Consent] Onetrust accepted.")
            return True
    except PwTimeoutError:
        return False # No button found, not an error
    except Exception as e:
        log.debug(f"[Consent] Onetrust accept failed: {e}")
    return False

async def handle_consent(page: Page, logger: Optional[Any] = None) -> bool:
    log = _get_logger(logger)
    selectors = ['button:has-text("Accept All")', 'button:has-text("Accept")']
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if await btn.is_visible(timeout=1000):
                await btn.click()
                log.info(f"[Consent] Handled with generic selector '{sel}'")
                return True
        except Exception:
            continue
    return False

async def ensure_consent(page: Page, logger: Optional[Any] = None) -> bool:
    if await accept_onetrust(page, logger=logger):
        return True
    return await handle_consent(page, logger=logger)

def _flatten_selectors(entries: Sequence[Union[str, Dict[str, Any]]]) -> List[str]:
    flat = []
    for s in entries:
        if isinstance(s, dict):
            if s.get('type', 'css') == 'css' and s.get('value'):
                flat.append(s['value'])
        elif isinstance(s, str) and s.strip():
            flat.append(s)
    return flat

async def wait_for_any(page: Page, selectors_dict: Dict, keys: List[str], timeout: int = 30000) -> bool:
    selectors = []
    for key in keys:
        selectors.extend(_flatten_selectors(selectors_dict.get(key, {}).get('value', [])))
    
    if not selectors: return False
    
    union_selector = ", ".join(selectors)
    try:
        await page.locator(union_selector).first.wait_for(state='visible', timeout=timeout)
        return True
    except PwTimeoutError:
        return False

async def find_element_with_fallbacks(page: Page, key: str, selectors_dict: Dict) -> Optional[Locator]:
    selectors = _flatten_selectors(selectors_dict.get(key, {}).get('value', []))
    for sel in selectors:
        try:
            locator = page.locator(sel)
            if await locator.count() > 0:
                return locator
        except Exception:
            continue
    return None