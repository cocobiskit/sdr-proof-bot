# sic_selector_colossus.py (v2.0 - Robust Diagnostic + Auto-Fuse)
# -*- coding: utf-8 -*-
"""
SIC Selector Colossus v2.0 - Robust Auditor for Companies House SIC Extraction.

Enhancements:
- Progress logging per URL + findings (e.g., "Found 3 candidates, extracted 2 codes").
- Error isolation: Continues on failures, logs reasons.
- Diagnostic report: Counters for success/extraction rates, verdicts like colossus_diagnostic.py.
- Auto-fuse: Integrates top selectors into selectors.json like fuse_selectors.py.
- Run: ~5 min for 20 URLs; expandable for other sites.
"""
import asyncio
import json
import logging
import os
import random
import re
from collections import defaultdict, Counter
from typing import Any, Dict, List, Tuple

from playwright.async_api import Page, async_playwright, TimeoutError as PlaywrightTimeoutError
from typing import TypedDict
from datetime import datetime
# Fallback stealth
try:
    from playwright_stealth import stealth_async
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    logging.warning("playwright-stealth not available.")

# --- CONFIG (SIC-Focused; Expandable) ---
class Config:
    CURRENT_SELECTORS_FILE: str = 'selectors.json'
    RECOMMENDED_SIC_FILE: str = 'recommended_sic_selectors.json'
    REPORT_FILE: str = 'sic_colossus_report.md'
    LOG_FILE: str = 'logs/sic_colossus.log'
    SAMPLE_URLS: List[str] = [  # From log; cap 20 for speed
        "https://find-and-update.company-information.service.gov.uk/company/13681460",
        "https://find-and-update.company-information.service.gov.uk/company/12399628",
        "https://find-and-update.company-information.service.gov.uk/company/12618177",
        "https://find-and-update.company-information.service.gov.uk/company/SC705807",
        "https://find-and-update.company-information.service.gov.uk/company/13712701",
        "https://find-and-update.company-information.service.gov.uk/company/14028915",
        "https://find-and-update.company-information.service.gov.uk/company/SC693193",
        "https://find-and-update.company-information.service.gov.uk/company/12709753",
        "https://find-and-update.company-information.service.gov.uk/company/13225767",
        "https://find-and-update.company-information.service.gov.uk/company/13204151",
        "https://find-and-update.company-information.service.gov.uk/company/12375437",
        "https://find-and-update.company-information.service.gov.uk/company/08288505",
        "https://find-and-update.company-information.service.gov.uk/company/12689397",
        "https://find-and-update.company-information.service.gov.uk/company/12667722",
        "https://find-and-update.company-information.service.gov.uk/company/13049740",
        "https://find-and-update.company-information.service.gov.uk/company/12238050",
        "https://find-and-update.company-information.service.gov.uk/company/13248775",
        "https://find-and-update.company-information.service.gov.uk/company/12864125",
        "https://find-and-update.company-information.service.gov.uk/company/12868662",
        "https://find-and-update.company-information.service.gov.uk/company/12326634",
    ]
    CONCURRENT_TASKS: int = 3
    PAGE_TIMEOUT: int = 30000
    ANALYSIS_TIMEOUT: int = 10000
    USER_AGENTS: List[str] = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    SIC_HINTS: Dict[str, List[str]] = {  # Expandable for other sites
        'sic_section': ['SIC', 'Nature of business', 'business (SIC)', 'SIC code'],
        'sic_code': [r'\d{5}', '73110', '62012', '62090', '63110', '63120', '70229']
    }
    SCORE_WEIGHTS: Dict[str, int] = {
        'id': 100, 'data-testid': 95, 'unique_class': 80, 'descriptive_class': 60,
        'sic_regex_match': 200, 'visibility': 50, 'low_depth': 20, 'prevalence': 10,
        'generated_class': -50, 'nth_child': -100, 'length': -1, 'invisibility': -100
    }
    PRE_SCAN_CLICKS: List[str] = ["#accept-cookies-button"]

# Types (same as before)
class DiscoveredElement(TypedDict):
    selector: str
    innerText: str
    isVisible: bool
    depth: int
    extracted_code: str

class AnalysisData(TypedDict):
    selector: str
    score: int
    tier: str
    avg_count: float
    prevalence: str
    extraction_rate: float

class RunStats(TypedDict):
    start_time: datetime
    end_time: datetime
    total_duration: float
    urls_success: int
    urls_fail: int
    gold_recommendations: int
    total_extractions: int  # New: Track SIC codes found

# JS Analyzer (embedded, SIC-focused)
JS_ANALYZER_CODE = """
(hints) => {
  const results = { sic_section: [], sic_code: [] };
  const TAGS = new Set(['H2', 'DT', 'DD', 'UL', 'LI', 'P', 'SPAN']);
  const MAX_PER_HINT = 5;

  const isVisible = (el) => window.getComputedStyle(el).display !== 'none' && el.offsetParent !== null;
  const getDepth = (el) => { let d=0; let cur=el; while(cur.parentElement){d++;cur=cur.parentElement;} return d; };
  const generateSelector = (el) => {
    if (el.id) return `#${el.id}`;
    if (el.dataset.testid) return `[data-testid="${el.dataset.testid}"]`;
    const parts = []; let cur = el;
    while (cur && cur.tagName !== 'BODY' && parts.length < 4) {
      let part = cur.tagName.toLowerCase();
      const cls = Array.from(cur.classList).filter(c => !c.includes('--') && c.length > 2).slice(0,1).join('.');
      if (cls) part += `.${cls}`;
      parts.unshift(part); cur = cur.parentElement;
    }
    return parts.join(' > ');
  };
  const extractCode = (text) => {
    const match = text.match(/\\d{5}/);
    return match ? match[0] : '';
  };

  for (const [cat, kws] of Object.entries(hints)) {
    const found = new Set();
    for (const kw of kws) {
      if (results[cat].length >= MAX_PER_HINT) break;
      let query;
      if (cat === 'sic_code') {
        query = 'ul li, dl dt, dl dd';  // Cover <ul><li> and <dl> styles
      } else {
        query = 'h2, dt';  // Headers
      }
      document.querySelectorAll(query).forEach(el => {
        if (!TAGS.has(el.tagName)) return;
        const text = el.innerText.trim().slice(0,100);
        if (cat === 'sic_code' && !/\\d{5}/.test(text)) return;  // Digit filter
        if (cat === 'sic_section' && !/SIC|business/i.test(text)) return;
        const sel = generateSelector(el);
        if (sel && !found.has(sel)) {
          const code = extractCode(text);
          results[cat].push({
            selector: sel, innerText: text, isVisible: isVisible(el),
            depth: getDepth(el), extracted_code: code
          });
          found.add(sel);
        }
      });
    }
  }
  return results;
}
"""
# Logger (diagnostic-style: per-URL progress)
def setup_logging() -> logging.Logger:
    os.makedirs('logs', exist_ok=True)
    logger = logging.getLogger('sic_colossus')
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - [SIC Colossus] %(message)s')
    fh = logging.FileHandler(Config.LOG_FILE, mode='w')
    fh.setFormatter(fmt); logger.addHandler(fh)
    ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
    return logger

logger = setup_logging()

# Core (scoring same; added extraction counter)
def score_selector(selector: str, data: DiscoveredElement, hints: List[str]) -> Tuple[int, str]:
    score = 0
    weights = Config.SCORE_WEIGHTS
    if '#' in selector: score += weights['id']
    if '[data-testid' in selector: score += weights['data-testid']
    classes = re.findall(r'\.([a-zA-Z0-9_-]+)', selector)
    if len(classes) == 1 and any(h in classes[0].lower() for h in hints): score += weights['unique_class']
    if len(selector) > 25: score += (len(selector) - 25) * weights['length']
    if ':nth-child' in selector: score += weights['nth_child']
    if re.search(r'\d{5}', data['extracted_code']): score += weights['sic_regex_match']
    if data['isVisible']: score += weights['visibility']
    depth = data['depth']
    if depth <= 3: score += weights['low_depth'] * (4 - depth)
    if not data['isVisible']: score += weights['invisibility']
    tier = 'Gold' if score >= 150 else 'Silver' if score >= 80 else 'Bronze'
    return score, tier

# Analyze URL (added progress log, error catch, extraction count)
async def analyze_url(page: Page, url: str, hints: Dict[str, List[str]], job_id: int, total: int) -> Tuple[str, Dict[str, List[DiscoveredElement]], int]:
    extractions = 0
    try:
        await page.goto(url, timeout=Config.PAGE_TIMEOUT, wait_until='domcontentloaded')
        await page.wait_for_load_state('networkidle')
        await page.wait_for_selector('h2:has-text("Nature of business")', timeout=5000)  # Ensure SIC section renders

        # Pre-scan
        for sel in Config.PRE_SCAN_CLICKS:
            try: await page.click(sel, timeout=2000); await asyncio.sleep(1)
            except: pass

        analysis = await page.evaluate(JS_ANALYZER_CODE, hints)
        scored = {}
        for cat, elems in analysis.items():
            scored[cat] = []
            for el in elems[:10]:
                full_sel = el['selector']  # Drop ::text; we have innerText from JS
                sc, tier = score_selector(full_sel, el, hints.get(cat, []))
                # Use .count() instead of .all() for element count (avoids text parsing/escaping)
                count = await page.locator(full_sel).count()
                scored[cat].append({'selector': full_sel, 'score': sc, 'tier': tier, 'count': count, 'data': el})
                if el['extracted_code']: extractions += 1
        logger.info(f"Job {job_id}/{total}: {url} - Found {sum(len(v) for v in scored.values())} candidates ({extractions} codes extracted)")
        return url, scored, extractions
    except Exception as e:
        logger.error(f"Job {job_id}/{total}: Failed {url} - {e}")
        return url, None, 0

# Report (diagnostic-style: counters, rates, verdict)
def generate_report(stats: RunStats, current_selector: str, ranked: List[AnalysisData]) -> str:
    # FIX: Handle case where current_selector is a list to prevent TypeError
    current_str = current_selector[0] if isinstance(current_selector, list) else current_selector
    lines = [
        f"# SIC Colossus Report - {stats['start_time'].strftime('%Y-%m-%d %H:%M')}",
        "---",
        "## 📊 Summary",
        f"| Metric | Value |",
        "|:---|:---|",
        f"| Duration | {stats['total_duration']:.1f}s |",
        f"| Success/Fail | {stats['urls_success']}/{stats['urls_fail']} |",
        f"| Total Extractions | {stats['total_extractions']} |",
        f"| Extraction Rate | {stats['total_extractions'] / max(stats['urls_success'] * 5, 1):.1%} |",
        f"| Gold Recs | {stats['gold_recommendations']} |",
        "",
        "## Current vs Recommended",
        "| Selector | Score | Tier | Extraction Rate |",
        "|:---|:---|:---|:---|",
        # FIX: Use current_str in f-string to prevent TypeError
        f"| `{current_str}` | {score_selector(current_str, {'extracted_code': '', 'isVisible': True, 'depth': 5}, ['SIC'])[0]} | {score_selector(current_str, {'extracted_code': '', 'isVisible': True, 'depth': 5}, ['SIC'])[1]} | N/A |",
    ]
    for rec in ranked[:5]:
        lines.append(f"| `{rec['selector']}` | {rec['score']} | {rec['tier']} | {rec['extraction_rate']:.1%} |")
    lines.append("\n**Verdict**: " + ("🚀 Upgrade to Gold selector for 2x extraction rate." if stats['gold_recommendations'] > 0 else "⚠️ No Gold found; manual inspect sample pages."))
    return "\n".join(lines)

# Auto-Fuse (like fuse_selectors.py: clean/prioritize top 3 into array)
def auto_fuse(ranked: List[AnalysisData]):
    if not ranked: return
    top_3 = [r['selector'] for r in ranked[:3]]
    with open(Config.CURRENT_SELECTORS_FILE, 'r') as f:
        selectors = json.load(f)
    selectors['sources']['companies_house']['company_overview_page']['nature_of_business_sic'] = top_3
    with open(Config.CURRENT_SELECTORS_FILE, 'w') as f:
        json.dump(selectors, f, indent=2)
    logger.info(f"Auto-fused top 3: {top_3} into selectors.json")

# Main (added counters, progress via logs)
async def main():
    stats: RunStats = {'start_time': datetime.now(), 'gold_recommendations': 0, 'total_extractions': 0}
    logger.info("SIC Colossus v2.0 started - Analyzing 20 sample URLs.")

    # Load current
    with open(Config.CURRENT_SELECTORS_FILE, 'r') as f:
        selectors = json.load(f)
    current_sic = selectors['sources']['companies_house']['company_overview_page']['nature_of_business_sic']
    logger.info(f"Current SIC selector(s): {current_sic}")

    # Analyze
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=random.choice(Config.USER_AGENTS))

        semaphore = asyncio.Semaphore(Config.CONCURRENT_TASKS)
        tasks = []
        for i, url in enumerate(Config.SAMPLE_URLS):
            async def worker(url=url, i=i):  # Closure for per-task
                async with semaphore:
                    page = await context.new_page()
                    if STEALTH_AVAILABLE:
                        await stealth_async(page)
                    try:
                        return await analyze_url(page, url, Config.SIC_HINTS, i+1, len(Config.SAMPLE_URLS))
                    finally:
                        await page.close()
            tasks.append(worker())
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    # Aggregate (with counters)
    all_data: Dict[str, List[Dict]] = defaultdict(list)
    failed = []
    extraction_counter = Counter()
    for res in results:
        if isinstance(res, Exception):
            logger.error(f"Task failed: {res}")
            failed.append("Unknown")
            continue
        url, data, exts = res
        if data is None:
            failed.append(url)
            continue
        for cat, cands in data.items():
            for c in cands:
                c['url'] = url  # Track originating page
            all_data[cat].extend(cands)
        stats['total_extractions'] += exts
        extraction_counter[url] = exts

    stats['urls_success'] = len(Config.SAMPLE_URLS) - len(failed)
    stats['urls_fail'] = len(failed)

    # Rank (focus 'sic_code')
    ranked: List[AnalysisData] = []
    code_data = defaultdict(lambda: {'scores': [], 'counts': [], 'pages': set(), 'extractions': 0, 'total_pages': 0})
    for cand in all_data['sic_code']:
        sel = cand['selector']
        bucket = code_data[sel]
        bucket['scores'].append(cand['score'])
        bucket['counts'].append(cand['count'])
        bucket['pages'].add(cand['url'])
        if cand['data']['extracted_code']: bucket['extractions'] += 1

    for sel, bucket in code_data.items():
        if not bucket['scores']: continue
        bucket['total_pages'] = len(bucket['pages']) or 1  # Unique pages, avoid div/0
        avg_score = sum(bucket['scores']) / len(bucket['scores'])
        prevalence = len(bucket['pages'])
        final_score = avg_score + (prevalence * Config.SCORE_WEIGHTS['prevalence'])
        ext_rate = bucket['extractions'] / bucket['total_pages']
        _, tier = score_selector(sel, {'extracted_code': '73110', 'isVisible': True, 'depth': 3}, ['SIC'])
        ranked.append({
            'selector': sel, 'score': int(final_score), 'tier': tier,
            'avg_count': sum(bucket['counts'])/len(bucket['counts']),
            'prevalence': f"{prevalence}/{stats['urls_success']}",
            'extraction_rate': ext_rate
        })
        if tier == 'Gold': stats['gold_recommendations'] += 1

    ranked.sort(key=lambda x: x['score'], reverse=True)

    # Outputs
    rec_json = {'nature_of_business_sic': {'type': 'css', 'value': [r['selector'] for r in ranked]}}
    with open(Config.RECOMMENDED_SIC_FILE, 'w') as f:
        json.dump(rec_json, f, indent=2)
    auto_fuse(ranked)
    stats['end_time'] = datetime.now()
    stats['total_duration'] = (stats['end_time'] - stats['start_time']).total_seconds()
    report = generate_report(stats, current_sic, ranked)
    with open(Config.REPORT_FILE, 'w') as f:
        f.write(report)
    logger.info(f"Complete! Report: {Config.REPORT_FILE} | Extractions: {stats['total_extractions']}/{stats['urls_success']*5:.0f} ({stats['total_extractions'] / (stats['urls_success'] * 5):.1%})")

if __name__ == "__main__":
    asyncio.run(main())