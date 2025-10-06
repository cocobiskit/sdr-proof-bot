import os
import json
import logging
import re
import asyncio
import random
import concurrent.futures
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
from urllib.parse import urljoin, quote_plus, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser, BrowserContext, TimeoutError as PlaywrightTimeoutError
from email_validator import validate_email, EmailNotValidError
from phonenumbers import parse, is_valid_number, format_number, PhoneNumberFormat
import urllib.robotparser as robotparser

from .config import BotConfig
from .models import Lead
from . import utils


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logger = logging.getLogger('ProofBot.Scraper')

url_logger = logging.getLogger('VisitedUrls')
if not url_logger.handlers:
    # Ensure log directory exists if handler were to use an absolute path
    # For simplicity, keeping it relative as in the original code.
    handler = logging.FileHandler('visited_urls.log', 'w')
    handler.setFormatter(logging.Formatter('%(message)s'))
    url_logger.addHandler(handler)
    url_logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Main Class
# ---------------------------------------------------------------------------
class EliteWebScraper:
    """Async, production-grade scraper for both directory sources and Companies House."""

    def __init__(self, config: BotConfig):
        self.config = config
        self.session = self._create_session()
        self.rate_limiter = utils.DomainRateLimiter(min_delay_s=config.request_delay)
        self.selectors = self._load_selectors()
        self.target_sic_codes = self.config.sic_codes  # Use the sic_codes from config
        logger.info("EliteWebScraper initialized.")
        url_logger.info(f"# Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # -----------------------------------------------------------------------
    # Utilities
    # -----------------------------------------------------------------------
    def _create_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            'User-Agent': utils.choose_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.5',
        })
        s.timeout = 15
        return s

    def _merge_selectors(self, loaded: Dict, default: Dict) -> Dict:
        """Recursively merge loaded selectors with defaults, prioritizing loaded."""
        merged = {}
        for key, value in default.items():
            if key in loaded:
                if isinstance(value, dict) and isinstance(loaded[key], dict):
                    # Recursive merge for nested dicts (e.g., sources.companies_house)
                    merged[key] = self._merge_selectors(loaded[key], value)
                else:
                    merged[key] = loaded[key]  # Override with loaded
            else:
                merged[key] = value  # Use default
        # Add any extra keys from loaded not in default
        for key in loaded:
            if key not in default:
                merged[key] = loaded[key]
        return merged

    def _load_selectors(self) -> Dict:
        try:
            with open(self.config.selectors_file, 'r') as f:
                data = json.load(f)
                logger.info(f"Loaded selectors from {self.config.selectors_file}")
                defaults = self._default_selectors()
                return self._merge_selectors(data, defaults)  # <-- Merge here
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Could not load selectors.json: {e}; using defaults.")
            return self._default_selectors()

    def _default_selectors(self) -> Dict:
        logger.warning("Using fallback default selectors.")
        return {
            "sources": {
                "clutch": {
                    "url": "https://clutch.co/uk/agencies/digital-marketing/london",
                    "selectors": {
                        "agency_list": "li.provider-row",
                        "company_name": "h3.company-name a",
                        "website_link": ".website-link__item[href*='http']",
                        "location": "span.loc locality",
                    },
                },
                # Added comprehensive Companies House selectors for robustness
                "companies_house": {
                    "start_url": "https://find-and-update.company-information.service.gov.uk/",
                    "navigation": {
                        "accept_cookies_button": "button#cookie-accept-all-button",
                        "search_input": "input#searchText",
                        "search_submit": "form[action='/search/companies'] button[type='submit'], #search-submit",
                    },
                    "search_results_page": {
                        "result_rows": "ol.results-list li.type-company",
                        "company_link": "h3.company-name a",
                        "company_status_cell": "p.meta strong, dd.govuk-summary-list__value",
                        "active_status_text": "active",
                        "pagination_next_link": "li.govuk-pagination__next a"
                    },
                    "company_overview_page": {
                        "name_header": "h1.heading-xlarge",
                        "company_number": "p#company-number",
                        "registered_address": "dt:has-text('Registered office address') + dd",
                        "company_status": "dt:has-text('Company status') + dd",
                        "company_type": "dt:has-text('Company type') + dd",
                        "incorporation_date": "dt:has-text('Incorporated on') + dd",
                        "accounts": "dt:has-text('Accounts') + dd",
                        "confirmation_statement": "dt:has-text('Confirmation statement') + dd",
                        "nature_of_business_sic": [ # Prioritized SIC selectors
                            "div#sic-codes ul li",
                            "dt:has-text('Nature of business (SIC)') + dd",
                        ],
                        "people_tab_link": "a[href*='/officers']"
                    },
                    "officers_page": {
                        "officer_cards": "div.appointment-card",
                        "officer_name_link": "h2 a",
                        "officer_role": "p.officer-role",
                        "officer_role_status": "span.govuk-tag",
                        "active_role_text": "active",
                    },
                    "officer_appointments_page": {
                        "officer_dob": "dt:has-text('Date of birth') + dd",
                        "officer_nationality": "dt:has-text('Nationality') + dd",
                        "officer_residence": "dt:has-text('Country of residence') + dd",
                        "officer_occupation": "dt:has-text('Occupation') + dd",
                        "officer_appointment_date": "dt:has-text('Date of appointment') + dd",
                        "other_appointments_list": "div.appointments-list div.appointment-card"
                    }
                }
            },
            "generic_website": {
                "phone_patterns": [
                    r"\+44\s?\(?0?\)?\s?\d{2,4}[\s.\-]?\d{3,4}[\s.\-]?\d{3,4}",
                    r"\(?0\d{2,4}\)?[\s.\-]?\d{3,4}[\s.\-]?\d{3,4}",
                ],
                "email_pattern": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            },
        }

    async def _inject_stealth(self, page: Page):
        # Assuming 'stealth.js' is in the same directory as this file or accessible via the path logic
        path = os.path.join(os.path.dirname(__file__), 'stealth.js')
        try:
            with open(path, 'r') as f:
                await page.add_init_script(f.read())
            logger.debug("Injected stealth script.")
        except Exception as e:
            logger.warning(f"Could not inject stealth script: {e}")

    def _normalize(self, s: Optional[str]) -> str:
        return (s or "").strip()

    def _address_matches(self, address: str) -> bool:
        loc = (self.config.target_location or "").strip().lower()
        if not loc:
            return True
        return loc in (address or "").strip().lower()

    def _extract_sic_codes_list(self, sic_text: str) -> List[str]:
        """Extract list of 5-digit SIC codes from a blob of text."""
        if not sic_text or sic_text == "Unknown":
            return []
        codes = re.findall(r"\b(\d{5})\b", sic_text)
        return list(dict.fromkeys(codes))  # de-dupe, keep order

    def _sic_matches_target(self, sic_text: str) -> bool:
        target = set(self.target_sic_codes or [])
        if not target:
            return True
        codes = set(self._extract_sic_codes_list(sic_text))
        return not target.isdisjoint(codes)

    def _compute_icp_score(self, sic_text: str, address: str) -> float:
        industry_match = 1.0 if self._sic_matches_target(sic_text) else 0.0
        geo_match = 1.0 if self._address_matches(address) else 0.0
        # Simple weighted score; adjust as needed
        return round(0.7 * industry_match + 0.3 * geo_match, 2)

    def _respect_robots_allowed(self, url: str) -> bool:
        if not getattr(self.config, "respect_robots", False):
            return True
        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            ua = self.session.headers.get("User-Agent", "*")
            return rp.can_fetch(ua, url)
        except Exception:
            # Fail open to avoid blocking entire pipeline; log only
            logger.debug(f"robots.txt check failed for {url}, proceeding.")
            return True

    async def _extract_field(self, page: Page, dt_text: str) -> str:
        """Extract text from dd following dt with given text."""
        try:
            dt_locator = page.locator(f"dt:has-text('{dt_text}')")
            if await dt_locator.count() == 0:
                return "Unknown"
            dd_locator = dt_locator.locator("+ dd")
            # Get text robustly, handling multiple children if necessary, then strip
            text_content = await dd_locator.inner_text()
            return text_content.strip() or "Unknown"
        except Exception as e:
            logger.debug(f"Error extracting '{dt_text}': {e}")
            return "Unknown"

    async def _extract_sic(self, page: Page) -> str:
        """Extract SIC codes, robustly using configured selectors or fallback."""
        sic_codes_text_parts = []
        
        ch_selectors = self.selectors.get("sources", {}).get("companies_house", {})
        sic_selector_configs = ch_selectors.get("company_overview_page", {}).get("nature_of_business_sic", [])

        # Prioritize configured SIC selectors if available
        if sic_selector_configs:
            for sel in sic_selector_configs:
                try:
                    elements = await page.locator(sel).all()
                    for el in elements:
                        text = await el.inner_text()
                        if re.search(r'\b\d{5}\b', text): # Only add text that seems to contain a 5-digit code
                            sic_codes_text_parts.append(text)
                    if sic_codes_text_parts: # If we found something with this selector, break and process
                        break # Found codes, no need to try other selectors
                except PlaywrightTimeoutError:
                    logger.debug(f"SIC config selector '{sel}' timed out on {page.url}.")
                except Exception as e:
                    logger.debug(f"Error using SIC config selector '{sel}' on {page.url}: {e}")
        
        # If no codes found via config selectors, or if config selectors weren't defined, use existing dt+dd logic
        if not sic_codes_text_parts:
            try:
                dt_text = "Nature of business (SIC)"
                dt_locator = page.locator(f"dt:has-text('{dt_text}')")
                if await dt_locator.count() > 0:
                    dd_locator = dt_locator.locator("+ dd")
                    dd_text = await dd_locator.inner_text()
                    if re.search(r'\b\d{5}\b', dd_text): # Check if dd_text contains SIC codes
                        sic_codes_text_parts.append(dd_text)
            except Exception as e:
                logger.debug(f"Fallback dt+dd SIC extraction error: {e}")

        # Process the collected text parts
        if sic_codes_text_parts:
            combined_sic_text = "\n".join(sic_codes_text_parts)
            extracted_codes = self._extract_sic_codes_list(combined_sic_text)
            if extracted_codes:
                logger.debug(f"SIC extracted: {', '.join(extracted_codes)} from {page.url}")
                return ", ".join(extracted_codes)

        logger.debug(f"SIC not found for {page.url}, returning 'Unknown'.")
        return "Unknown"

    async def _parse_accounts(self, accounts_text: str) -> Tuple[str, str]:
        """Parse accounts text into next and last."""
        accounts_next = "Unknown"
        accounts_last = "Unknown"
        if accounts_text and accounts_text != "Unknown":
            # Next accounts made up to 31 July 2025 due by 30 April 2026
            next_match = re.search(r'Next accounts made up to\s*(.+?)\s*(?:due by\s*(.+))?', accounts_text, re.IGNORECASE)
            if next_match:
                made_up_to = (next_match.group(1) or "").strip()
                due_by = (next_match.group(2) or "").strip()
                accounts_next = f"{made_up_to}{' due by ' + due_by if due_by else ''}".strip()
            # Last accounts made up to 31 July 2024
            last_match = re.search(r'Last accounts made up to\s*(.+)', accounts_text, re.IGNORECASE)
            if last_match:
                accounts_last = (last_match.group(1) or "").strip()
        return accounts_next, accounts_last

    async def _parse_confirmation(self, conf_text: str) -> Tuple[str, str]:
        """Parse confirmation text into next and last."""
        conf_next = "Unknown"
        conf_last = "Unknown"
        if conf_text and conf_text != "Unknown":
            # Next statement date 5 July 2026 due by 19 July 2026
            next_match = re.search(r'Next statement date\s*(.+?)\s*(?:due by\s*(.+))?', conf_text, re.IGNORECASE)
            if next_match:
                date = (next_match.group(1) or "").strip()
                due_by = (next_match.group(2) or "").strip()
                conf_next = f"{date}{' due by ' + due_by if due_by else ''}".strip()
            # Last statement dated 5 July 2025
            last_match = re.search(r'Last statement dated\s*(.+)', conf_text, re.IGNORECASE)
            if last_match:
                conf_last = (last_match.group(1) or "").strip()
        return conf_next, conf_last

    def _build_target_queries(self) -> List[str]:
        """Build targeted CH queries from config."""
        industry = (self.config.target_industry or "").strip()
        location = (self.config.target_location or "").strip()

        seeds = []
        if industry and location:
            seeds.append(f"{industry} {location}")
        if location:
            seeds.extend([
                f"digital marketing {location}",
                f"marketing agency {location}",
                f"advertising agency {location}",
                f"creative agency {location}",
            ])
        else:
            seeds.extend([
                "digital marketing agency",
                "advertising agency",
            ])
        # De-dup, keep order
        queries = list(dict.fromkeys([s for s in seeds if s]))
        logger.info(f"Target queries for Companies House: {queries}")
        return queries

    async def _collect_company_links_from_search(self, page: Page) -> List[str]:
        """Collect company profile links from a CH search results page."""
        # Be robust to layout: collect any anchor to /company/<number>
        links = []
        try:
            # Using a more specific selector from CH results page structure
            anchors = await page.locator("a.govuk-link[href*='/company/']").all()
            for a in anchors:
                href = await a.get_attribute("href")
                # Filter for actual company profile links
                if href and "/company/" in href and not href.endswith("/filing-history"):
                    links.append(urljoin(page.url, href))
        except Exception as e:
            logger.debug(f"Error collecting company links: {e}")
        # De-dup while preserving order
        links = list(dict.fromkeys(links))
        return links

    # -----------------------------------------------------------------------
    # Simple directory scraper (example: Clutch)
    # -----------------------------------------------------------------------
    async def scrape_clutch(self, page: Page) -> List[Lead]:
        leads: List[Lead] = []
        if 'clutch' not in self.selectors.get('sources', {}):
            return leads
        src = self.selectors['sources']['clutch']
        url_logger.info(f"SOURCE: {src['url']}")

        try:
            # Use Playwright for dynamic page load
            await utils.polite_goto(page, src['url'], logger=logger, limiter=self.rate_limiter)
            await utils.ensure_consent(page, logger=logger)
            # Scroll to load more (Clutch typically lazy-loads)
            for _ in range(3):
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(random.uniform(2.0, 3.0))

            soup = BeautifulSoup(await page.content(), "html.parser")
            
            # Select the main listing elements
            list_elements = soup.select(src["selectors"]["agency_list"])
            
            for a in list_elements[: self.config.target_count]:
                name_el = a.select_one(src["selectors"]["company_name"])
                # Website link can be in a data attribute or href, checking for common pattern
                site_el = a.select_one(src["selectors"]["website_link"])
                loc_el = a.select_one(src["selectors"]["location"])

                name = name_el.get_text(strip=True) if name_el else "Unknown"
                website = site_el.get("href") if site_el else None
                loc = loc_el.get_text(strip=True) if loc_el else self.config.target_location
                
                # Simple filtering checks
                if not website:
                    continue
                if self.config.target_location and self.config.target_location.lower() not in (loc or "").lower():
                    continue

                leads.append(
                    Lead(company_name=name, website=website, location=loc,
                         source="Clutch.co", scraped_at=datetime.now())
                )
                logger.info(f"✓ Found on Clutch: {name} | {website}")
        except Exception as e:
            logger.error(f"Error scraping Clutch: {e}", exc_info=True)
        return leads

    # -----------------------------------------------------------------------
    # Companies House Helper: Scrape Single Company Profile
    # -----------------------------------------------------------------------
    async def _scrape_single_company_profile(self, company_url: str, context: BrowserContext) -> Optional[Lead]:
        """
        Navigates to a single Companies House company profile URL, scrapes all details
        including officers, and returns a Lead object.
        """
        key = "companies_house"
        sel = self.selectors["sources"][key]
        num = "Unknown"  # Initialize num

        company_page = await context.new_page()
        await self._inject_stealth(company_page)

        try:
            logger.info(f"Scraping company profile: {company_url}")
            url_logger.info(f"COMPANY: {company_url}")
            # Use polite_goto for individual profile pages too
            await utils.polite_goto(company_page, company_url, logger=logger, limiter=self.rate_limiter)

            # Extract main company details
            name_locator = company_page.locator("h1.heading-xlarge")
            name = (await name_locator.inner_text()).strip() if await name_locator.count() else "Unknown"
            
            num_raw_locator = company_page.locator("p#company-number")
            if await num_raw_locator.count() > 0:
                num_raw = await num_raw_locator.inner_text()
                num = re.sub(r"Company number\s*", "", num_raw, flags=re.IGNORECASE).strip()
            else:
                match_num_from_url = re.search(r'/company/([A-Za-z0-9]+)', company_url)
                if match_num_from_url:
                    num = match_num_from_url.group(1)
                else:
                    num = "Unknown"

            address = await self._extract_field(company_page, "Registered office address")
            c_status = await self._extract_field(company_page, "Company status")
            c_type = await self._extract_field(company_page, "Company type")
            inc_date = await self._extract_field(company_page, "Incorporated on")
            accounts_text = await self._extract_field(company_page, "Accounts")
            conf_text = await self._extract_field(company_page, "Confirmation statement")
            sic = await self._extract_sic(company_page)

            # Pre-filter checks (redundant if main loop filters, but good for isolated calls)
            if "active" not in (c_status or "").lower():
                logger.debug(f"Skipping {name} due to status: {c_status}")
                return None
            if not self._sic_matches_target(sic):
                logger.debug(f"Skipping {name} due to SIC: {sic}")
                return None
            if not self._address_matches(address):
                logger.debug(f"Filtered out geo {name} | Addr: {address}")
                return None

            accounts_next, accounts_last = await self._parse_accounts(accounts_text)
            conf_next, conf_last = await self._parse_confirmation(conf_text)

            lead = Lead(
                company_name=name,
                website=f"https://find-and-update.company-information.service.gov.uk/company/{num}",
                location=address,
                source='Companies House',
                scraped_at=datetime.now(),
                officers=[]
            )
            lead.status = c_status
            lead.company_type = c_type
            lead.incorporation_date = inc_date
            match = re.search(r"\b(19|20)\d{2}\b", inc_date or "")
            lead.founded_year = match[0] if match else None
            lead.sic_codes = sic
            lead.accounts_next = accounts_next
            lead.accounts_last = accounts_last
            lead.confirmation_next = conf_next
            lead.confirmation_last = conf_last

            active_officers = []
            try:
                people_tab_locator = company_page.locator(sel['company_overview_page']['people_tab_link'])
                if await people_tab_locator.count() > 0 and await people_tab_locator.is_enabled(timeout=5000):
                    await people_tab_locator.click()
                    await company_page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(random.uniform(1.0, 2.0))

                    cards = await company_page.locator(sel['officers_page']['officer_cards']).all()
                    for card in cards:
                        try:
                            status_span_locator = card.locator(sel['officers_page']['officer_role_status'])
                            status_span = (await status_span_locator.inner_text()).strip() if await status_span_locator.count() else ""
                            if sel['officers_page']['active_role_text'].lower() not in status_span.lower():
                                continue
                                
                            name_loc = card.locator(sel['officers_page']['officer_name_link']).first
                            officer_name = (await name_loc.inner_text()).strip() if await name_loc.count() else "Unknown"
                            officer_href = await name_loc.get_attribute('href') if await name_loc.count() else None
                            
                            role_text = "Unknown"
                            try:
                                role_text_locator = card.locator(sel['officers_page']['officer_role'])
                                if await role_text_locator.count() > 0:
                                    role_text = (await role_text_locator.inner_text()).strip()
                            except Exception:
                                pass
                            
                            officer_dict = {"name": officer_name, "role": role_text, "status": status_span, "link": officer_href}
                            
                            if officer_href:
                                officer_url = urljoin(company_page.url, officer_href)
                                officer_detail_page = await context.new_page()
                                await self._inject_stealth(officer_detail_page)
                                try:
                                    # Use polite_goto for officer profile pages
                                    await utils.polite_goto(officer_detail_page, officer_url, logger=logger, limiter=self.rate_limiter)
                                    selectors = sel['officer_appointments_page']
                                    
                                    async def _get_officer_field(p: Page, selector: str) -> str:
                                        loc = p.locator(selector)
                                        return (await loc.inner_text()).strip() if await loc.count() else "Unknown"

                                    dob = await _get_officer_field(officer_detail_page, selectors['officer_dob'])
                                    nationality = await _get_officer_field(officer_detail_page, selectors['officer_nationality'])
                                    residence = await _get_officer_field(officer_detail_page, selectors['officer_residence'])
                                    occupation = await _get_officer_field(officer_detail_page, selectors['officer_occupation'])
                                    appointed_on = await _get_officer_field(officer_detail_page, selectors['officer_appointment_date'])
                                    
                                    officer_dict.update({
                                        'dob': dob,
                                        'nationality': nationality,
                                        'residence': residence,
                                        'occupation': occupation,
                                        'appointed_on': appointed_on
                                    })
                                    
                                    # Extract other appointments with better regex
                                    appts_elements = await officer_detail_page.locator("div.appointments-list > div").all()
                                    officer_appointments = []
                                    for appt_el in appts_elements:
                                        appt_text = await appt_el.inner_text()
                                        # ### PATCH START ###
                                        # More flexible regex to handle variations in company number formatting
                                        company_match = re.search(r'(.+?)\s+\(?(?:Company number:\s*)?([A-Za-z0-9]{2,}\d{6,})\)?\s+Appointed on\s+(.+)', appt_text, re.IGNORECASE)
                                        # ### PATCH END ###
                                        if company_match:
                                            officer_appointments.append({
                                                'company': company_match.group(1).strip(),
                                                'company_number': company_match.group(2).strip(),
                                                'date': company_match.group(3).strip()
                                            })
                                        else: 
                                            # Fallback for less structured text
                                            parts = appt_text.split('Appointed on ')
                                            if len(parts) > 1:
                                                company_info = parts[0].strip()
                                                fallback_company_match = re.match(r'(.+?)\s+\(?(?:Company number:\s*)?([A-Za-z0-9]{2,}\d{6,})\)?', company_info, re.IGNORECASE)
                                                if fallback_company_match:
                                                    officer_appointments.append({
                                                        'company': fallback_company_match.group(1).strip(),
                                                        'company_number': fallback_company_match.group(2).strip(),
                                                        'date': parts[-1].strip()
                                                    })
                                                else:
                                                    officer_appointments.append({'company': company_info, 'date': parts[-1].strip()})
                                            elif appt_text.strip():
                                                officer_appointments.append({'company': appt_text.strip(), 'date': 'Unknown'})


                                    officer_dict['appointments'] = officer_appointments

                                except Exception as e:
                                    logger.debug(f"Failed to get officer details for {officer_name} at {officer_url}: {e}")
                                finally:
                                    await officer_detail_page.close()
                                    await asyncio.sleep(random.uniform(1.5, 2.5)) # Polite delay
                            active_officers.append(officer_dict)
                        except Exception as off_err:
                            logger.debug(f"Officer card processing error for {company_url}: {off_err}")
                else:
                    logger.debug(f"No 'People' tab found or enabled for {name}.")
            except Exception as e:
                logger.debug(f"Error processing officers section for {name}: {e}")
                
            lead.officers = active_officers
            lead.ideal_customer_profile_match = self._compute_icp_score(sic, address)
            
            logger.info(f"✓ Scraped company profile: {lead.company_name} | SIC: {sic} | ICP: {lead.ideal_customer_profile_match}")
            return lead

        except Exception as e:
            logger.error(f"Failed to process company {company_url}: {e}", exc_info=True)
            return None
        finally:
            await company_page.close()
            await asyncio.sleep(random.uniform(0.8, 1.6)) # Polite delay after closing page


    # -----------------------------------------------------------------------
    # Robust & Polite Companies House scraper (targeted)
    # -----------------------------------------------------------------------
    async def scrape_companies_house(self, browser: Browser) -> List[Lead]:
        key = "companies_house"
        if key not in self.selectors.get("sources", {}):
            logger.warning("Companies House selectors missing; skipping.")
            return []

        sel = self.selectors["sources"][key]
        leads: List[Lead] = []
        visited_company_urls = set()

        # Prepare proxy if available (requires Playwright to support it)
        proxy_dict = None
        if self.config.use_proxies and self.config.proxy_pool:
            proxy_dict = {'server': random.choice(self.config.proxy_pool)}

        # New context for isolation and stealth (proxy is passed at context creation)
        context = await utils.new_context_with_profile(browser, proxy=proxy_dict)
        # Main search page for navigation
        search_page = await context.new_page() # Renamed to search_page for clarity
        await self._inject_stealth(search_page) 

        try:
            logger.info(f"Navigating to Companies House homepage: {sel['start_url']}")
            await utils.polite_goto(search_page, sel['start_url'], logger=logger, limiter=self.rate_limiter)
            url_logger.info(f"SOURCE: {sel['start_url']}")

            try:
                # Attempt to accept cookies using the configured selector
                cookie_button_locator = search_page.locator(sel['navigation']['accept_cookies_button'])
                if await cookie_button_locator.count() > 0 and await cookie_button_locator.is_visible(timeout=5000):
                    await cookie_button_locator.click(timeout=5000)
                    logger.info("Accepted cookies.")
                else:
                    logger.info("Cookie banner not found or already accepted.")
            except PlaywrightTimeoutError:
                logger.info("Cookie banner not found or already accepted (timeout).")
            except Exception as e:
                logger.debug(f"Error handling cookie banner: {e}")
            
            await asyncio.sleep(random.uniform(1.5, 3.0))

            if self.config.exhaustive_mode:
                # --- Exhaustive Alphabetical Search Mode ---
                logger.info("Using exhaustive alphabetical mode...")
                alpha_search_url = "https://find-and-update.company-information.service.gov.uk/alphabetical-search"
                await utils.polite_goto(search_page, alpha_search_url, logger=logger, limiter=self.rate_limiter)
                await asyncio.sleep(random.uniform(2, 4)) 

                for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                    if len(leads) >= self.config.target_count:
                        break

                    logger.info(f"Scraping letter: {letter}")
                    
                    input_key = 'search_input' if 'search_input' in sel['navigation'] else 'search_input_box'
                    input_sel = sel['navigation'][input_key]
                    await search_page.wait_for_selector(input_sel, timeout=60000)
                    await search_page.fill(input_sel, letter)
                    await search_page.click(sel['navigation']['search_submit'], timeout=10000) 
                    await search_page.wait_for_load_state("networkidle", timeout=60000)
                    await asyncio.sleep(random.uniform(2.0, 3.5))

                    page_num = 1
                    while len(leads) < self.config.target_count:
                        logger.info(f"Scraping page {page_num} for letter {letter}")
                        
                        await search_page.wait_for_selector(sel['search_results_page']['result_rows'], timeout=30000)

                        rows = await search_page.locator(sel['search_results_page']['result_rows']).all()
                        if not rows:
                            logger.info(f"No results found on page {page_num} for letter {letter}")
                            break
                        
                        for row in rows:
                            if len(leads) >= self.config.target_count:
                                break

                            try:
                                company_link_el = row.locator(sel['search_results_page']['company_link']).first
                                company_href = await company_link_el.get_attribute('href')
                                if not company_href:
                                    continue
                                company_url = urljoin(search_page.url, company_href)

                                status_el = row.locator(sel['search_results_page']['company_status_cell']).first
                                status = await status_el.inner_text()
                                if sel['search_results_page']['active_status_text'].lower() not in status.lower():
                                    continue

                                if company_url in visited_company_urls:
                                    continue
                                visited_company_urls.add(company_url)

                                # Call the new helper method to scrape the company profile
                                profile_lead = await self._scrape_single_company_profile(company_url, context)
                                if profile_lead:
                                    leads.append(profile_lead)

                            except Exception as e:
                                logger.error(f"Failed to process a row from alphabetical search results: {e}")

                        next_link_locator = search_page.locator(sel['search_results_page']['pagination_next_link'])
                        if await next_link_locator.count() == 0 or not await next_link_locator.is_enabled():
                            break
                        
                        await next_link_locator.click()
                        await search_page.wait_for_load_state("networkidle")
                        page_num += 1
                        await asyncio.sleep(random.uniform(2.0, 4.0))

                    await asyncio.sleep(random.uniform(3.0, 5.0))
            else:
                # --- Targeted Query Search Mode (Default) ---
                logger.info("Using targeted query mode...")
                queries = self._build_target_queries()
                if not queries:
                    logger.warning("No target queries built; skipping.")
                    return []

                for q in queries:
                    if len(leads) >= self.config.target_count: # Stop if global target met
                        break

                    logger.info(f"CH Search: {q}")
                    
                    # Navigate to search results for this query. Use polite_goto.
                    search_url = urljoin(sel['start_url'], f"search/companies?q={quote_plus(q)}")
                    await utils.polite_goto(search_page, search_url, logger=logger, limiter=self.rate_limiter)
                    
                    page_num = 1
                    # --- Start Pagination Loop for Targeted Search ---
                    while len(leads) < self.config.target_count: # Continue until global target is met
                        logger.info(f"Scraping page {page_num} for query '{q}'")
                        
                        await search_page.wait_for_load_state("networkidle", timeout=30000)
                        company_links = await self._collect_company_links_from_search(search_page)
                        
                        if not company_links:
                            logger.info(f"No more company links on page {page_num} for query '{q}'.")
                            break # No more links, break pagination for this query

                        for company_url in company_links:
                            if len(leads) >= self.config.target_count:
                                break # Stop if global target met
                            if company_url in visited_company_urls:
                                continue
                            visited_company_urls.add(company_url)

                            # Call the new helper method to scrape the company profile
                            profile_lead = await self._scrape_single_company_profile(company_url, context)
                            if profile_lead:
                                leads.append(profile_lead)

                        # Check if global target count is met after processing a page of links
                        if len(leads) >= self.config.target_count:
                            logger.info(f"Target count {self.config.target_count} reached. Stopping pagination.")
                            break # Break pagination loop
                        
                        # Pagination for query results (using rel='next' as a common CH pattern)
                        next_link_locator = search_page.locator(sel['search_results_page']['pagination_next_link'])
                        if await next_link_locator.count() == 0 or not await next_link_locator.is_enabled(timeout=3000): # Reduced timeout for check
                            logger.info(f"No next pagination link found for query '{q}' on page {page_num}.")
                            break # No next link or disabled, break pagination for this query
                        
                        await next_link_locator.click()
                        await search_page.wait_for_load_state("networkidle", timeout=60000)
                        page_num += 1
                        await asyncio.sleep(random.uniform(2.0, 4.0)) # Polite delay between pages

                    await asyncio.sleep(random.uniform(3.0, 5.0)) # Delay between queries
        except Exception as e:
            logger.critical(f"A critical error occurred during Companies House scrape: {e}", exc_info=True)
        finally:
            await search_page.close()
            await context.close()

        return leads


    # -----------------------------------------------------------------------
    # Website discovery and enrichment (NOW ASYNC)
    # -----------------------------------------------------------------------
    def _clean_candidate_url(self, url: str) -> Optional[str]:
        """Normalize URL and filter out known non-company domains."""
        try:
            u = urlparse(url)
            if not u.scheme:
                url = "https://" + url
                u = urlparse(url)
            if not u.netloc:
                return None
            
            # List of domains that are typically not a company's main website
            bad_hosts = [
                "find-and-update.company-information.service.gov.uk",
                "companieshouse.gov.uk", "linkedin.com", "facebook.com",
                "twitter.com", "x.com", "instagram.com", "glassdoor.com",
                "yell.com", "maps.google", "crunchbase.com", "companycheck.co.uk",
                "opencorporates.com", "companiesintheuk.co.uk", "bing.com", "duckduckgo.com"
            ]
            if any(bad in u.netloc for bad in bad_hosts):
                return None
                
            # Return only scheme and netloc (root domain)
            return f"{u.scheme}://{u.netloc}"
        except Exception:
            return None

    async def _guess_website_via_search(self, company_name: str, location_hint: Optional[str] = None) -> Optional[str]:
        """Use a simple search (Bing -> DuckDuckGo fallback) to guess official website."""
        queries = []
        # Prioritize queries that are most likely to yield the official website
        if company_name:
            queries.append(f'"{company_name}" official website')
            if location_hint:
                queries.append(f'"{company_name}" {location_hint} website')
            # Fallbacks
            queries.append(f'"{company_name}"')


        # Search engines with selectors for main link
        engines = [
            ("bing", "https://www.bing.com/search?q={q}", "li.b_algo h2 a"),
            # DuckDuckGo requires the html endpoint for static parsing
            ("ddg", "https://duckduckgo.com/html/?q={q}", "a.result__a"),
        ]

        for q in queries:
            for name, tmpl, selector in engines:
                url = tmpl.format(q=quote_plus(q))
                try:
                    await self.rate_limiter.wait(url) # Respect rate limits
                    resp = await asyncio.to_thread(
                        self.session.get, url, allow_redirects=True, timeout=12
                    )
                    if resp.status_code != 200:
                        continue
                        
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for a in soup.select(selector):
                        href = a.get("href")
                        candidate = self._clean_candidate_url(href) if href else None
                        
                        # Ensure the candidate domain contains part of the company name as a heuristic
                        if candidate and company_name.lower().split()[0] in candidate.lower():
                            logger.info(f"Guessed website for {company_name}: {candidate} (q={q}, engine={name})")
                            return candidate
                            
                        # Less strict fallback: just return the first clean candidate
                        if candidate:
                             logger.debug(f"Found potential candidate {candidate} for {company_name} (q={q}, engine={name})")
                             return candidate

                except Exception as e:
                    logger.debug(f"Search error ({name}/{url}): {e}")
        return None

    def _harvest_contacts_from_html(self, html: str, base_url: str, lead: Lead):
        """Extract phones, emails, and social links from raw HTML."""
        sel = self.selectors.get("generic_website", {})
        
        # --- Phones ---
        if not getattr(lead, "phone_number", None):
            for pat in sel.get("phone_patterns", []):
                for m in re.finditer(pat, html):
                    try:
                        # Use phonenumbers library for robust UK number validation and formatting
                        n = parse(m.group(0), "GB")
                        if is_valid_number(n):
                            lead.phone_number = format_number(n, PhoneNumberFormat.INTERNATIONAL)
                            lead.phone_verified = True
                            logger.info(f"✓ Phone: {lead.phone_number}")
                            break
                    except Exception:
                        continue
                if getattr(lead, "phone_number", None):
                    break

        # --- Emails ---
        ep = sel.get("email_pattern")
        if ep and not getattr(lead, "email", None):
            for m in re.finditer(ep, html):
                email = m.group(0)
                # Filter common false positives
                if any(x in email.lower() for x in ['example.com', '.png', '.jpg', '.svg', '.gif', 'w3.org']):
                    continue
                try:
                    # Use email_validator for basic format check
                    v = validate_email(email, check_deliverability=False)
                    lead.email, lead.email_verified = v.email, True
                    logger.info(f"✓ Email: {lead.email}")
                    break
                except EmailNotValidError:
                    continue

        # --- Socials ---
        socials = getattr(lead, "socials", {}) or {}
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                h = href.lower()
                # Check for social media links
                if "linkedin.com/company" in h or "linkedin.com/in" in h:
                    socials["linkedin"] = href
                elif "facebook.com" in h and "sharer" not in h: # Exclude share links
                    socials["facebook"] = href
                elif "twitter.com" in h or "x.com" in h:
                    socials["twitter"] = href
                elif "instagram.com" in h:
                    socials["instagram"] = href
        except Exception:
            pass
        if socials:
            lead.socials = socials

    async def _get_with_robots(self, url: str) -> Optional[str]:
        """Perform a simple HTTP GET request, respecting robots.txt and rate limits."""
        if not url:
            return None
        if not self._respect_robots_allowed(url):
            logger.info(f"robots.txt disallows fetching {url}; skipping.")
            return None
        try:
            await self.rate_limiter.wait(url)
            resp = await asyncio.to_thread(
                self.session.get, url, allow_redirects=True, timeout=15
            )
            resp.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
            return resp.text
        except requests.RequestException as e:
            logger.debug(f"HTTP GET failed for {url}: {e}")
            return None

    def _try_paths(self, root_url: str) -> List[str]:
        """Generate common paths to check for contact details."""
        paths = ["", "/contact", "/contact-us", "/about", "/about-us", "/services"]
        root = self._clean_candidate_url(root_url) or root_url
        if not root:
            return []
            
        # Ensure root has no trailing slash before appending path segments
        urls = list(dict.fromkeys([root.rstrip("/") + p for p in paths]))
        return urls

    async def _enrich_via_http(self, lead: Lead) -> Lead:
        """HTTP-only enrichment: phones, emails, socials; respect robots."""
        if not lead.website:
            return lead
        url_logger.info(f"ENRICH: {lead.website}")

        # Iterate through homepage + contact/about pages
        for u in self._try_paths(lead.website):
            html = await self._get_with_robots(u)
            if not html:
                continue
            
            self._harvest_contacts_from_html(html, u, lead)
            
            # Short-circuit if we have both essential contacts
            if getattr(lead, "email", None) and getattr(lead, "phone_number", None):
                break
                
        return lead

    async def _screenshot_url(self, browser: Browser, url: str, out_path: str) -> Optional[str]:
        """Captures a full-page screenshot of a URL using Playwright."""
        try:
            # New context for isolation, using a randomized user agent
            context = await browser.new_context(user_agent=utils.choose_user_agent(), locale="en-GB")
            page = await context.new_page()
            await self._inject_stealth(page)
            
            await page.goto(url, wait_until="networkidle", timeout=30000)
            
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            await page.screenshot(path=out_path, full_page=True)
            
            await page.close()
            await context.close()
            return out_path
            
        except Exception as e:
            logger.debug(f"screenshot failed for {url}: {e}")
            return None

    def _choose_ceo_from_officers(self, officers: List[Dict]) -> Optional[str]:
        """Selects the name of the most likely CEO/Director from the officer list."""
        if not officers:
            return None
            
        # Prefer titles suggesting top leadership
        preferred = ["chief executive officer", "chief executive", "ceo", "managing director", "director", "owner", "founder", "proprietor"]
        
        for o in officers:
            role = (o.get("role") or "").lower()
            name = (o.get("name") or "").strip()
            
            # Simple keyword match on role
            if any(p in role for p in preferred) and name:
                return name
                
        # Fallback: first officer listed is often the most senior
        return officers[0].get("name") if officers else None

    async def _maybe_find_ceo_linkedin(self, company_name: str, ceo_name: str) -> Optional[str]:
        """Best-effort guess for CEO's LinkedIn profile via search."""
        if not company_name or not ceo_name:
            return None
            
        # Targeted search queries for LinkedIn profiles
        queries = [
            f'site:linkedin.com/in "{ceo_name}" "{company_name}"',
            f'site:linkedin.com/in "{ceo_name}" marketing director', # Added a role hint
            f'site:linkedin.com/in "{ceo_name}"',
        ]
        
        # Use Bing as a typical, reliable search engine for this purpose
        for q in queries:
            url = f"https://www.bing.com/search?q={quote_plus(q)}"
            try:
                await self.rate_limiter.wait(url)
                resp = await asyncio.to_thread(self.session.get, url, timeout=10)
                html = resp.text
                soup = BeautifulSoup(html, "html.parser")
                
                # Bing selector for result links
                for a in soup.select("li.b_algo h2 a"):
                    href = a.get("href", "")
                    if "linkedin.com/in/" in href:
                        logger.debug(f"Found CEO LinkedIn for {ceo_name}: {href}")
                        return href
                        
            except Exception as e:
                logger.debug(f"LinkedIn search error: {e}")
                continue
                
        return None

    async def enrich_lead_data(self, lead: Lead) -> Lead:
        """Asynchronous enrichment step."""
        logger.debug(f"Starting enrichment for: {lead.company_name}")
        
        # --- 1. Website Discovery ---
        ch_domains = [
            "find-and-update.company-information.service.gov.uk",
            "companieshouse.gov.uk"
        ]
        website_host = urlparse(lead.website or "").netloc.lower() if lead.website else ""
        # If no website, or it's a Companies House URL, try to guess the real website
        if not lead.website or any(h in website_host for h in ch_domains):
            guessed = await self._guess_website_via_search(lead.company_name, lead.location)
            if guessed:
                lead.website = guessed
                logger.info(f"Updated CH website to guessed: {lead.website}")

        # --- 2. HTTP-only enrichment (phones, emails, socials) ---
        lead = await self._enrich_via_http(lead)

        # --- 3. CEO/LinkedIn Inference ---
        try:
            ceo_name = self._choose_ceo_from_officers(getattr(lead, "officers", []))
            if ceo_name:
                lead.ceo_name = ceo_name
                # Only attempt LinkedIn if it's missing
                if not getattr(lead, "ceo_linkedin", None):
                    lead.ceo_linkedin = await self._maybe_find_ceo_linkedin(lead.company_name, ceo_name)
                    if lead.ceo_linkedin:
                        logger.info(f"Found CEO LinkedIn: {lead.ceo_linkedin}")
        except Exception as e:
            logger.warning(f"Error during CEO/LinkedIn inference for {lead.company_name}: {e}")

        # --- 4. Final ICP/Quality Score Computation ---
        
        # Compute ICP if not already done (mainly for Clutch leads)
        if not getattr(lead, "ideal_customer_profile_match", None):
            sic = getattr(lead, "sic_codes", "") or ""
            addr = getattr(lead, "location", "") or ""
            lead.ideal_customer_profile_match = self._compute_icp_score(sic, addr)
        
        # Additional ICP boosts
        officers = getattr(lead, "officers", [])
        if any(o.get('role', '').lower() in ['director', 'ceo', 'managing director'] for o in officers):
            # Boost if a senior officer is present, indicating high data value
            lead.ideal_customer_profile_match = min(1.0, lead.ideal_customer_profile_match + 0.20)
        
        # Pain points based on SIC (simple heuristic)
        sic_codes = lead.sic_codes.split(', ') if lead.sic_codes else []
        if "73110" in sic_codes: # Advertising agencies
            lead.pain_points = ["Lead generation efficiency", "Attribution/ROI visibility", "Scaling paid media profitably"]
        elif "62012" in sic_codes: # Business & domestic software development
            lead.pain_points = ["Technical debt management", "Scalability issues", "Talent acquisition challenges"]
        
        # Filter: If SIC codes are set in config, only keep leads that match
        if self.config.sic_codes:
            if not self._sic_matches_target(lead.sic_codes or ""):
                logger.debug(f"Final filter: Removing {lead.company_name} due to non-target SIC: {lead.sic_codes}")
                # Return a special Lead indicating it should be filtered out
                lead.data_quality_score = -1 # Set a very low score for easy filtering in the main loop
                return lead
        
        # Calculate final quality score
        lead.calculate_quality_score()
        
        return lead

    # -----------------------------------------------------------------------
    # Orchestration
    # -----------------------------------------------------------------------
    async def _capture_missing_screenshots_async(self, leads: List[Lead]):
        """Runs screenshot capture concurrently for leads missing key data."""
        if not getattr(self.config, "screenshot_missing_data", False):
            return
            
        logger.info("Starting optional screenshot capture for missing data...")
        
        # Filter to those who have real websites and missing key data
        to_shoot = []
        for l in leads:
            if not l.website:
                continue
            # Skip Companies House and common social media links, as these are not the company's main website.
            if any(h in urlparse(l.website).netloc for h in ["find-and-update.company-information.service.gov.uk", "companieshouse.gov.uk", "linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com"]):
                continue
            # Only screenshot if we are missing essential contact info
            if not getattr(l, "email", None) or not getattr(l, "phone_number", None):
                to_shoot.append(l)

        if not to_shoot:
            logger.info("No leads require screenshots.")
            return

        async with async_playwright() as p:
            # Launch browser once for all screenshots
            browser = await p.chromium.launch(headless=True)
            tasks = []
            
            for l in to_shoot:
                fn = self._safe_filename(l.company_name) + ".png"
                out = os.path.join("screenshots", fn)
                tasks.append(self._screenshot_url(browser, l.website, out))
                
            # Run all screenshots concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Map results back to leads
            for l, path in zip(to_shoot, results):
                if isinstance(path, str) and os.path.exists(path):
                    l.screenshot_path = path
                    logger.debug(f"Screenshot saved for {l.company_name} at {path}")
                    
            await browser.close()
            logger.info(f"Captured {len([r for r in results if isinstance(r, str)])} screenshots.")


    def _safe_filename(self, name: str) -> str:
        """Converts a company name into a filesystem-safe filename."""
        # Replace non-alphanumeric/non-dot/non-hyphen with underscore
        n = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip())
        # Trim to a reasonable length
        n = n[:120] 
        # Fallback if name is empty after cleaning
        return n or f"screenshot_{int(datetime.utcnow().timestamp())}"


    async def run_scraper(self) -> List[Lead]:
        logger.info("=" * 60)
        logger.info("🚀 Starting PROOF BOT Elite Web Scraper")
        logger.info("=" * 60)

        all_leads: List[Lead] = []
        
        # Use a single playwright instance for async scraping phases
        async with async_playwright() as p:
            # Configure browser launch (e.g., headless, proxy options could go here)
            browser = await p.chromium.launch(headless=True)
            # Context for non-CH sources (like Clutch)
            context = await browser.new_context(user_agent=utils.choose_user_agent(), locale="en-GB")
            sources = self.selectors.get("sources", {})

            # --- Run Companies House scraper (targeted/exhaustive) ---
            if "companies_house" in sources:
                logger.info("🏭 Scraping Companies House...")
                ch_leads = await self.scrape_companies_house(browser) # Pass browser, not context
                all_leads.extend(ch_leads)
                logger.info(f"✓ Companies House produced {len(ch_leads)} leads.")

            # --- Run other simple scrapers concurrently ---
            tasks = []
            if "clutch" in sources:
                # Need a new page/context setup for concurrent execution if necessary, 
                # but Clutch is simpler, using the general context is fine.
                page = await context.new_page()
                await self._inject_stealth(page)
                tasks.append(self.scrape_clutch(page))
                logger.info("📄 Prepared Clutch scrape task.")

            if tasks:
                logger.info(f"Running {len(tasks)} directory scrapers concurrently...")
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"A directory scrape task failed: {r}")
                    elif r:
                        all_leads.extend(r)

            await context.close()
            await browser.close()
            logger.info("✓ Browser closed.")

        # --- Deduplication (by company name) ---
        if not all_leads:
            logger.warning("No leads gathered from any sources.")
            return []

        logger.info(f"📊 Total raw leads from all sources: {len(all_leads)}")
        seen, unique_leads = set(), []
        for lead in all_leads:
            key = (lead.company_name or "").strip().lower()
            if key and key not in seen:
                unique_leads.append(lead)
                seen.add(key)
        logger.info(f"🧬 Found {len(unique_leads)} unique leads after deduplication.")

        # --- Enrichment (now fully asynchronous) ---
        logger.info(f"✨ Enriching {len(unique_leads)} leads with async tasks.")
        enriched_leads = []
        
        enrich_tasks = [self.enrich_lead_data(lead) for lead in unique_leads]
        results = await asyncio.gather(*enrich_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"An enrichment task failed: {result}", exc_info=True)
            # Filter out leads that failed the final SIC check in enrichment
            elif result and getattr(result, "data_quality_score", 0) >= 0:
                enriched_leads.append(result)

        logger.info(f"✅ Enrichment completed. {len(enriched_leads)} leads remain after final filtering.")

        # --- Optional screenshots for missing data (Async again) ---
        await self._capture_missing_screenshots_async(enriched_leads)

        # --- Final Sort and Output ---
        enriched_leads.sort(key=lambda x: x.data_quality_score, reverse=True)
        
        if enriched_leads:
            logger.info("🏆 Top 5 leads by quality score:")
            for l in enriched_leads[:5]:
                # Assuming Lead has a calculate_quality_score method run in enrichment
                score = getattr(l, 'data_quality_score', 'N/A')
                logger.info(f"  - {l.company_name} (Source: {l.source}, Score: {score}/100, ICP: {l.ideal_customer_profile_match})")

        logger.info(f"🏁 Scraper run finished. Returning {len(enriched_leads)} processed leads.")
        logger.info("=" * 60)
        return enriched_leads