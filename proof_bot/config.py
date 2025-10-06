# proof_bot/config.py
import os
import json
import random
from dataclasses import dataclass, field
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# Load environment variables from .env file at the project root
load_dotenv()

@dataclass
class BotConfig:
    """Central configuration for the bot, loaded from environment variables."""
    # Target settings
    target_location: str = "London"
    target_industry: str = "Digital Marketing Agencies"
    target_count: int = 100
    github_repo: str = "cocobiskit/sdr-proof-bot"  # IMPORTANT: Change this!

    # NEW: JSON config file for expanded locations and SIC codes
    config_json_path: str = "expanded_locations_and_sics.json"  # Path to the JSON file

    # Loaded from JSON (populated in __post_init__)
    locations: List[Dict[str, Any]] = field(default_factory=list)
    business_types: List[Dict[str, Any]] = field(default_factory=list)

    # API Keys (loaded from .env file)
    github_token: str = os.getenv('GITHUB_TOKEN', '')
    telegram_token: str = os.getenv('TELEGRAM_TOKEN', '')
    telegram_chat_id: str = os.getenv('TELEGRAM_CHAT_ID', '')
    linkedin_username: str = os.getenv('LINKEDIN_USERNAME', '')
    linkedin_password: str = os.getenv('LINKEDIN_PASSWORD', '')
    twitter_api_key: str = os.getenv('TWITTER_API_KEY', '')
    twilio_account_sid: str = os.getenv('TWILIO_SID', '')
    twilio_auth_token: str = os.getenv('TWILIO_TOKEN', '')
    openai_api_key: str = os.getenv('OPENAI_API_KEY', '')

    # Scraping settings
    respect_robots: bool = True
    screenshot_missing_data: bool = True
    verify_all_data: bool = True
    max_workers: int = 5
    request_delay: float = 2.0  # Increased default for politeness
    selectors_file: str = "selectors.json"

    # Outreach settings
    use_psychology: bool = True
    personalize_outreach: bool = True
    multi_channel: bool = True

    # NEW: Exhaustive mode settings
    exhaustive_mode: bool = False
    # ✅ Use default_factory for mutable types like lists
    sic_codes: List[str] = field(default_factory=list)

    # NEW: Proxy settings
    use_proxies: bool = False
    # ✅ Also fix the proxy_pool while we're here
    proxy_pool: List[str] = field(default_factory=list)  # List of proxy strings

    # NEW: Smart selection settings (for dynamic picking from JSON)
    random_location: bool = True
    random_industry: bool = True  # e.g., pick random SICs for variety
    selected_location_index: Optional[int] = None  # Override for specific location
    selected_industry_index: Optional[int] = None  # Override for specific industry

def __post_init__(self):
    """Sets default lists... Also loads JSON and handles smart selections."""
    # ✅ Initialize default SIC codes if empty
    if not self.sic_codes:
        self.sic_codes = [
            "73110",  # Advertising agencies
            "62012",  # Business and domestic software development
            "62090",  # Other information technology service activities
            "63110",  # Data processing, hosting and related activities
            "63120",  # Web portals
            "70229"   # Management consultancy activities other than financial management
        ]
    # The proxy_pool is fine as an empty list, so no changes needed for it here.

    # Load the expanded JSON file if it exists
    if os.path.exists(self.config_json_path):
        with open(self.config_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.locations = data.get('locations', [])
            self.business_types = data.get('business_types', [])
        
        print(f"Loaded {len(self.locations)} locations, {len(self.business_types)} business types.")  # Log findings
        
        # NEW: Dynamic SIC cycling from business_types (varied for exhaustive/random_industry)
        if self.exhaustive_mode or self.random_industry:
            if self.business_types:
                selected_types = random.sample(self.business_types, min(3, len(self.business_types)))  # Pick 3 random
                self.sic_codes = []
                for bt in selected_types:
                    self.sic_codes.extend(bt['key_sic_codes'])
                self.sic_codes = list(set(self.sic_codes))  # Dedup
                self.target_industry = ', '.join([bt['example_industry'] for bt in selected_types])
                print(f"Selected varied SICs: {self.sic_codes} for industries: {self.target_industry}")  # Log
            else:
                # Fallback: Cycle defaults
                self.sic_codes = random.sample(self.sic_codes, min(4, len(self.sic_codes)))
                print(f"Fallback: Cycled to {self.sic_codes}")  # Log

        # NEW: Load improved SIC selector if audited (logs if found)
        rec_sic_file = "recommended_sic_selectors.json"
        if os.path.exists(rec_sic_file):
            with open(rec_sic_file, 'r') as f:
                rec = json.load(f)
                if 'nature_of_business_sic' in rec and rec['nature_of_business_sic']['value']:
                    # Update selectors.json implicitly (or reload in scraper)
                    print(f"Loaded audited SIC selector: {rec['nature_of_business_sic']['value'][0]} (from Colossus)")
                else:
                    print("Audited file exists but no SIC recs found.")

        # Rest: Random location
        if self.exhaustive_mode or self.random_location:
            self._select_random_location()
    else:
        print(f"Warning: {self.config_json_path} not found. Using defaults.")

    def _select_random_location(self):
        """Pick a random location from JSON and update target_location."""
        if self.locations:
            loc = random.choice(self.locations)
            self.target_location = loc['city_region']
            print(f"Selected random location: {self.target_location}")

    def _select_random_industry(self):
        """Pick random SIC codes from JSON and update sic_codes and target_industry."""
        if self.business_types:
            industry = random.choice(self.business_types)
            self.sic_codes = industry['key_sic_codes']
            self.target_industry = industry['example_industry']
            print(f"Selected random industry: {self.target_industry} (SIC: {self.sic_codes})")

    def get_all_locations(self) -> List[str]:
        """Helper: Get list of all city_regions from JSON."""
        return [loc['city_region'] for loc in self.locations]

    def get_all_sic_codes(self) -> List[str]:
        """Helper: Flatten all unique SIC codes from JSON."""
        sics = set()
        for bt in self.business_types:
            sics.update(bt['key_sic_codes'])
        return list(sics)

    def cycle_locations(self, start_index: int = 0) -> str:
        """For exhaustive mode: Cycle through locations starting from index."""
        if self.locations:
            idx = (start_index + random.randint(0, len(self.locations) - 1)) % len(self.locations)
            loc = self.locations[idx]
            self.target_location = loc['city_region']
            return self.target_location
        return self.target_location

    def cycle_sic_codes(self, start_index: int = 0) -> List[str]:
        """For exhaustive mode: Cycle through industries/SICs starting from index."""
        if self.business_types:
            idx = (start_index + random.randint(0, len(self.business_types) - 1)) % len(self.business_types)
            industry = self.business_types[idx]
            self.sic_codes = industry['key_sic_codes']
            self.target_industry = industry['example_industry']
            return self.sic_codes
        return self.sic_codes