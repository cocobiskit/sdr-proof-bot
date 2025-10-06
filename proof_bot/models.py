# proof_bot/models.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any

@dataclass
class Lead:
    """Enhanced lead data model"""
    # Basic info
    company_name: str
    website: str
    location: str
    source: str
    scraped_at: datetime

    # Contact info
    phone_number: Optional[str] = None
    email: Optional[str] = None

    # Social media
    linkedin: Optional[str] = None
    twitter: Optional[str] = None
    facebook: Optional[str] = None
    instagram: Optional[str] = None

    # Decision makers
    ceo_name: Optional[str] = None
    ceo_linkedin: Optional[str] = None
    sales_director: Optional[str] = None
    marketing_director: Optional[str] = None

    # Company details
    employee_count: Optional[int] = None
    annual_revenue: Optional[str] = None
    founded_year: Optional[int] = None
    industry_awards: List[str] = field(default_factory=list)
    key_clients: List[str] = field(default_factory=list)
    technologies_used: List[str] = field(default_factory=list)

    # NEW: Additional company details
    status: Optional[str] = None
    company_type: Optional[str] = None
    incorporation_date: Optional[str] = None
    sic_codes: Optional[str] = None
    accounts_next: Optional[str] = None
    accounts_last: Optional[str] = None
    confirmation_next: Optional[str] = None
    confirmation_last: Optional[str] = None

    # Officers from Companies House
    officers: List[Dict[str, Any]] = field(default_factory=list)

    # Verification
    phone_verified: bool = False
    email_verified: bool = False
    robots_txt_status: str = "Unchecked"
    screenshot_path: Optional[str] = None
    data_quality_score: float = 0.0

    # Intelligence
    trigger_events: List[str] = field(default_factory=list)
    pain_points: List[str] = field(default_factory=list)
    ideal_customer_profile_match: float = 0.0
    estimated_deal_size: Optional[str] = None

    def __post_init__(self):
        """Calculate quality score after initialization."""
        self.calculate_quality_score()

    def calculate_quality_score(self):
        """Calculate lead quality score based on data completeness"""
        score = 0
        # You may want to add 'officers' to the quality score later
        weights = {
            'phone_number': 20, 'email': 20, 'linkedin': 15,
            'ceo_name': 15, 'employee_count': 10, 'phone_verified': 10,
            'email_verified': 10
        }
        for field, weight in weights.items():
            if getattr(self, field):
                score += weight
        self.data_quality_score = score