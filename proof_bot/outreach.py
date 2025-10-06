# proof_bot/outreach.py
import logging
import re
import random
from typing import Dict, List, Optional, Any

# The relative import below is correct when running as part of the 'proof_bot' package.
# However, it will show an error in VS Code when run as a standalone file.
# It has been commented out, and a placeholder class is used for demonstration.
# from .models import Lead

class Lead:
    """Placeholder for the actual Lead model for standalone script execution."""
    def __init__(self, company_name: str, ceo_name: Optional[str] = None, location: Optional[str] = None):
        self.company_name = company_name
        self.ceo_name = ceo_name
        self.location = location if location else ""

logger = logging.getLogger('ProofBot.Outreach')

class PsychologyOutreachSystem:
    """
    Advanced outreach generator using proven sales psychology to create
    highly personalized and effective communication for leads.
    """

    def __init__(self):
        """Initializes the outreach system and loads all frameworks."""
        self.load_psychology_frameworks()
        logger.info("Psychology Outreach System initialized.")

    def load_psychology_frameworks(self):
        """Loads all psychological frameworks, industry data, and templates."""

        # Industry-specific pain points, value props, and case studies
        self.industry_patterns = {
            'automotive': {
                'keywords': ['cars', 'automotive', 'motors', 'garage', 'vehicle', 'auto'],
                'pain_points': [
                    "keeping your service bays filled during quiet periods",
                    "managing customer bookings efficiently",
                    "competing with larger dealership groups",
                    "attracting younger customers who research online first"
                ],
                'value_props': [
                    "We help independent garages fill 23% more appointments through local digital presence",
                    "Our automotive clients see an average 34% increase in service bookings",
                    "We've helped 12 independent garages compete effectively against dealerships"
                ],
                'case_studies': [
                    "A local garage in Manchester increased their MOT bookings by 156% in 90 days",
                    "An independent dealer we work with now gets 40+ qualified enquiries per month"
                ]
            },
            'food_beverage': {
                'keywords': ['kitchen', 'bakery', 'bar', 'cafe', 'restaurant', 'food', 'catering'],
                'pain_points': [
                    "filling tables during off-peak hours",
                    "standing out in a crowded local market",
                    "managing online reviews and reputation",
                    "attracting customers beyond your immediate area"
                ],
                'value_props': [
                    "We've helped local restaurants increase covers by 28% during quiet periods",
                    "Our F&B clients average 4.6-star ratings with 3x more reviews",
                    "We specialize in hyper-local marketing that fills seats"
                ],
                'case_studies': [
                    "A London bar we work with increased Tuesday-Thursday covers by 67%",
                    "One bakery client went from 12 to 87 Google reviews in 4 months"
                ]
            },
            'beauty_personal': {
                'keywords': ['beauty', 'beauties', 'hair', 'salon', 'spa', 'cuts', 'barber'],
                'pain_points': [
                    "reducing no-shows and last-minute cancellations",
                    "keeping your appointment book full",
                    "attracting higher-value clients",
                    "standing out from competition on your high street"
                ],
                'value_props': [
                    "Our beauty clients reduce no-shows by 67% with our booking system",
                    "We help salons increase average transaction value by £32",
                    "We've filled 890+ appointment slots for beauty businesses this year"
                ],
                'case_studies': [
                    "A Birmingham salon increased their average ticket from £45 to £78",
                    "One barber shop reduced no-shows from 18% to just 3%"
                ]
            },
            'childcare': {
                'keywords': ['childcare', 'nursery', 'nurseries', 'kids', 'children', 'baby'],
                'pain_points': [
                    "maintaining full enrollment throughout the year",
                    "communicating your unique approach to anxious parents",
                    "standing out from larger nursery chains",
                    "building trust with parents who are researching online"
                ],
                'value_props': [
                    "We help independent nurseries maintain 95%+ occupancy year-round",
                    "Our childcare clients see 3x more qualified parent enquiries",
                    "We specialize in building trust that converts concerned parents into enrollments"
                ],
                'case_studies': [
                    "A local nursery went from 78% to 98% occupancy in 5 months",
                    "One childcare provider now has a 6-month waiting list"
                ]
            },
            'retail': {
                'keywords': ['shop', 'store', 'retail', 'mart', 'minimart', 'market'],
                'pain_points': [
                    "competing with online retailers and supermarkets",
                    "driving foot traffic during quiet periods",
                    "building customer loyalty in your local area",
                    "showcasing your unique products to the right audience"
                ],
                'value_props': [
                    "We've helped local retailers increase foot traffic by 41%",
                    "Our retail clients see average basket sizes increase by £18",
                    "We specialize in hyper-local campaigns that drive customers to your door"
                ],
                'case_studies': [
                    "A local shop increased daily footfall from 34 to 89 customers",
                    "One retailer's repeat customer rate jumped from 22% to 61%"
                ]
            },
            'property': {
                'keywords': ['property', 'properties', 'estate', 'developments', 'housing'],
                'pain_points': [
                    "generating qualified buyer/tenant leads consistently",
                    "standing out in a saturated property market",
                    "reducing time properties stay on your books",
                    "competing with larger estate agency chains"
                ],
                'value_props': [
                    "We help independent agents generate 3x more qualified property leads",
                    "Our property clients reduce average time to let/sell by 23 days",
                    "We've helped 17 independent agents compete successfully against major chains"
                ],
                'case_studies': [
                    "An independent agent went from 4 to 23 qualified leads per month",
                    "One property company reduced their average time to let from 67 to 31 days"
                ]
            },
            'professional_services': {
                'keywords': ['accountant', 'accounting', 'consulting', 'advisor', 'advisors', 'legal', 'solicitor'],
                'pain_points': [
                    "attracting higher-value clients consistently",
                    "differentiating from other local firms",
                    "generating referrals beyond your existing network",
                    "establishing expertise and trust online"
                ],
                'value_props': [
                    "We help professional firms attract 40% more qualified leads",
                    "Our clients see average engagement value increase by £4,200",
                    "We specialize in positioning that attracts premium clients"
                ],
                'case_studies': [
                    "An accounting firm increased average client value from £2,400 to £7,100",
                    "One consulting practice generated £340k in new business in 6 months"
                ]
            },
            'tech_digital': {
                'keywords': ['software', 'digital', 'tech', 'coding', 'media', 'design', 'web'],
                'pain_points': [
                    "finding clients who understand the value you provide",
                    "standing out in a crowded digital services market",
                    "generating consistent project pipeline",
                    "commanding premium rates for your expertise"
                ],
                'value_props': [
                    "We help digital agencies generate 52% more qualified project leads",
                    "Our tech clients increase average project value by £12,000",
                    "We specialize in positioning that attracts clients who value quality"
                ],
                'case_studies': [
                    "A digital agency went from £8k to £31k average project value",
                    "One software company generated 34 qualified demos in 90 days"
                ]
            },
            'construction_trades': {
                'keywords': ['building', 'construction', 'doors', 'windows', 'roofing', 'plumbing', 'developments'],
                'pain_points': [
                    "keeping your project pipeline full year-round",
                    "attracting higher-value residential or commercial projects",
                    "reducing reliance on word-of-mouth alone",
                    "standing out from cheaper competition"
                ],
                'value_props': [
                    "We help trade businesses maintain 92% capacity utilization",
                    "Our construction clients see average project value increase by £7,800",
                    "We've generated £2.4M in project pipeline for trades this year"
                ],
                'case_studies': [
                    "A local builder went from 3 to 11 projects in their pipeline",
                    "One tradesman increased average job value from £3,200 to £9,400"
                ]
            },
            'investment_finance': {
                'keywords': ['investment', 'investments', 'capital', 'finance', 'funding'],
                'pain_points': [
                    "attracting qualified high-net-worth clients",
                    "building trust in a skeptical market",
                    "differentiating from larger institutions",
                    "demonstrating expertise and track record"
                ],
                'value_props': [
                    "We help investment firms attract 3x more qualified HNW leads",
                    "Our clients see average client AUM increase by 47%",
                    "We specialize in trust-building that converts cautious investors"
                ],
                'case_studies': [
                    "One investment firm attracted £12M in new AUM in 8 months",
                    "A financial advisor doubled their qualified consultation requests"
                ]
            },
            'general_business': {
                'keywords': ['limited', 'ltd', 'associates', 'group', 'global', 'alliance', 'cic'],
                'pain_points': [
                    "generating consistent quality leads for your business",
                    "standing out in your local market",
                    "maximizing the return on your marketing investment",
                    "building a predictable growth engine"
                ],
                'value_props': [
                    "We help UK small businesses increase qualified leads by 47%",
                    "Our clients see an average ROI of 340% within 6 months",
                    "We've helped 127 UK businesses build predictable growth systems"
                ],
                'case_studies': [
                    "A local business went from 5 to 28 qualified leads per month",
                    "One SME increased revenue by £180k in their first year with us"
                ]
            }
        }

        # UK location-specific hooks and context
        self.location_hooks = {
            'london': {
                'hook': "In a market as competitive as London",
                'context': "London businesses",
                'challenge': "standing out in the capital's crowded marketplace"
            },
            'birmingham': {
                'hook': "In Birmingham's growing business ecosystem",
                'context': "Birmingham businesses",
                'challenge': "capturing market share in the Midlands' largest city"
            },
            'manchester': {
                'hook': "In Manchester's competitive market",
                'context': "Manchester businesses",
                'challenge': "thriving in the Northwest's business hub"
            },
            'scotland': {
                'hook': "In the Scottish market",
                'context': "Scottish businesses",
                'challenge': "growing your presence across Scotland"
            },
            'wales': {
                'hook': "In the Welsh business community",
                'context': "Welsh businesses",
                'challenge': "expanding in the Welsh market"
            },
            'liverpool': {
                'hook': "In Liverpool's vibrant business scene",
                'context': "Liverpool businesses",
                'challenge': "standing out in Merseyside"
            },
            'leeds': {
                'hook': "In Leeds' competitive environment",
                'context': "Leeds businesses",
                'challenge': "growing in Yorkshire's business capital"
            },
            'default': {
                'hook': "In your local market",
                'context': "local businesses",
                'challenge': "standing out from your competition"
            }
        }

        # SPIN Framework questions
        self.spin_framework = {
            'situation': [
                "How are you currently attracting new customers to {company_name}?",
                "What's been your main source of new business over the past 6 months?",
                "How much time do you spend on business development each week?",
                "Who's responsible for bringing in new clients at {company_name}?"
            ],
            'problem': [
                "Are you finding it challenging to generate consistent leads?",
                "Is it getting harder to stand out from local competition?",
                "Do you worry about feast-or-famine in your pipeline?",
                "Are you relying too heavily on word-of-mouth alone?"
            ],
            'implication': [
                "What happens to {company_name} if your main lead source dries up?",
                "How much revenue are you leaving on the table with an inconsistent pipeline?",
                "What's it costing you to turn away work because you're too busy, then go quiet?",
                "How is unpredictable revenue affecting your ability to plan and invest?"
            ],
            'need_payoff': [
                "How valuable would it be to have a predictable flow of qualified leads?",
                "What would it mean for {company_name} to reduce customer acquisition costs by 40%?",
                "How would your business transform with a full pipeline year-round?",
                "What could you achieve if marketing became your competitive advantage?"
            ]
        }
        
        # Influence principles (Cialdini)
        self.influence_principles = {
            'social_proof': [
                "We work with {number} other {industry} businesses in your area",
                "{competitor_type} businesses are increasingly using this approach",
                "This is becoming the standard for successful {industry} companies"
            ],
            'scarcity': [
                "We only take on {number} new clients per quarter to maintain quality",
                "We're booking consultations for {month} right now",
                "This approach works best when implemented before your busy season"
            ],
            'authority': [
                "We've generated over £{amount} in revenue for {industry} businesses",
                "Our team has {number} years of experience in {industry} marketing",
                "We've helped {number} businesses in your sector achieve {result}"
            ],
            'reciprocity': [
                "I'd be happy to share our {industry} benchmark report with you",
                "I can send you a quick analysis of your current online presence",
                "Would a 15-minute marketing diagnostic be valuable?"
            ]
        }

        # Objection handling frameworks
        self.objection_responses = {
            'too_busy': {
                'empathy': "I completely understand - that's exactly why this might be perfect timing.",
                'reframe': "The businesses I work with were too busy to think about marketing... until their pipeline dried up.",
                'close': "Can we schedule 15 minutes next week when you're less rushed?",
                'alternative': "What if I sent you a 2-minute video explaining how this works, and you can watch it when convenient?"
            },
            'happy_with_current': {
                'empathy': "That's great to hear - it sounds like things are working well right now.",
                'reframe': "Can I ask - if what you're doing now suddenly stopped working tomorrow, what would be your backup plan?",
                'insight': "That's the exact situation we help prevent. Even successful businesses benefit from diversifying their lead sources.",
                'close': "Would it be worth a quick conversation just to explore what else might be possible?"
            },
            'too_expensive': {
                'empathy': "I appreciate the concern about cost - that's always an important consideration.",
                'reframe': "Here's what I've found: most {industry} businesses waste money on marketing that doesn't work. Our approach is about spending smarter, not more.",
                'value': "Our clients typically see ROI within 90 days, and the cost is less than hiring one part-time person.",
                'close': "Would you be open to a quick diagnostic to see where you might be leaving money on the table?"
            },
            'send_information': {
                'empathy': "I could absolutely send information.",
                'reframe': "But honestly, 90% of it won't apply to {company_name} specifically.",
                'alternative': "What if we spent 15 minutes, I learn about your specific situation, and then I send you something actually relevant?",
                'close': "Would Tuesday or Wednesday work better?"
            }
        }

    def _get_random_element(self, data_list: List[Any]) -> Optional[Any]:
        """Safely gets a random element from a list."""
        return random.choice(data_list) if data_list else None

    def _identify_industry(self, company_name: str) -> str:
        """
        Identifies the industry of a company based on keywords in its name.
        Defaults to 'general_business' if no specific industry is found.
        """
        company_name_lower = company_name.lower()
        for industry, data in self.industry_patterns.items():
            if any(re.search(r'\b' + keyword + r'\b', company_name_lower) for keyword in data['keywords']):
                return industry
        return 'general_business'

    def _identify_location_data(self, location_str: str) -> Dict[str, str]:
        """Identifies location-specific data based on a location string."""
        location_lower = location_str.lower()
        for city, data in self.location_hooks.items():
            if city in location_lower:
                return data
        return self.location_hooks['default']

    def _get_ceo_first_name(self, lead: Lead) -> str:
        """Extracts the first name from the CEO's full name."""
        if not lead.ceo_name:
            return ""
        return lead.ceo_name.split()[0].strip()

    def _format_template(self, template: str, context: Dict[str, Any]) -> str:
        """Formats a string template using a context dictionary."""
        for key, value in context.items():
            template = template.replace(f"{{{key}}}", str(value))
        return template

    def generate_email(self, lead: Lead) -> Dict[str, str]:
        """
        Generates a complete, personalized outreach email for a given lead.

        Args:
            lead: A Lead object containing company information.

        Returns:
            A dictionary containing the components of the email:
            'subject', 'greeting', 'body', and 'cta'.
        """
        industry_key = self._identify_industry(lead.company_name)
        industry_data = self.industry_patterns[industry_key]
        location_data = self._identify_location_data(lead.location)
        ceo_first_name = self._get_ceo_first_name(lead)

        context = {
            "company_name": lead.company_name,
            "ceo_first_name": ceo_first_name,
            "industry": industry_key.replace('_', ' '),
            "location_hook": location_data['hook'],
            "challenge": location_data['challenge'],
            "pain_point": self._get_random_element(industry_data['pain_points']),
            "value_prop": self._get_random_element(industry_data['value_props']),
            "case_study": self._get_random_element(industry_data['case_studies']),
        }

        # Select a random Need-Payoff question for the CTA
        cta_question = self._get_random_element(self.spin_framework['need_payoff'])

        # Build the email components
        subject = f"Idea for {context['company_name']}"
        greeting = f"Hi {context['ceo_first_name']}," if context['ceo_first_name'] else f"Hi {context['company_name']} team,"

        body = (
            f"Noticed {context['company_name']} and wanted to reach out. {context['location_hook']}, "
            f"I imagine that {context['challenge']} is a constant focus.\n\n"
            f"We often speak with {context['industry']} businesses facing challenges with {context['pain_point']}. "
            f"This is an area we specialize in. For context, {context['value_prop']}.\n\n"
            f"To give you a real-world example, {context['case_study']}."
        )

        cta = self._format_template(cta_question, context)

        return {
            "subject": subject,
            "greeting": greeting,
            "body": body,
            "cta": cta
        }

if __name__ == '__main__':
    # This block demonstrates how to use the PsychologyOutreachSystem class.
    # It will only run when the script is executed directly.

    # Configure basic logging for demonstration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # 1. Initialize the system
    outreach_system = PsychologyOutreachSystem()

    # 2. Create some sample leads to test different scenarios
    leads_to_test = [
        Lead(company_name="Manchester Auto Services Ltd", ceo_name="John Smith", location="Manchester, UK"),
        Lead(company_name="The London Bakery", ceo_name="Jane Doe", location="Central London"),
        Lead(company_name="Global Tech Solutions", ceo_name=None, location="Reading"),
    ]

    # 3. Generate and print an outreach email for each lead
    for i, lead in enumerate(leads_to_test):
        print("-" * 60)
        print(f"Generating outreach for Lead {i+1}: {lead.company_name}")
        
        email_content = outreach_system.generate_email(lead)
        
        print(f"\nSubject: {email_content['subject']}\n")
        print(email_content['greeting'])
        print(email_content['body'])
        print(f"\n{email_content['cta']}")
        print("-" * 60 + "\n")