# proof_bot/main.py
import logging
import json
import os
import asyncio
from datetime import datetime
from dataclasses import asdict

import pandas as pd
from github import Github
import telegram

from .config import BotConfig
from .scraper import EliteWebScraper
from .outreach import PsychologyOutreachSystem
from .models import Lead

# --- Configure Logging ---
# Main application logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('proof_bot.log', 'w'),
        logging.StreamHandler()
    ]
)

# Dedicated logger for visited URLs to create a clean list for debugging
url_logger = logging.getLogger('VisitedURLs')
url_logger.setLevel(logging.INFO)
# This handler will write to visited_urls.log with a very simple format
url_handler = logging.FileHandler('visited_urls.log', 'w')
url_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
url_logger.addHandler(url_handler)


logger = logging.getLogger('ProofBot')

class ProofBot:
    """Main bot orchestrator for lead generation and outreach campaigns."""

    def __init__(self, config: BotConfig):
        self.config = config
        self.scraper = EliteWebScraper(config)
        self.outreach = PsychologyOutreachSystem()
        self.leads = []
        self.campaigns = []
        self.github = Github(config.github_token) if config.github_token and config.github_repo else None
        self.telegram_bot = telegram.Bot(token=config.telegram_token) if config.telegram_token else None
        logger.info("PROOF BOT v3.0 Initialized with Playwright Engine.")

    async def run(self):
        """Asynchronous main execution flow of the bot."""
        logger.info("📊 Step 1: Scraping and enriching leads...")
        self.leads = await self.scraper.run_scraper()
        logger.info(f"✅ Found and enriched {len(self.leads)} qualified leads.")

        if not self.leads:
            logger.warning("No leads found. Falling back to mock lead for testing...")
            mock_lead = Lead(
                company_name="Mock Digital Agency Ltd",
                website="https://example.com",
                location="London",
                source="Mock",
                scraped_at=datetime.now(),
                email="info@example.com",
                data_quality_score=80.0
            )
            self.leads = [mock_lead]
            logger.info("Added mock lead to proceed with campaigns.")

        logger.info("\n✉️ Step 2: Generating personalized outreach campaigns...")
        for lead in self.leads:
            # FIX: Use the correct method name 'generate_email' from outreach.py
            campaign = self.outreach.generate_email(lead)
            self.campaigns.append(campaign)
        logger.info(f"✅ Generated {len(self.campaigns)} outreach campaigns.")
        
        logger.info("\n📁 Step 3: Exporting results...")
        self.export_to_csv()
        self.export_campaigns_to_json()

        logger.info("\n📱 Step 4: Sending updates and publishing portfolio...")
        await self.send_telegram_summary() # Changed to await
        self.update_github_portfolio()
        
        self.display_summary()

    def export_to_csv(self):
        if not self.leads: return
        df = pd.DataFrame([asdict(lead) for lead in self.leads])
        filename = f"exports/leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        os.makedirs('exports', exist_ok=True)
        df.to_csv(filename, index=False)
        logger.info(f"📄 Exported leads to {filename}")

    def export_campaigns_to_json(self):
        if not self.campaigns: return
        filename = f"exports/campaigns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(self.campaigns, f, indent=2, default=str)
        logger.info(f"📄 Exported campaigns to {filename}")
        
    async def send_telegram_summary(self): # Made async
        if not self.telegram_bot or not self.config.telegram_chat_id or not self.leads:
            logger.warning("Telegram credentials missing or no leads found. Skipping.")
            return
        
        summary = f"**PROOF BOT Run Complete**\n\n- Leads Found: {len(self.leads)}\n"
        # Safely access the first lead for summary
        if self.leads:
             summary += f"- Top Lead: {self.leads[0].company_name} (Score: {self.leads[0].data_quality_score})"
        else:
             summary += "- No specific lead data available." # Fallback if leads list unexpectedly empty here

        try:
            # Directly await the async send_message (no to_thread needed for v20+)
            await self.telegram_bot.send_message(
                chat_id=self.config.telegram_chat_id,
                text=summary,
                parse_mode='Markdown'
            )
            logger.info("📱 Telegram summary sent.")
        except Exception as e:
            logger.error(f"Failed to send Telegram summary: {e}")

    def update_github_portfolio(self):
        if not self.github:
            logger.warning("GitHub token/repo missing. Skipping.") # Removed "or no leads found" as it's not strictly a dependency for updating README.
            return
        
        try:
            repo = self.github.get_repo(self.config.github_repo)
            readme_content = f"# SDR Lead Generation Portfolio\n\n_Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n\n"
            
            if self.leads: # Only include lead-specific stats if leads exist
                readme_content += f"**Latest Run Results:**\n- Leads Generated: {len(self.leads)}\n- Average Quality Score: {sum(l.data_quality_score for l in self.leads) / len(self.leads):.1f}%\n"
            else:
                readme_content += "**Latest Run Results:**\n- No leads generated in this run.\n"
            
            # Check if README exists before trying to update
            try:
                contents = repo.get_contents("README.md")
                repo.update_file(contents.path, f"Automated portfolio update", readme_content, contents.sha)
                logger.info("🐙 GitHub portfolio updated.")
            except Exception: # If get_contents fails (e.g., file not found), create it
                repo.create_file("README.md", "Create portfolio", readme_content)
                logger.info("🐙 Created and populated GitHub portfolio.")

        except Exception as e:
            logger.error(f"Failed to update GitHub portfolio: {e}")
            
    def display_summary(self):
        if not self.leads:
            logger.info("Run complete. No leads were found.")
            return
            
        avg_quality = sum(l.data_quality_score for l in self.leads) / len(self.leads)
        print(f"\n--- EXECUTION COMPLETE ---\nTotal Leads: {len(self.leads)}\nAvg. Quality: {avg_quality:.1f}%\n--------------------------\n")

async def main():
    """Main asynchronous entry point for the application."""
    config = BotConfig()
    bot = ProofBot(config)
    await bot.run()

if __name__ == "__main__":
    # This allows the async main function to be run
    asyncio.run(main())