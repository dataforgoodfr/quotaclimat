"""
Mock Factiva Streaming Client for local testing.

This module simulates the Factiva Streams API behavior for development and testing purposes.
It generates sample events that mimic the real Factiva Streams API events.
"""

import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


class MockFactivaClient:
    """
    Mock client that simulates the Factiva Streaming API.

    This class generates fake events for testing the ingestion pipeline locally.
    """

    def __init__(
        self, subscription_id: str = "mock-subscription", max_events: int = 100
    ):
        """
        Initialize the mock client.

        Args:
            subscription_id: Fake subscription ID for logging
            max_events: Maximum number of events to generate (for testing)
        """
        self.subscription_id = subscription_id
        self.max_events = max_events
        self.event_count = 0
        self.generated_article_ids = []

        logging.info(
            f"Initialized MockFactivaClient with subscription: {subscription_id}"
        )

    def pull(
        self, max_messages: int = 10, timeout: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Simulate pulling messages from Factiva Streams subscription.

        Args:
            max_messages: Maximum number of messages to pull
            timeout: Timeout in seconds (not used in mock)

        Returns:
            List of event dictionaries simulating Factiva events
        """
        if self.event_count >= self.max_events:
            logging.info("Mock client has reached max_events limit, no more events")
            return []

        # Randomly generate between 0 and max_messages events
        num_events = random.randint(
            0, min(max_messages, self.max_events - self.event_count)
        )

        if num_events == 0:
            logging.debug("Mock client generated 0 events this time")
            return []

        events = []
        for _ in range(num_events):
            # 80% chance of add, 15% rep, 4% del, 1% bulk event
            event_type = random.choices(
                ["add", "rep", "del", "bulk"], weights=[80, 15, 4, 1]
            )[0]

            if event_type == "bulk":
                event = self._generate_bulk_event()
            else:
                event = self._generate_article_event(event_type)

            events.append(event)
            self.event_count += 1

        logging.info(
            f"Mock client pulled {len(events)} events (total: {self.event_count}/{self.max_events})"
        )
        return events

    def _generate_article_event(self, action: str) -> Dict[str, Any]:
        """Generate a mock article event (add, rep, or del)"""

        # For 'rep' and 'del', reuse an existing article ID if available
        if action in ["rep", "del"] and self.generated_article_ids:
            an = random.choice(self.generated_article_ids)
        else:
            # Generate new article ID
            source_codes = ["WSJ", "NYT", "FT", "REU", "AFP", "AP", "BBC", "CNN"]
            source_code = random.choice(source_codes)
            an = f"{source_code}{random.randint(100000, 999999)}"

            if action == "add":
                self.generated_article_ids.append(an)

        # For delete events, only return minimal data
        if action == "del":
            return {"an": an, "document_type": "article", "action": "del"}

        # Generate full article event for 'add' and 'rep'
        now = datetime.utcnow()
        pub_date = now - timedelta(days=random.randint(0, 7))

        titles = [
            "Climate Change Impacts Global Economy",
            "Tech Giants Announce New AI Initiative",
            "Markets Rally on Positive Economic Data",
            "Healthcare Reform Debate Continues",
            "Renewable Energy Adoption Accelerates",
            "International Trade Agreement Signed",
            "Sports Teams Compete in Championship",
            "Cultural Festival Draws Large Crowds",
        ]

        bodies = [
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
            "Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo.",
        ]

        publishers = [
            "The Wall Street Journal",
            "New York Times",
            "Financial Times",
            "Reuters",
            "Agence France-Presse",
            "Associated Press",
        ]

        event = {
            "an": an,
            "document_type": "article",
            "action": action,
            "source_code": an[:3],  # Extract source code from AN
            "publication_date": pub_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "publication_datetime": pub_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "modification_date": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "modification_datetime": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "availability_datetime": pub_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "ingestion_datetime": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "title": random.choice(titles),
            "body": " ".join(random.choices(bodies, k=random.randint(1, 3))),
            "snippet": bodies[0][:200] + "...",
            "art": "",
            "credit": f"Staff Writer {random.randint(1, 100)}",
            "byline": f"By John Doe {random.randint(1, 50)}",
            "language_code": random.choice(["en", "fr", "es", "de"]),
            "copyright": f"Copyright {now.year} {random.choice(publishers)}. All Rights Reserved.",
            "region_of_origin": random.choice(
                ["EUR UK", "NAMZ USA", "APACZ AUS", "SAMZ BRA"]
            ),
            "publisher_name": random.choice(publishers),
            "section": random.choice(
                ["Business", "Technology", "World", "Sports", "Culture"]
            ),
            "word_count": str(random.randint(300, 1500)),
            "company_codes": ",".join(
                [
                    f"comp{random.randint(1000, 9999)}"
                    for _ in range(random.randint(0, 5))
                ]
            ),
            "subject_codes": ",".join(
                [f"subj{random.randint(100, 999)}" for _ in range(random.randint(1, 3))]
            ),
            "region_codes": random.choice(
                ["usa,namz", "uk,eur", "fra,eur", "chn,apacz"]
            ),
            "industry_codes": ",".join(
                [f"i{random.randint(10, 99)}" for _ in range(random.randint(0, 3))]
            ),
            "person_codes": "",
            "currency_codes": random.choice(["usd", "eur", "gbp", "jpy", ""]),
            "market_index_codes": "",
        }

        return event

    def _generate_bulk_event(self) -> Dict[str, Any]:
        """Generate a mock bulk event (e.g., source_delete)"""
        source_codes = ["ELECON", "TESTSR", "DEMOSRC"]
        source_code = random.choice(source_codes)

        return {
            "event_type": "source_delete",
            "source_code": source_code,
            "modification_datetime": datetime.utcnow().strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            ),
            "description": f"Delete all articles associated with source code {source_code} due to the expiration of the content licensing agreement.",
        }

    def acknowledge(self, event: Dict[str, Any]) -> None:
        """
        Simulate acknowledging an event.

        In the real Factiva client, this would remove the event from the queue.
        In the mock, we just log it.
        """
        an = event.get("an", event.get("event_type", "unknown"))
        logging.debug(f"Mock client acknowledged event: {an}")

    def close(self) -> None:
        """Close the mock client connection"""
        logging.info(f"Mock client closed. Processed {self.event_count} events total.")


class MockFactivaClientConfig:
    """Configuration for the mock Factiva client"""

    def __init__(
        self,
        service_account_id: str = "mock-service-account",
        subscription_id: str = "mock-subscription",
        max_events: int = 100,
        events_per_pull: int = 10,
    ):
        """
        Initialize mock client configuration.

        Args:
            service_account_id: Fake service account ID
            subscription_id: Fake subscription ID
            max_events: Maximum total events to generate
            events_per_pull: Maximum events to pull at once
        """
        self.service_account_id = service_account_id
        self.subscription_id = subscription_id
        self.max_events = max_events
        self.events_per_pull = events_per_pull

    def create_client(self) -> MockFactivaClient:
        """Create a mock Factiva client with this configuration"""
        return MockFactivaClient(
            subscription_id=self.subscription_id, max_events=self.max_events
        )
