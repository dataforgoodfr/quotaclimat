"""
Factiva Streaming Data Ingestion

This module handles the ingestion of Factiva articles from the Streaming API
into the PostgreSQL database. It processes events (add, rep, del) and bulk events
(source_delete) according to Factiva's event-driven Pub/Sub model.
"""

import asyncio
import logging
import os
import sys
import time
from typing import Any, Dict, List

import sentry_sdk
from sentry_sdk.crons import monitor

from postgres.database_connection import get_db_session
from postgres.insert_factiva_data import get_article_stats, process_factiva_event
from quotaclimat.data_ingestion.factiva.config import (
    get_batch_size,
    get_factiva_service_account_id,
    get_factiva_subscription_id,
    get_max_messages_per_pull,
    get_poll_interval,
    is_mock_mode,
)
from quotaclimat.data_ingestion.factiva.mock_factiva_client import (
    MockFactivaClient,
    MockFactivaClientConfig,
)
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import CustomFormatter
from quotaclimat.utils.sentry import sentry_init


class FactivaStreamIngestion:
    """
    Main class for ingesting Factiva streaming data.

    This class handles:
    - Connecting to Factiva Streams (or mock for testing)
    - Pulling events from the subscription
    - Processing events (add/rep/del/bulk)
    - Saving to PostgreSQL
    - Acknowledging processed events
    """

    def __init__(self):
        """Initialize the Factiva stream ingestion service"""
        self.service_account_id = get_factiva_service_account_id()
        self.subscription_id = get_factiva_subscription_id()
        self.mock_mode = is_mock_mode()
        self.max_messages = get_max_messages_per_pull()
        self.poll_interval = get_poll_interval()
        self.batch_size = get_batch_size()

        self.client = None
        self.running = True
        self.stats = {
            "total_events": 0,
            "add_count": 0,
            "update_count": 0,
            "delete_count": 0,
            "source_delete_count": 0,
            "skip_count": 0,
            "error_count": 0,
        }

        logging.info(f"Initialized FactivaStreamIngestion")
        logging.info(f"Mock mode: {self.mock_mode}")
        logging.info(f"Subscription ID: {self.subscription_id}")
        logging.info(f"Max messages per pull: {self.max_messages}")
        logging.info(f"Poll interval: {self.poll_interval}s")

    def _create_client(self):
        """Create Factiva client (real or mock)"""
        if self.mock_mode:
            logging.info("Creating mock Factiva client for testing")
            config = MockFactivaClientConfig(
                service_account_id=self.service_account_id or "mock-account",
                subscription_id=self.subscription_id or "mock-subscription",
                max_events=int(os.environ.get("FACTIVA_MOCK_MAX_EVENTS", "1000")),
                events_per_pull=self.max_messages,
            )
            return config.create_client()
        else:
            logging.info("Creating real Factiva client")
            # TODO: Import and use real Factiva client library when available
            # from factiva_client import FactivaStreamClient
            # return FactivaStreamClient(
            #     service_account_id=self.service_account_id,
            #     subscription_id=self.subscription_id
            # )
            raise NotImplementedError(
                "Real Factiva client not yet implemented. "
                "Set FACTIVA_MOCK_MODE=true for testing."
            )

    def process_events(self, events: List[Dict[str, Any]]) -> None:
        """
        Process a batch of events and save to database.

        Args:
            events: List of Factiva events to process
        """
        if not events:
            return

        session = get_db_session()

        try:
            processed_events = []

            for event in events:
                try:
                    action = process_factiva_event(event, session)

                    # Update statistics
                    self.stats["total_events"] += 1
                    if action == "add":
                        self.stats["add_count"] += 1
                    elif action == "update":
                        self.stats["update_count"] += 1
                    elif action == "delete":
                        self.stats["delete_count"] += 1
                    elif action == "source_delete":
                        self.stats["source_delete_count"] += 1
                    elif action == "skip":
                        self.stats["skip_count"] += 1

                    processed_events.append(event)

                except Exception as err:
                    logging.error(f"Error processing event: {err}")
                    # Roll back to clear any failed transaction and allow next events to proceed
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    self.stats["error_count"] += 1
                    # Don't add to processed_events - will not be acknowledged
                    continue

            # Commit all changes in one transaction
            try:
                session.commit()
                logging.info(
                    f"Successfully committed batch of {len(processed_events)} events"
                )

                # Acknowledge only successfully processed events
                for event in processed_events:
                    self.client.acknowledge(event)

            except Exception as commit_err:
                logging.error(f"Error committing batch: {commit_err}")
                session.rollback()
                # Don't acknowledge any events if commit failed
                raise

            # Log statistics periodically
            if self.stats["total_events"] % 100 == 0:
                self._log_statistics()

        except Exception as err:
            logging.error(f"Error in batch processing: {err}")
            session.rollback()
            raise
        finally:
            session.close()

    def _log_statistics(self) -> None:
        """Log current processing statistics"""
        logging.info("=" * 60)
        logging.info("FACTIVA INGESTION STATISTICS")
        logging.info(f"Total events processed: {self.stats['total_events']}")
        logging.info(f"  - Added: {self.stats['add_count']}")
        logging.info(f"  - Updated: {self.stats['update_count']}")
        logging.info(f"  - Deleted: {self.stats['delete_count']}")
        logging.info(f"  - Source deletes: {self.stats['source_delete_count']}")
        logging.info(f"  - Skipped: {self.stats['skip_count']}")
        logging.info(f"  - Errors: {self.stats['error_count']}")

        # Log database statistics
        session = get_db_session()
        try:
            db_stats = get_article_stats(session)
            logging.info(
                f"Database: {db_stats['active']} active articles, {db_stats['deleted']} deleted"
            )
        except Exception as err:
            logging.warning(f"Could not get database stats: {err}")
        finally:
            session.close()

        logging.info("=" * 60)

    async def run_ingestion_loop(self, exit_event: asyncio.Event) -> None:
        """
        Main ingestion loop: pull events and process them.

        Args:
            exit_event: Event to signal when to stop the loop
        """
        try:
            # Tables are created by Alembic migrations in docker-entrypoint-factiva.sh
            # Create Factiva client
            self.client = self._create_client()

            logging.info("Starting Factiva ingestion loop...")

            while self.running:
                try:
                    # Pull events from Factiva
                    logging.debug(f"Pulling up to {self.max_messages} messages...")
                    events = self.client.pull(max_messages=self.max_messages)

                    if events:
                        logging.info(f"Received {len(events)} events from Factiva")
                        self.process_events(events)
                    else:
                        logging.debug("No events received, waiting...")

                    # Wait before next poll
                    await asyncio.sleep(self.poll_interval)

                except KeyboardInterrupt:
                    logging.info("Received interrupt signal, stopping...")
                    self.running = False
                    break
                except Exception as err:
                    logging.error(f"Error in ingestion loop: {err}")
                    # Wait a bit before retrying on error
                    await asyncio.sleep(self.poll_interval)
                    continue

            # Final statistics
            self._log_statistics()

            # Close client
            if self.client:
                self.client.close()

            exit_event.set()

        except Exception as err:
            logging.fatal(f"Fatal error in ingestion loop: {err}")
            sys.exit(1)


async def main():
    """Main entry point for Factiva streaming ingestion"""
    with monitor(monitor_slug="factiva-ingestion"):
        try:
            logging.info("Starting Factiva Streaming Ingestion Service")
            sentry_init()

            event_finish = asyncio.Event()

            # Start health check server
            health_check_task = asyncio.create_task(run_health_check_server())

            # Start ingestion
            ingestion = FactivaStreamIngestion()
            await ingestion.run_ingestion_loop(event_finish)

            # Wait for completion
            await event_finish.wait()

            # Cancel health check
            health_check_task.cancel()

        except Exception as err:
            logging.fatal(f"Main crashed: {err}")
            sys.exit(1)

    logging.info("Exiting with success")
    sys.exit(0)


if __name__ == "__main__":
    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(level=os.getenv("LOGLEVEL", "INFO").upper())

    if logger.hasHandlers():
        logger.handlers.clear()

    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    # Run main
    asyncio.run(main())
    sys.exit(0)
