import logging
import os
import sys

import sentry_sdk
import wmill
from sentry_sdk.integrations.logging import LoggingIntegration


def main(country: str, service: str, send_logs: bool):
    dsn = wmill.get_variable("f/secrets/SENTRY_DSN")
    if not dsn:
        sys.exit("SENTRY_DSN is empty: the secret is not set")

    run_id = os.environ.get("WM_ROOT_FLOW_JOB_ID") or os.environ.get("WM_JOB_ID", "local")

    def add_context_to_log(log, _hint):
        # Tags are not attached to Sentry logs: add them as attributes
        attributes = log.setdefault("attributes", {})
        attributes["country"] = country
        attributes["service"] = service
        attributes["run_id"] = run_id
        return log

    logging_kwargs = {}
    if send_logs:
        logging_kwargs = dict(
            enable_logs=True,
            before_send_log=add_context_to_log,
            integrations=[LoggingIntegration(sentry_logs_level=logging.INFO)],
        )

    sentry_sdk.init(dsn=dsn, environment="dev", release="glitchtip-log-test", **logging_kwargs)
    sentry_sdk.set_tag("country", country)
    sentry_sdk.set_tag("service", service)
    sentry_sdk.set_tag("run_id", run_id)

    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, force=True)
    logger = logging.getLogger("glitchtip_log_test")

    # A series of logs: only INFO and above are sent to GlitchTip
    logger.debug("[%s] log test: debug (printed here, not sent)", run_id)
    logger.info("[%s] log test: info 1/3", run_id)
    logger.info("[%s] log test: info 2/3", run_id)
    logger.info("[%s] log test: info 3/3", run_id)
    logger.warning("[%s] log test: warning, something looks odd", run_id)

    # An error that is handled: caught, reported with its traceback, and the
    # script carries on (the step does not fail).
    try:
        processed = 10
        total = 0
        ratio = processed / total
    except ZeroDivisionError:
        logger.error(
            "[%s] log test: handled error, division by zero while computing a ratio",
            run_id,
            exc_info=True,
        )

    logger.info("[%s] log test: finished, the error above was handled", run_id)

    # The SDK sends in a background thread: wait for the queue before exiting
    sentry_sdk.flush(timeout=15)
    print("Done. Look for run_id %s in GlitchTip (Logs and Issues)." % run_id)
    return {"run_id": run_id}
