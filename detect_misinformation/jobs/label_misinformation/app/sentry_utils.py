import os
import sys
import logging
import sentry_sdk
import tomli
from sentry_sdk.integrations.logging import LoggingIntegration

def get_app_version():
     # Open and read the pyproject.toml file
    with open('pyproject.toml', 'rb') as toml_file:
        pyproject_data = tomli.load(toml_file)

    # Access the version from the pyproject.toml file
    version = pyproject_data['project']['version']
    return version


def _country():
    # Set via the COUNTRY env var; defaults to "prod" like the job itself.
    return os.environ.get("COUNTRY") or "prod"


def _service():
    # Name of the running job so logs and events can be filtered per service.
    # SERVICE_NAME overrides it; otherwise it comes from the entry script name.
    name = os.environ.get("SERVICE_NAME")
    if name:
        return name
    script = sys.argv[0] if sys.argv and sys.argv[0] and sys.argv[0] != "-c" else ""
    base = os.path.splitext(os.path.basename(script))[0]
    if base in ("main", "__main__", "run", ""):
        parent = os.path.basename(os.path.dirname(os.path.abspath(script))) if script else ""
        base = parent or base
    return base or "unknown"


def _add_context_to_log(log, _hint):
    # Scope tags are not attached to Sentry logs: add them explicitly.
    attributes = log.setdefault("attributes", {})
    attributes["country"] = _country()
    attributes["service"] = _service()
    return log


def sentry_init():
    if os.environ.get("SENTRY_DSN", None) != None:
        logging.info("Sentry init")
        logging_kwargs = {}
        if os.getenv("SENTRY_LOGGING") == "true":
            logging_kwargs = dict(
                enable_logs=True,
                before_send_log=_add_context_to_log,
                integrations=[
                    # Only send WARNING (and higher) logs to Sentry logs,
                    # even if the logger is set to a lower level.
                    LoggingIntegration(sentry_logs_level=logging.INFO),
                ]
            )
        sentry_sdk.init(
            enable_tracing=False,
            traces_sample_rate=0.3,
            # To set a uniform sample rate
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production,
            profiles_sample_rate=0.3,
            release=get_app_version,
            environment=os.environ.get("ENV"),
            # functions_to_trace=functions_to_trace,
            # integrations=[ # TODO : https://docs.sentry.io/platforms/python/integrations/ray/
            #     RayIntegration(),
            # ],
            **logging_kwargs
        )
        # Tag every event, transaction and cron check-in with the country and service
        sentry_sdk.set_tag("country", _country())
        sentry_sdk.set_tag("service", _service())
    else:
        logging.info("Sentry not init - SENTRY_DSN not found")


def sentry_close():
    sentry_sdk.flush(timeout=2.0)
