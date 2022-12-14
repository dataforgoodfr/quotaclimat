import json
import traceback
from logging import Handler, CRITICAL, ERROR, WARNING, INFO, FATAL, DEBUG, NOTSET, Formatter

import six
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


ERROR_COLOR = '#FF0000'
WARNING_COLOR = '#FFA500'
INFO_COLOR = '#439FE0'

COLORS = {
    CRITICAL: ERROR_COLOR,
    FATAL: ERROR_COLOR,
    ERROR: ERROR_COLOR,
    WARNING: WARNING_COLOR,
    INFO: INFO_COLOR,
    DEBUG: INFO_COLOR,
    NOTSET: INFO_COLOR,
}


class NoStacktraceFormatter(Formatter):
    """
    By default the stacktrace will be formatted as part of the message.
    Since we want the stacktrace to be in the attachment of the Slack message,
     we need a custom formatter to leave it out of the message
    """

    def formatException(self, ei):
        return None

    def format(self, record):
        # Work-around for https://bugs.python.org/issue29056
        saved_exc_text = record.exc_text
        record.exc_text = None
        try:
            return super(NoStacktraceFormatter, self).format(record)
        finally:
            record.exc_text = saved_exc_text


class SlackerLogHandler(Handler):
    def __init__(self, slack_token, channel, stack_trace=True, fail_silent=False):
        Handler.__init__(self)
        self.formatter = NoStacktraceFormatter()

        self.stack_trace = stack_trace
        self.fail_silent = fail_silent

        self.client = WebClient(token=slack_token)

        self.channel = channel
        if not self.channel.startswith('#') and not self.channel.startswith('@'):
            self.channel = '#' + self.channel

    def build_msg(self, record):
        return six.text_type(self.format(record))

    def send_slack_message(self, message, attachments=None):
        self.client.chat_postMessage(
            channel=self.channel,
            text=message,
            attachments=attachments
        )

    def build_trace(self, record, fallback):
        trace = {
            'fallback': fallback,
            'color': COLORS.get(self.level, NOTSET)
        }
        if record.exc_info:
            trace['text'] = '\n'.join(traceback.format_exception(*record.exc_info))
        return trace

    def emit(self, record):
        message = self.build_msg(record)
        if self.stack_trace:
            trace = self.build_trace(record, fallback=message)
            attachments = json.dumps([trace])
        else:
            attachments = None
        try:
            self.send_slack_message(
                message=message,
                attachments=attachments
            )
        except SlackApiError as e:
            if self.fail_silent:
                pass
            else:
                raise e
