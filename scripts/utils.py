import logging


class LogObfuscator(logging.Filter):
    """
    A logging filter that obfuscates sensitive keywords in log messages.

    This filter scans the raw log record message (`record.msg`) and replaces
    occurrences of specified keywords with a placeholder string (e.g., "***").
    It is useful for preventing sensitive information such as passwords or
    tokens from appearing directly in log output.

    Note:
        The filter modifies only the `msg` attribute of the log record. If the
        logger uses formatting arguments (e.g., logger.info("User %s", name)),
        sensitive data passed via `record.args` will NOT be obfuscated.
    """

    def __init__(self, keywords):
        self.keywords = keywords

    def filter(self, record):
        """
        Apply the obfuscation filter to a log record.

        This replaces all occurrences of the configured keywords found in
        the raw message (`record.msg`) before the record is processed by
        the rest of the logging pipeline.

        Args:
            record (logging.LogRecord): The log record being processed.

        Returns:
            bool: Always returns True to ensure the record continues through
            the logging pipeline.
        """
        if record.msg:
            msg = record.msg
            for word in self.keywords:
                msg = msg.replace(word, "***")
            record.msg = msg
        return True
