
import logging

class LogObfuscator(logging.Filter):
    def __init__(self, keywords):
        self.keywords = keywords

    def filter(self, record):
        if record.msg:
            msg = record.msg
            for word in self.keywords:
                msg = msg.replace(word, "***")
            record.msg = msg
        return True