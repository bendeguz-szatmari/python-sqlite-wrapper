import sys

class LogHandler:
    """wrapper for log messages"""

    def __init__(self, loglevel):
        self.__logLevel = loglevel

    def debug(self, msg):
        if self.__logLevel == 0:
            print("[DEBUG]\t\t", msg)

    def info(self, msg):
        if self.__logLevel <= 1:
            print("[INFO]\t\t", msg)

    def warning(self, msg):
        if self.__logLevel <= 2:
            print("[WARNING]\t", msg)

    def critical(self, msg):
        if self.__logLevel <= 3:
            print("[CRITICAL]\t", msg)

    def fatal(self, msg):
        if self.__logLevel <= 4:
            print("[FATAL]\t\t", msg)

    def exceptionHandling(self, e: Exception):
        etype = sys.exc_info()[0].__name__
        self.critical("Exception type " + etype + " caught: " + e.__str__())
