import logging
import sys


class ProLogger:
    """
    ELITE PRO LOGGER v2.0 - The most professional logging system.
    Features: ANSI Colors, Visual Framing, Summary Reports.
    """

    # ANSI Colors
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    ENDC = "\033[0m"

    def __init__(self, name="SMART_CITY_PRO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Format: [TIME] | LEVEL | MESSAGE
        formatter = logging.Formatter(
            "%(message)s"  # We handle formatting manually for colors
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _get_time(self):
        from datetime import datetime

        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def info(self, msg):
        self.logger.info(
            f"[{self._get_time()}] | {self.BLUE}INFO{self.ENDC}     | {msg}"
        )

    def warn(self, msg):
        self.logger.info(
            f"[{self._get_time()}] | {self.YELLOW}WARNING{self.ENDC}  | ⚠️  {msg}"
        )

    def error(self, msg):
        self.logger.info(
            f"[{self._get_time()}] | {self.RED}{self.BOLD}ERROR{self.ENDC}    | 🚨  {self.RED}{msg}{self.ENDC}"
        )

    def success(self, msg):
        self.logger.info(
            f"[{self._get_time()}] | {self.GREEN}SUCCESS{self.ENDC}  | ✅  {self.GREEN}{msg}{self.ENDC}"
        )

    def header(self, title):
        width = 70
        self.logger.info("\n" + self.BOLD + self.HEADER + "=" * width + self.ENDC)
        self.logger.info(self.BOLD + self.HEADER + title.center(width) + self.ENDC)
        self.logger.info(self.BOLD + self.HEADER + "=" * width + self.ENDC + "\n")

    def summary(self, stats: dict):
        """
        Prints a professional summary box.
        """
        width = 50
        self.logger.info(
            "\n" + self.BOLD + self.BLUE + "+" + "-" * (width - 2) + "+" + self.ENDC
        )
        self.logger.info(
            self.BOLD
            + self.BLUE
            + "| "
            + "ELITE EXECUTION SUMMARY".center(width - 4)
            + " |"
            + self.ENDC
        )
        self.logger.info(
            self.BOLD + self.BLUE + "+" + "-" * (width - 2) + "+" + self.ENDC
        )

        for key, value in stats.items():
            line = f"| {key:<25}: {value:<18} |"
            self.logger.info(self.BOLD + self.BLUE + line + self.ENDC)

        self.logger.info(
            self.BOLD + self.BLUE + "+" + "-" * (width - 2) + "+" + self.ENDC + "\n"
        )


# Global singleton
log = ProLogger()
