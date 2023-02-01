# import nest_asyncio
# nest_asyncio.apply()

import logging
from IPython.display import display, HTML


class DisplayHandler(logging.Handler):
    def emit(self, record):
        message = self.format(record)
        display(message)


class HTMLFormatter(logging.Formatter):
    level_colors = {
        logging.DEBUG: 'lightblue',
        logging.INFO: 'dodgerblue',
        logging.WARNING: 'goldenrod',
        logging.ERROR: 'crimson',
        logging.CRITICAL: 'firebrick'
    }

    def __init__(self):
        super().__init__(
            '<span style="font-weight: bold; color: green">{asctime}</span> '
            '[<span style="font-weight: bold; color: {levelcolor}">{levelname}</span>] '
            '{message}',
            style='{'
        )

    def format(self, record):
        record.levelcolor = self.level_colors.get(record.levelno, 'black')
        return HTML(super().format(record))


log = logging.getLogger()
handler = DisplayHandler()
handler.setFormatter(HTMLFormatter())
log.addHandler(handler)
log.setLevel(logging.DEBUG)
