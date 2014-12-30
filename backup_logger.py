import logging
import sys

consoleformatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')

consolehandler = logging.StreamHandler(sys.stdout)
consolehandler.setFormatter(consoleformatter)

logger = logging.getLogger(__name__)

logger.addHandler(consolehandler)
logger.setLevel(logging.DEBUG)