#!/usr/bin/python3
import time
import datetime
from datetime import datetime, timedelta
import os
import sys
import stat
import logging
from logging.handlers import RotatingFileHandler


LOG_FILE = 'image-watchdog.log'

ROOT_DIR = '/mnt/usb/timelapse'
TIMEOUT = timedelta(seconds=120)
INTERVAL_SECONDS = 120

class DirectoryWatchdog:
  def __init__(self, root_dir, timeout, interval_seconds):
    self.root_dir = root_dir
    self.timeout = timeout
    self.interval_seconds = interval_seconds
    self.logger = logging.getLogger(type(self).__name__)

  def _get_last_dir_modification_datetime(self):
    latest_modification_time = None
    try:
      for root, sub_folders, files in os.walk(self.root_dir):
        for sub_folder in sub_folders:
          foldername = os.path.join(root, sub_folder)
          try:
            modification_time = datetime.fromtimestamp(os.stat(foldername)[stat.ST_MTIME])
            if latest_modification_time is None or modification_time > latest_modification_time:
              latest_modification_time = modification_time
          except:
            self.logger.exception("Error getting modification time of folder '%s'", foldername)
    except:
      self.logger.exception("Error walking '%s'", self.root_dir)
    return latest_modification_time

  def _on_fault(self):
    os.system('/sbin/shutdown -r +1')

  def run_forever(self):
    while True:
      self.logger.debug("Sleeping for %d seconds", self.interval_seconds)
      time.sleep(self.interval_seconds)
      self.run_one()

  def run_once(self):
    last_mod_time = self._get_last_dir_modification_datetime()
    if last_mod_time is None:
      self.logger.error("Last modification time is None, rebooting system.")
      self._on_fault()
      continue

    timediff = (datetime.now() - last_mod_time)
    self.logger.debug("Time since last dir modification: %s", str(timediff))
    if (timediff > self.timeout):
      self.logger.warning("Time since last modification %s exceeds timeout of %s, rebooting system.", str(timediff), str(self.timeout))
      self._on_fault()

def setup_logging():
  root_log = logging.getLogger('')
  root_log.setLevel(logging.DEBUG)
  formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

  stream_handler = logging.StreamHandler(sys.stdout)
  stream_handler.setFormatter(formatter)
  root_log.addHandler(stream_handler)

  file_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=(1024*1024*10), backupCount=7)
  file_handler.setFormatter(formatter)
  root_log.addHandler(file_handler)

def main():
  setup_logging()
  watchdog = DirectoryWatchdog(ROOT_DIR, TIMEOUT, INTERVAL_SECONDS)
  watchdog.run_once()

if __name__ == "__main__":
  main()
