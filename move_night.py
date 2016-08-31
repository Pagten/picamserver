#!/usr/bin/python3
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import os
import astral
import pytz
import re
import sys

TZ = pytz.timezone('Europe/Brussels')
DIR = '/mnt/storage0/timelapse/'
IMG_PATTERN = re.compile('img_([0-9_]+)(\.jpg|_md5-)')
LOC = astral.Location(('Eksel', 'Europe', 51.15, 5.3833, 'Europe/Brussels', 0))
LOC.solar_depression = 8.5

def move_files(day, path, base_dir):
  global logger
  logger.info("Handling dir '%s' as date '%s' with base dir '%s'", path, day, base_dir)
  for name in os.listdir(path):
    filepath = os.path.join(path, name)
    if os.path.isfile(filepath):
      m = IMG_PATTERN.match(name)
      if m is None:
        logger.info("No match for '%s'", filepath)
        continue

      timestr = m.group(1)
      time = TZ.localize(datetime.strptime(timestr, '%Y%m%d_%H%M%S'))
      if is_night(time):
        dst_dir = os.path.join(base_dir, 'night')
      else:
        dst_dir = base_dir
      dst_path = os.path.join(dst_dir, name)
      if filepath != dst_path:
        if os.path.exists(dst_path):
          logging.error("Destination file '%s' already exists!", dst_path)
          sys.exit('Abort!')
        os.makedirs(dst_dir, exist_ok=True)
        logging.info("Moving '%s' to '%s'", filepath, dst_path)
        os.rename(filepath, dst_path)

def is_night(time):
  return time < LOC.dawn(date=time, local=True) or LOC.dusk(date=time, local=True) < time

def run():
  src_path = DIR
  for name in os.listdir(src_path):
    path = os.path.join(src_path, name)
    if os.path.isdir(path):
      day = TZ.localize(datetime.strptime(name, '%Y%m%d'))
      night_path = os.path.join(path, 'night')
      if os.path.isdir(night_path):
        move_files(day, night_path, path)
      if os.path.isdir(path):
        move_files(day, path, path)

def setup_logging():
  global logger
  logger = logging.getLogger('')
  logger.setLevel(logging.INFO)
  formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%d/%m/%Y %H:    %M:%S')

  stream_handler = logging.StreamHandler(sys.stdout)
  stream_handler.setFormatter(formatter)
  logger.addHandler(stream_handler)

def main():
  setup_logging()
  run()

if __name__ == "__main__":
  main()

