#!/usr/bin/python3
import os
import tempfile
import logging
from logging.handlers import RotatingFileHandler
import sys
import shutil
from subprocess import call

LOG_FILE = 'timelapse-video-builder.log'
SRC_FOLDER = '/mnt/constructioncam/timelapse'
DST_FOLDER = '/mnt/constructioncam-vids/timelapse'

AVCONV_BIN = '/usr/bin/avconv'
IMG_FMT = 'img_%04d.jpg'
FRAMERATE = 30
QUALITY = 22 # lower is better, 23 is default, 0 is lossless, sensible is between 18 and 28

class TimelapseVideoBuilder:
  def __init__(self, src_folder, dst_folder, framerate, quality):
    self.src_folder = src_folder
    self.dst_folder = dst_folder
    self.framerate = framerate
    self.quality = quality
    self.logger = logging.getLogger(type(self).__name__)

  def _make_symlinks(self, input_folder):
    tmp_dir = tempfile.mkdtemp(prefix='tmp-timelapse-symlinks-')
    files = sorted([f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))])
    i = 0
    for fn in files:
      src_file = os.path.join(input_folder, fn)
      dst_file = os.path.join(tmp_dir, IMG_FMT % i)
      os.symlink(src_file, dst_file)
      self.logger.debug("Created symlink from '%s' to '%s'", src_file, dst_file)
      i += 1
    return tmp_dir

  def run(self):
    for name in os.listdir(self.src_folder):
      path = os.path.join(self.src_folder, name)
      if os.path.isdir(path):
        self.handle_dir(path)

  def handle_dir(self, input_folder):
    (_, output_filename) = os.path.split(input_folder)
    output_path = os.path.join(self.dst_folder, "%s.mkv" % output_filename)
    if os.path.exists(output_path):
      self.logger.debug("Skipping '%s' because output file '%s' already exists", input_folder, output_path)
      return

    self.make_timelapse_video(input_folder, output_path)

  def make_timelapse_video(self, input_folder, output_filename):
    self.logger.info("Creating symlinks for '%s'", input_folder)
    tmp_dir = self._make_symlinks(input_folder)
    input_filename = os.path.join(tmp_dir, IMG_FMT)

    self.logger.info("Encoding video to '%s'", output_filename)
    call([AVCONV_BIN, '-f', 'image2', '-r', str(self.framerate), '-i', input_filename, '-r', str(self.framerate), '-vcodec', 'libx264', '-preset', 'slow', '-crf', str(self.quality), output_filename])
    self.logger.info("Removing temporary dir '%s'", tmp_dir)
    shutil.rmtree(tmp_dir)

def setup_logging():
  root_log = logging.getLogger('')
  root_log.setLevel(logging.INFO)
  formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

  stream_handler = logging.StreamHandler(sys.stdout)
  stream_handler.setFormatter(formatter)
  root_log.addHandler(stream_handler)

  file_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=(1024*1024*10), backupCount=7)
  file_handler.setFormatter(formatter)
  root_log.addHandler(file_handler)

def main():
  setup_logging()
  builder = TimelapseVideoBuilder(SRC_FOLDER, DST_FOLDER, FRAMERATE, QUALITY)
  builder.run()

if __name__ == "__main__":
  main()

