#!/usr/bin/python3
import os
import shutil
import logging
import re
import hashlib
import sys
import functools
import subprocess
from logging.handlers import RotatingFileHandler
from functools import partial

LOG_FILE = 'moveimages.log'

SOURCE_FOLDERS = ['/mnt/usb/timelapse/', '/mnt/sdcard/timelapse/']
TARGET_FOLDERS = ['/mnt/storage0/timelapse/']
MOUNT_POINTS   = ['/mnt/storage0/']

MD5SUM_REGEX = re.compile(r"_md5-(?P<md5sum>[0-9A-Fa-f]32)[_\.]")

def is_subdir_of(path, directory):
    path = os.path.realpath(path)
    directory = os.path.realpath(directory)
    relative = os.path.relpath(path, directory)
    return not relative.startswith(os.pardir + os.sep)

def get_md5sum(path, try_from_basename=False):
  if try_from_basename:
    match = MD5SUM_REGEX.search(os.path.basename(path))
    if match:
      md5sum = match.group('md5sum')
      if md5sum:
        logging.debug('Found MD5 sum in filename %s: %s', path, md5sum)
        return md5sum

  d = hashlib.md5()
  with open(path, mode='rb') as f:
    for buf in iter(partial(f.read, 4096), b''):
      d.update(buf)

  md5sum = d.hexdigest()
  logging.debug("Calculated MD5 sum %s for '%s'", md5sum, path)
  return md5sum

def mount(path):
  subprocess.call(["/bin/mount", path], timeout=10)


class InvalidPathException(Exception):
  def __init(self, path):
    self.path = path

  def __str__(self):
    return repr(self.path)


class FileMover():
  def __init__(self, src_folders, dst_folders, mountpoints):
    self.logger = logging.getLogger(type(self).__name__)
    self.src_folders = src_folders
    self.dst_folders = dst_folders
    self.mountpoints = mountpoints

  def is_src_path(self, path):
    return any([is_subdir_of(path, x) for x in self.src_folders])

  def is_dst_path(self, path):
    return any([is_subdir_of(path, x) for x in self.dst_folders])

  def _check_src_path(self, src_path):
    if not self.is_src_path(src_path):
      logging.error("Path '%s' is not a file or subdirectory of any configured source folder", src_path)
      raise InvalidPathException(src_path)
    return src_path

  def _check_dst_path(self, dst_path):
    if not self.is_dst_path(dst_path):
      logging.error("Path '%s' is not a subdirectory of any configured destination folder", dst_path)
      raise InvalidPathException(dst_path)
    return dst_path
  
  def run(self):
    # First try to mount all destination folders
    for path in self.mountpoints:
      self.logger.info("Mounting '%s'", path)
      try:
        mount(path)
      except Exception as e:
        self.logger.error("Error mounting folder '%s': %s", path, str(e))

    # Start moving files
    for src_path in self.src_folders:
      self.logger.info("Handling source folder '%s'", src_path)
      for name in os.listdir(src_path):
        path = os.path.join(src_path, name)
        if os.path.isdir(path):
          self.move_dir(src_path, name, self.dst_folders)
        elif os.path.isfile(path):
          self.move_file(src_path, name, self.dst_folders)

  def move_dir(self, base_src_path, rel_src_path, base_dst_paths):
    src_path = self._check_src_path(os.path.join(base_src_path, rel_src_path))
    dst_paths = list(map(lambda base_dst_path: self._check_dst_path(os.path.join(base_dst_path, rel_src_path)), base_dst_paths))

    self.logger.info("Copying dir '%s'", src_path)
    dst_reachable = False
    for dst_path in dst_paths:
      try:
        os.mkdir(dst_path)
        dst_reachable = True
      except FileExistsError as e:
        dst_reachable = True
      except Exception as e:
        self.logger.warning("Unable to create destination dir '%s': %s", dst_path, str(e))

    if not dst_reachable:
      self.logger.error("Aborting because no destination dir could be created")
      return;

    for name in os.listdir(src_path):
      path = os.path.join(src_path, name)
      if os.path.isdir(path):
        self.move_dir(src_path, name, dst_paths)
      elif os.path.isfile(path):
        self.move_file(src_path, name, dst_paths)

    try:
      os.rmdir(src_path)
      self.logger.info("Removed empty source dir '%s'", src_path)
    except OSError as e:
      self.logger.warning("Not removing source dir '%s' because it is not empty: %s", src_path, str(e))
  
  def move_file(self, src_dir_path, filename, dst_dir_paths):
    # Check input
    if len(dst_dir_paths) == 0:
      raise ValueError("List of destination dirs cannot be empty")

    src_path = self._check_src_path(os.path.join(src_dir_path, filename))
    for dst_dir_path in dst_dir_paths:
      self._check_dst_path(os.path.join(dst_dir_path, filename))
    
    # Calculate or retreive MD5 sum
    try:
      src_md5sum = get_md5sum(src_path, try_from_basename=True)
    except Exception as e:
      self.logger.error("Error retrieving MD5 sum of '%s': %s", src_path, str(e))
      return

    filesize = os.stat(src_path).st_size
    all_ok = True
    for dst_dir_path in dst_dir_paths:
      dst_path = os.path.join(dst_dir_path, filename)

      if os.path.isfile(dst_path):
        try:
          dst_md5sum = get_md5sum(dst_path, try_from_basename=False)
        except Exception as e:
          self.logger.error("Unable to calculate MD5 sum of existing file '%s': %s", dst_path, str(e))
          dst_md5sum = None

        if dst_md5sum is not None and dst_md5sum == src_md5sum:
          self.logger.info("Not copying file '%s' to '%s' because destination file already exists and MD5 sum matches source", src_path, dst_path)
          continue
        else:
          self.logger.warning("Destination file '%s' already exists, but MD5 sum does not match source. Will overwrite...", dst_path)

      # Perform copy
      self.logger.info("Copying file '%s' to '%s' [filesize: %d KB, md5sum: %s]", src_path, dst_path, filesize // 1024, src_md5sum)
      try:
        shutil.copy2(src_path, dst_path)
        self.logger.debug("Copied file '%s' to '%s'", src_path, dst_path)
      except Exception as e:
        all_ok = False
        self.logger.error("Unable to copy file '%s' to '%s': %s", src_path, dst_path, str(e))
        continue

      # Calculate and compare MD5 sum
      try:
        dst_md5sum = get_md5sum(dst_path, try_from_basename=False)
      except Exception as e:
        all_ok = False
        self.logger.error("Error calculating MD5 sum of '%s': %s", dst_path, str(e))
        continue

      if src_md5sum != dst_md5sum:
        all_ok = False
        self.logger.error("MD5 sum of '%s' does NOT match that '%s'. Found %s instead of %s", dst_path, src_path, dst_md5sum, src_md5sum)

    # Remove if all MD5 sums matched
    if all_ok:
      self.logger.info("Removing '%s'", src_path)
      os.remove(src_path)
    else:
      self.logger.warning("Not removing '%s' because not all destination MD5 sums matched", src_path)


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
  fm = FileMover(SOURCE_FOLDERS, TARGET_FOLDERS, MOUNT_POINTS)
  fm.run()


if __name__ == "__main__":
  main()

