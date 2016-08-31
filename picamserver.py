#!/usr/bin/python3
import socketserver
import time
from datetime import datetime, timedelta
import picamera
import logging
from logging.handlers import RotatingFileHandler
import threading
import os
import queue
import io
import sys
import hashlib
import functools
import pytz
import astral
from functools import partial

BIND_ADDRESS = '192.168.0.12'
BIND_PORT = 8000
LOG_FILE = 'picamserver.log'

TIMELAPSE_INTERVAL = timedelta(seconds=60)
TIMELAPSE_FOLDERS = ['/mnt/usb/timelapse/']
TIMELAPSE_FOLDERS_FALLBACK = ['/mnt/sdcard/timelapse/']
TIMELAPSE_ASTRAL_LOCATION = astral.Location(('Eksel', 'Europe', 51.15, 5.3833, 'Europe/Brussels', 0))
TIMELAPSE_ASTRAL_SOLAR_DEPRESSION = 8.5
TZ=pytz.timezone('Europe/Brussels')
 
STILL_RESOLUTION = (1640,1232) #(3240,2464)
VIDEO_RESOLUTION = (947,720)
VIDEO_FRAMERATE = 7 
VIDEO_BITRATE = 400000

DAYTIME_EXPOSURE_MODE = 'verylong'
DAYTIME_METER_MODE = 'backlit'
DAYTIME_AWB_MODE = 'off'
DAYTIME_AWB_GAINS = (1.75, 1.47)
    
DATESTR_FORMAT = '%Y%m%d'
SUBDIR_TEMPLATE = '%(datestr)s' + os.sep + '%(night)s'
DATETIMESTR_FORMAT = '%Y%m%d_%H%M%S' 
FILE_NAME_TEMPLATE  = 'img_%(datetimestr)s_md5-%(md5sum)s%(suffix)s.jpg'

class StreamTee:
  def __init__(self, streams):
    self.__streams = set(streams)

  def write(self, b):
    for s in set(self.__streams):
      try:
        s.write(b)
      except Exception:
        self.__streams.discard(s)
        try:
          s.close()
        except Exception:
          pass

  def flush(self):
    for s in set(self.__streams):
      try:
        s.flush()
      except Exception:
        self.__streams.discard(s)
        try:
          s.close()
        except Exception:
          pass

  def close(self):
    for s in set(self.__streams):
      try:
        s.close()
      except Exception:
        self.__streams.discard(s)

class TcpVideoStreamHandler(socketserver.StreamRequestHandler):
  def __init__(self, request, client_address, server):
    self.logger = logging.getLogger(type(self).__name__)
    super(TcpVideoStreamHandler, self).__init__(request, client_address, server)
    

  def handle(self):
    self.logger.info("Accepted new connection from %s, port %d", self.client_address[0], self.client_address[1])
    self.server.add_output(self.wfile)
    while self.server.keep_running and not self.wfile.closed:
      time.sleep(1)
    
  def finish(self):
    self.server.remove_output(self.wfile)
    self.logger.info("Client %s:%d disconnected", self.client_address[0], self.client_address[1])

class TcpVideoStreamServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  def __init__(self, camera, server_address, resolution, framerate, bitrate):
    self.logger = logging.getLogger(type(self).__name__)
    self.camera = camera
    self.camera.framerate = framerate
    self.resolution = resolution
    self.bitrate = bitrate
    self.outputs = set()
    type(self).allow_reuse_address = True
    super(TcpVideoStreamServer, self).__init__(server_address, TcpVideoStreamHandler)
    self.logger.info("Tcp video stream server listening on %s:%d", self.server_address[0], self.server_address[1])
    self.logger.info(" Resolution: %d x %d", self.resolution[0], self.resolution[1])
    self.logger.info(" Frame rate: %dfps", self.camera.framerate)
    self.logger.info("   Bit rate: %dbps", self.bitrate)

  def start(self):
    self.camera.start_recording(StreamTee(self.outputs), format='h264', resize=self.resolution, bitrate=self.bitrate)
    server_thread = threading.Thread(target=self.serve_forever)
    server_thread.daemon = True
    self.keep_running = True
    server_thread.start()

  def stop(self):
    self.keep_running = False
    self.camera.stop_recording()
    self.shutdown()

  def poll_recording_errors(self):
    self.camera.wait_recording()

  def add_output(self, output):
    self.outputs.add(output)
    self._outputs_changed()

  def remove_output(self, output):
    self.outputs.discard(output)
    self._outputs_changed()

  def _outputs_changed(self):
    try:
      self.camera.split_recording(StreamTee(self.outputs))
    except picamera.exc.PiCameraNotRecording:
      pass


class Timer:
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)
    self.q = queue.Queue()

  def sleep(self, seconds):
    try:
      self.q.get(timeout=seconds)
      self.logger.debug("Timer sleep of %d seconds was interrupted", seconds)
    except queue.Empty:
      pass

  def interrupt(self):
    self.q.put(None)


class Timelapse:
  def __init__(self, camera, resolution, interval, location):
    self.logger = logging.getLogger(type(self).__name__)
    self.camera = camera
    self.camera.resolution = resolution
    self.interval = interval
    self.location = location
    self.root_folders = set()
    self.fallback_folders = set()
    self.timer = Timer()

  def add_root_folder(self, folder):
    self.root_folders.add(folder)

  def remove_root_folder(self, folder):
    self.root_folders.discard(folder)

  def add_fallback_root_folder(self, folder):
    self.fallback_folders.add(folder)

  def remove_fallback_root_folder(self, folder):
    self.fallback_folders.discard(folder)

  def print_camera_settings(self, printfunc):
    printfunc(" Resolution: %d x %d", self.camera.resolution[0], self.camera.resolution[1])
    printfunc(" Analog gain: %f", self.camera.analog_gain)
    printfunc(" Digital gain: %f", self.camera.digital_gain)
    printfunc(" ISO value: %f", self.camera.iso)
    printfunc(" Shutter time: %dus", self.camera.shutter_speed)
    printfunc(" Exposure time: %dus", self.camera.exposure_speed)
    printfunc(" AWB gains: %s", self.camera.awb_gains)
    printfunc(" AWB mode: %s", self.camera.awb_mode)

  def generate_filename(self, root_folder, time, md5sum):
    date_str = time.strftime(DATESTR_FORMAT)
    datetime_str = time.strftime(DATETIMESTR_FORMAT)
    night_str = 'night' if self._is_night(time) else ''

    suffix_int = 0
    suffix_str = ''
    subdir = SUBDIR_TEMPLATE % { 
      'datetimestr': datetime_str,
      'datestr': date_str,
      'suffix': suffix_str,
      'night': night_str}
    while suffix_int == 0 or os.path.exists(filename):
      file_name = FILE_NAME_TEMPLATE % { 
        'datetimestr': datetime_str,
        'datestr': date_str,
        'suffix': suffix_str,
        'md5sum': md5sum,
        'night': night_str}
      filename = os.path.join(root_folder, subdir, file_name)
      suffix_int += 1
      suffix_str = '_%d' % suffix_int
      
    return filename 

  def start(self):
    self.logger.info("Starting time lapse with capture settings:")
    self.print_camera_settings(self.logger.info)

    server_thread = threading.Thread(target=self.__run)
    server_thread.daemon = True
    self.keep_running = True
    server_thread.start()

  def stop(self):
    self.keep_running = False
    self.timer.interrupt()

  def _is_night(self, time):
    return time < self.location.dawn(date=time, local=True) or self.location.dusk(date=time, local=True) < time
  
  def _write_to_file(self, input, root_folders, now=None):
    if now is None: now = self._now()
    success = True
    for root_folder in set(root_folders):
      try:
        md5sum = self.__calc_md5sum(input)
        filename = self.generate_filename(root_folder, now, md5sum)
        dirname = os.path.dirname(filename)
        os.makedirs(dirname, exist_ok=True)
        input.seek(0)
        with open(filename, 'wb') as f:
          f.write(input.getvalue())
        self.logger.info("Wrote image to '%s'", filename)
      except:
        success = False
        self.logger.exception("Error writing captured image to file '%s'", filename)
    return success
  
  def _now(self):
    return TZ.localize(datetime.now())

  def __calc_md5sum(self, f):
    d = hashlib.md5()
    f.seek(0)
    for buf in iter(partial(f.read, 4096), b''):
      d.update(buf)
    return d.hexdigest()

  def __run(self):
    self.__wait_until_next_capture()
    while self.keep_running:
      self.logger.debug("Starting capture")
      mem_stream = io.BytesIO()
      self.print_camera_settings(self.logger.debug)
      now = self._now()
      self.camera.capture(mem_stream, 'jpeg')
      self.logger.debug("Captured image successfully")

      success = self._write_to_file(mem_stream, self.root_folders, now)
      for fallback_folder in self.fallback_folders:
        if success: break;
        success = self._write_to_file(mem_stream, self.fallback_folders, now)
        
      self.__wait_until_next_capture()

  def __wait_until_next_capture(self):
    now = self._now()
    next_instant = self.__floorTime(now + self.interval, self.interval)
    delay = max(0, (next_instant - now).total_seconds())
    self.logger.info("Sleeping for %d seconds before next image capture at %s", delay, next_instant.strftime('%d-%m-%Y %H:%M:%S'))
    self.timer.sleep(delay)

  def __floorTime(self, dt=None, delta=timedelta(minutes=1)):
    roundTo = delta.total_seconds()
    if dt == None: dt = self._now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds // roundTo) * roundTo
    return dt + timedelta(0, rounding - seconds, -dt.microsecond)


class PiCamServer:
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)

  def __warm_up(self, camera, seconds):
    self.logger.info("Warming up camera for %d seconds", seconds)
    camera.start_preview()
    time.sleep(seconds)
    self.logger.info("Done warming up camera")

  def run(self):
    camera = picamera.PiCamera()
    camera.rotation = 180
    self.__warm_up(camera, 3)

    self.logger.info("Setting camera settings")
    camera.meter_mode = DAYTIME_METER_MODE
    camera.exposure_mode = DAYTIME_EXPOSURE_MODE
    camera.awb_gains = DAYTIME_AWB_GAINS
    camera.awb_mode = DAYTIME_AWB_MODE
    
    self.logger.info("Creating video stream server")
    video_server = TcpVideoStreamServer(camera, (BIND_ADDRESS, BIND_PORT), VIDEO_RESOLUTION, VIDEO_FRAMERATE, VIDEO_BITRATE)

    self.logger.info("Creating Astral location")
    location = TIMELAPSE_ASTRAL_LOCATION
    location.solar_depression = TIMELAPSE_ASTRAL_SOLAR_DEPRESSION

    self.logger.info("Creating time lapse")
    timelapse = Timelapse(camera, STILL_RESOLUTION, TIMELAPSE_INTERVAL, location)
    for f in TIMELAPSE_FOLDERS:
      timelapse.add_root_folder(f)
    for f in TIMELAPSE_FOLDERS_FALLBACK:
      timelapse.add_fallback_root_folder(f)

    self.logger.info("Starting video server")
    video_server.start()
    
    self.logger.info("Starting time lapse")
    timelapse.start()

    try:
      while True:
        time.sleep(1)
    except KeyboardInterrupt:
      self.logger.info("Caught keyboard interrupt. Shutting down server...")
    finally:
      timelapse.stop()
      video_server.stop()
      video_server.shutdown()
      camera.close() 

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
 
  server = PiCamServer()
  server.run()

if __name__ == "__main__":
  main()

