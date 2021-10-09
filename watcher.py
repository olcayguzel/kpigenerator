import os 
import time
from logger import LogTypes
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

class Watcher:
	def __init__(self) -> None:
		self.Folder = os.getcwd()
		self.Queue = None
		self.Log = None

	def onfilecreate(self, event):
		self.Queue.put(event.src_path)
		self.Log.write(LogTypes.DEBUG, f"New file added to queue: {event.src_path}")

	def createeventhandler(self):
		patterns = ["*.cdr"]
		ignore_patterns = None
		ignore_directories = True
		case_sensitive = False
		handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
		handler.on_created = self.onfilecreate
		return handler

	def watchFolder(self):
		handler = self.createeventhandler()
		watcher = Observer()
		watcher.schedule(handler, self.Folder, recursive=False)
		watcher.start()
		self.Log.write(LogTypes.DEBUG, f"Watching has been started: {self.Folder}")
		while True:
			try:
				time.sleep(1)
			except KeyboardInterrupt: 
				self.Log.write(LogTypes.ERROR, "Watcher stopped")
				watcher.stop()
				break
			except Exception as ex:
				self.Log.write(LogTypes.ERROR, ex)

	def start(self, queue, folder, logger):
		try:
			self.Folder = folder
			self.Queue = queue
			self.Log = logger
			self.watchFolder()
		except KeyboardInterrupt as ex:
			self.Log.write(LogTypes.ERROR, "Exiting watch process")
		except Exception as ex:
			self.Log.write(LogTypes.ERROR, ex)
