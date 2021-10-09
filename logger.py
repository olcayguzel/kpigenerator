import os
import enum
import datetime
import sys
import const

class LogTypes(enum.Enum):
	INFO = 1
	DEBUG = 2
	ERROR = 3
	FATAL = 4

class OutputTargets(enum.Enum):
	FILE = 0,
	STDOUTPUT = 1
	BOTH = 2

class Logger:
	pass
	def __init__(self):
		self.pwd = os.getcwd()
		self.filename = "kpigenerator.log"
		self.OutputTo:OutputTargets = OutputTargets.BOTH # 0 -> FILE		1-> STD OUTPUT

	def changeoutput(self, target:OutputTargets):
		if target == OutputTargets.STDOUTPUT:
			self.OutputTo = OutputTargets.STDOUTPUT.value
		elif target == OutputTargets.FILE:
			self.OutputTo = OutputTargets.FILE.value
		elif target == OutputTargets.BOTH:
			self.OutputTo = OutputTargets.BOTH.value
		else:
			self.write(LogTypes.ERROR, "Unsupported output target")

	def createfolder(self, folder):
		try:
			if os.path.exists(os.path.join(self.pwd, folder)) == False:
				os.mkdir(os.path.join(self.pwd, folder))
		except Exception as ex:
			print(ex)

	def write(self, logtype:LogTypes, message):
		self.createfolder(const.LOG_FOLDER_PATH)
		now = datetime.datetime.now()
		logtime = now.strftime("%Y-%m-%d %H:%M:%S")
		logmessage = const.LOG_PATTERN.replace("[TIME]", logtime)
		logmessage = logmessage.replace("[TYPE]", logtype.name)
		logmessage = logmessage.replace("[MESSAGE]", str(message))

		if self.OutputTo == OutputTargets.FILE:
			self.writeToFile(logmessage)
		elif self.OutputTo == OutputTargets.STDOUTPUT:
			if sys.stderr.buffer.writable():
				sys.stderr.buffer.write(bytes(logmessage.encode("utf-8")))
				sys.stderr.buffer.flush()
		elif self.OutputTo == OutputTargets.BOTH:
			self.writeToFile(logmessage)
			if sys.stderr.buffer.writable():
				sys.stderr.buffer.write(bytes(logmessage.encode("utf-8")))
				sys.stderr.buffer.flush()

	def checklogfile(self):
		path = os.path.join(self.pwd, const.LOG_FOLDER_PATH, self.filename)
		if os.path.exists(path):
			stat = os.stat(path)
			size = 0
			if stat is not None:
				size = stat.st_size
				size = size / 1024 / 2024
			if size >= const.MAX_LOG_FILE_SIZE:
				now = datetime.datetime.now()
				os.rename(path, os.path.join(self.pwd, const.LOG_FOLDER_PATH, now.strftime("%Y%m%d_%H%M%S") + ".log"))

	def writeToFile(self, message:str):
		fd = None
		try:
			self.checklogfile()
			fd = open(os.path.join(self.pwd, const.LOG_FOLDER_PATH, self.filename), "a")
			if fd.writable():
				fd.write(message)
				fd.flush()
		except Exception as ex:
			print(ex)
		finally:
			if fd is not None and fd.closed == False:
				fd.close()
