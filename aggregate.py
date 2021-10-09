import os
import re
from datetime import datetime
import configuration
import const
from logger import LogTypes

class Aggerate:

	def __init__(self):
		self.createfileinterval = 15
		self.filegeneratewaittime = 1
		self.querytimeinterval = 5
		self.causecodeinterval = 5
		self.cpsinterval = 1
		self.timelaps = dict()
		self.mindate = datetime.fromtimestamp(100000)
		self.maxdate = datetime.fromtimestamp(100000)
		self.outputfolder = ""
		self.outputfilepattern = ""
		self.nodename = ""
		self.log = None
		self.outputtypes = const.OUTPUT_TYPES
		self.querytimetitle = ""
		self.causecodetitle = ""
		self.cpsmetrictitle = ""
		self.lastkey = ""

	def setconfig(self, config:configuration.Config, logger):
		self.querytimeinterval = config.QueryTimeInterval
		self.causecodeinterval = config.CauseCodeInterval
		self.cpsinterval = config.CPSMetricInterval
		self.createfileinterval = config.FileCreateInterval
		self.outputfolder = config.OutputFolder
		self.outputfilepattern = config.OutputFilePattern
		self.nodename = config.NodeName
		self.log = logger
		self.querytimetitle = config.Kind.QueryTime
		self.causecodetitle = config.Kind.CauseCode
		self.cpsmetrictitle = config.Kind.CpsMetric
		if config.OutputTypes is None or len(config.OutputTypes) == 0:
			self.outputtypes = const.OUTPUT_TYPES
		else:
			self.outputtypes = config.OutputTypes
		
	def calculateKey(self, cdrtime, interval):
		key = cdrtime.strftime("%Y-%m-%d %H")
		minute = 0
		if interval > 0:
			minute = (cdrtime.minute // interval) * interval
		key = f"{key}:{minute:02}:00"
		return key

	def timelap(self, cdrtime, interval):
		timelap = cdrtime.strftime("%Y-%m-%d %H")
		minute = (cdrtime.minute // interval) * interval
		timelap = f"{timelap}:{minute:02}:00"
		return timelap

	def parsefilenameprefix(self, basefolder, pattern, key):
		prefix = pattern.replace("[NODE]", self.nodename)
		prefix = prefix.replace("[TIMESTAMP]", key)
		return os.path.join(basefolder, prefix)

	def createtmpfolder(self):
		try:
			folder = 'tmp'
			if os.path.exists(folder) == False:
				os.mkdir(folder)
			return folder
		except Exception as ex:
			self.log.write(LogTypes.ERROR, ex)

	def generate(self, cdrtime:datetime):
		if len(self.timelaps) > 0:
			generatedkeys = []
			for key in self.timelaps:
				if key != self.lastkey:
					diff = cdrtime.__sub__(self.timelaps[key][const.LAST_CDR_TIME]).total_seconds() // 60
					if diff >= self.filegeneratewaittime:
						self.createOutputFiles(key)
						self.deletetempfiles(key)
						generatedkeys.append(key)

			for key in generatedkeys:
				self.timelaps.__delitem__(key)

	def add(self, cdrtime, causecode, querytime):
		key = self.calculateKey(cdrtime, self.createfileinterval)
		if key not in self.timelaps:
			self.timelaps[key] = {
				const.QUERY_TIME_KEY: dict(),
				const.CAUSE_CODE_KEY: dict(),
				const.CPS_METRIC_KEY: dict(),
				const.LAST_CDR_TIME: datetime.fromtimestamp(100000)
		}
		self.timelaps[key][const.QUERY_TIME_KEY] = self.addToQueryTime(self.timelaps[key][const.QUERY_TIME_KEY], key, cdrtime, querytime)
		self.timelaps[key][const.CAUSE_CODE_KEY] = self.addToCauseCode(self.timelaps[key][const.CAUSE_CODE_KEY], key, cdrtime, causecode)
		self.timelaps[key][const.CPS_METRIC_KEY] = self.addToCpsMetric(self.timelaps[key][const.CPS_METRIC_KEY], key, cdrtime, querytime)
		self.timelaps[key][const.LAST_CDR_TIME] = cdrtime
		self.lastkey = key

	def addToQueryTime(self, data, key, cdrtime, querytime):
		timelap = self.timelap(cdrtime, self.querytimeinterval)
		if timelap not in data:
			data[timelap] = list()
		
		data[timelap].append(querytime)
		return data

	def addToCauseCode(self, data, key, cdrtime, causecode):
		timelap = self.timelap(cdrtime, self.causecodeinterval)
		if timelap not in data:
			data[timelap] = dict()

		codes = data[timelap]
		if causecode not in codes:
			codes[causecode] = 0
		codes[causecode] +=1
		data[timelap] = codes
		return data

	def addToCpsMetric(self, data, key, cdrtime, querytime):
		timelap = self.timelap(cdrtime, self.cpsinterval)
		if timelap not in data:
			data[timelap] = list()
		data[timelap].append(querytime)
		return data

	def createOutputFiles(self, key):
		timelap = self.timelaps[key]
		if timelap is not None:
			filename = self.parsefilenameprefix(self.outputfolder, self.outputfilepattern, key.replace("-", "").replace(":", ""))
			if const.OT_QUERY in self.outputtypes:
				self.writeQueryTimeData(filename, const.QUERY_TIME_DATA_PATTERN, timelap[const.QUERY_TIME_KEY], timelap[const.LAST_CDR_TIME], False)
			if const.OT_CODE in self.outputtypes:
				self.writeCauseCodeData(filename, const.CAUSE_CODE_DATA_PATTERN, timelap[const.CAUSE_CODE_KEY], timelap[const.LAST_CDR_TIME], False)
			if const.OT_CPS in self.outputtypes:
				self.writeCPSMetricData(filename, const.CPS_METRIC_DATA_PATTERN, timelap[const.CPS_METRIC_KEY], timelap[const.LAST_CDR_TIME], False)

	def writeToTempFile(self, cdrtime):
		key = self.calculateKey(cdrtime, self.createfileinterval)
		if key not in self.timelaps:
			return None
		timelap = self.timelaps[key]
		
		if timelap is not None:
			tmppath = self.createtmpfolder()
			if tmppath is not None:
				filename = self.parsefilenameprefix(tmppath, const.TMP_FILE_NAME_PATTERN, key.replace("-", "").replace(":", ""))
				self.writeQueryTimeData(filename, const.QUERY_TIME_TMP_DATA_PATTERN, timelap[const.QUERY_TIME_KEY], timelap[const.LAST_CDR_TIME], True)
				self.writeCauseCodeData(filename, const.CAUSE_CODE_TMP_DATA_PATTERN, timelap[const.CAUSE_CODE_KEY], timelap[const.LAST_CDR_TIME], True)
				self.writeCPSMetricData(filename, const.CPS_METRIC_TMP_DATA_PATTERN, timelap[const.CPS_METRIC_KEY], timelap[const.LAST_CDR_TIME], True)

	def generateTitle(self, title):
		title = title.replace("[NODE]", f"{const.NODENAME}")
		title = title.replace("[TIMESTAMP]", f"{const.TIMESTAMP}")
		title = title.replace("[AVERAGE]", f"{const.QUERYTIME}")
		title = title.replace("[CODE]", f"{const.CAUSECODE}")
		title = title.replace("[COUNT]", f"{const.CODECOUNT}")
		title = title.replace("[MIN]", f"{const.MINCPS}")
		title = title.replace("[MAX]", f"{const.MAXCPS}")
		title = title.replace("[AVG]", f"{const.AVGCPS}")
		return title

	def finish(self):
		for timelaps in self.timelaps:
			self.createOutputFiles(timelaps)
			self.deletetempfiles(timelaps)

	def writeQueryTimeData(self, filename:str, pattern:str, data:dict, lastcdrtime:datetime, totempfile:bool = False):
		filename = filename.replace("[KIND]", const.QUERY_TIME_KEY if totempfile else self.querytimetitle)
		fd = None
		try:
			fileContent = ""
			if totempfile == False:
				fileContent = self.generateTitle(const.QUERY_TIME_DATA_TITLE)
			else:
				fileContent = "{0}\n".format(lastcdrtime.strftime("%Y-%m-%d|%H:%M:%S"))
			for key in data:
				total = sum(data[key])
				count = len(data[key])
				content = pattern.replace("[NODE]", self.nodename)
				content = content.replace("[TIMESTAMP]", key)
				if totempfile == False:
					content = content.replace("[AVERAGE]", str(total // count))
				else:
					content = content.replace("[AVERAGE]", "|".join(map(str, data[key])))
				fileContent += content
			if len(fileContent) > 0:
				fd = open(filename, "w")
				if fd.writable():
					fd.write(fileContent)
					fd.flush()
		except Exception as ex:
			self.log.write(LogTypes.ERROR, ex)
		finally:
			if fd is not None and fd.closed == False:
				fd.close()

	def writeCauseCodeData(self, filename:str, pattern:str, data:dict, lastcdrtime:datetime, totempfile:bool = False):
		filename = filename.replace("[KIND]", const.CAUSE_CODE_KEY if totempfile else self.causecodetitle)
		fd = None
		try:
			fileContent = ""
			if totempfile == False:
				fileContent = self.generateTitle(const.CAUSE_CODE_DATA_TITLE)
			else:
				fileContent = "{0}\n".format(lastcdrtime.strftime("%Y-%m-%d|%H:%M:%S"))
			for key in data:
				for code in data[key]:
					content = pattern.replace("[NODE]", self.nodename)
					content = content.replace("[TIMESTAMP]", key)
					content = content.replace("[CODE]", str(code))
					content = content.replace("[COUNT]", str(data[key][code]))
					fileContent += content
			fd = open(filename, "w")
			if fd.writable():
				fd.write(fileContent)
				fd.flush()
		except Exception as ex:
			self.log.write(LogTypes.ERROR, ex)
		finally:
			if fd is not None and fd.closed == False:
				fd.close()

	def writeCPSMetricData(self, filename:str, pattern:str, data:dict, lastcdrtime:datetime, totempfile:bool = False):
		filename = filename.replace("[KIND]", const.CPS_METRIC_KEY if totempfile else self.cpsmetrictitle)
		fd = None
		try:
			fileContent = ""
			if totempfile == False:
				fileContent = self.generateTitle(const.CPS_METRIC_DATA_TITLE)
			else:
				fileContent = "{0}\n".format(lastcdrtime.strftime("%Y-%m-%d|%H:%M:%S"))
			mincps = 0
			maxcps = 0
			sumcps = 0
			lencps = 0
			for key in data:
				if totempfile == False:
					mincps = min(data[key])
					maxcps = max(data[key])
					sumcps = sum(data[key])
					lencps = len(data[key])
				content = pattern.replace("[NODE]", self.nodename)
				content = content.replace("[TIMESTAMP]", key)
				if totempfile == False:
					content = content.replace("[MIN]", str(mincps))
					content = content.replace("[MAX]", str(maxcps))
					content = content.replace("[AVG]", str(sumcps // lencps))
				else:
					content = content.replace("[MIN]", "")
					content = content.replace("[MAX]", "")
					content = content.replace("[AVG]", "|".join(map(str, data[key])))
				fileContent += content
			fd = open(filename, "w")
			if fd.writable():
				fd.write(fileContent)
				fd.flush()
		except Exception as ex:
			self.log.write(LogTypes.ERROR, ex)
		finally:
			if fd is not None and fd.closed == False:
				fd.close()

	def datetokey(self, filename):
		if len(filename) > 0:
			chars = list(filename)
			chars.insert(4, '-')
			chars.insert(7, '-')
			chars.insert(13, ':')
			chars.insert(16, ':')
			return ''.join(chars)

	def parsequerytime(self, data, line):
		isvalue = re.search(const.TIMESTAMP_REGEX, line)
		if isvalue is not None:
			line = line.strip()
			key = re.findall(const.TIMESTAMP_REGEX, line)[0]
			parts = re.split(const.TIMESTAMP_REGEX, line)
			if len(key) > 0 and len(parts) == 2:
				val = parts[1].strip()
				if key not in data:
					data[key] = list()
				data[key].extend(map(int, val.split("|")))
		else:
			data[const.LAST_CDR_TIME] = datetime.strptime(line.strip().replace("|", " "), "%Y-%m-%d %H:%M:%S")
		return data

	def parsecausecode(self, data, line):
		isvalue = re.search(const.TIMESTAMP_REGEX, line)
		if isvalue is not None:
			line = line.strip()
			key = re.findall(const.TIMESTAMP_REGEX, line)[0]
			parts = re.split(const.TIMESTAMP_REGEX, line)
			if len(key) > 0 and len(parts) == 2:
				parts = parts[1].split(",")
				parts = list(filter(lambda x: x, parts))
				code = int(parts[0].strip())
				count = int(parts[1].strip())
				if key not in data:
					data[key] = dict()
				if code not in data[key]:
					data[key][code] = 0
				data[key][code] = count
		else:
			data[const.LAST_CDR_TIME] = datetime.strptime(line.strip().replace("|", " "), "%Y-%m-%d %H:%M:%S")
		return data

	def parsecpsmetric(self, data, line):
		isvalue = re.search(const.TIMESTAMP_REGEX, line)
		if isvalue is not None:
			line = line.strip()
			key = re.findall(const.TIMESTAMP_REGEX, line)[0]
			parts = re.split(const.TIMESTAMP_REGEX, line)
			if len(key) > 0 and len(parts) == 2:
				val = parts[1].strip()
				if key not in data:
					data[key] = list()
				data[key].extend(map(int, val.split("|")))
		else:
			data[const.LAST_CDR_TIME] = datetime.strptime(line.strip().replace("|", " "), "%Y-%m-%d %H:%M:%S")
		return data

	def readtempfile(self, filename, kind):
		fd = None
		result = dict()
		try:            
			path = os.path.join('./tmp', filename)
			fd = open(path)
			if fd.readable():
				lines = fd.readlines()
				for line in lines:
					if kind == const.QUERY_TIME_KEY:
						result = self.parsequerytime(result, line)
					elif kind == const.CAUSE_CODE_KEY:
						result = self.parsecausecode(result, line)
					elif kind == const.CPS_METRIC_KEY:
						result = self.parsecpsmetric(result, line)
		except Exception as ex:
			self.log.write(LogTypes.ERROR, ex)
		finally:
			if fd is not None and fd.closed == False:
				fd.close()
		return result

	def loaddatafromtemp(self):
		try:
			path = "./tmp"
			if os.path.exists(path):
				files = os.listdir(path)
				files = list(sorted(files, key=lambda f: f.lower()))
				for file in files:
					if file.endswith('.tmp'):
						name = os.path.splitext(file)
						parts = name[0].split("_")
						if len(parts) == 2:
							date = parts[0]
							kind = parts[1]
							date = date.strip()
							kind = kind.strip().lower()
							key = self.datetokey(date)
							if key not in self.timelaps:
								self.timelaps[key] = {
									const.QUERY_TIME_KEY: dict(),
									const.CAUSE_CODE_KEY: dict(),
									const.CPS_METRIC_KEY: dict(),
									const.LAST_CDR_TIME: datetime.fromtimestamp(1000000)
								}
							if kind.__eq__(const.QUERY_TIME_KEY):
								self.timelaps[key][const.QUERY_TIME_KEY] = self.readtempfile(file, const.QUERY_TIME_KEY)
								if const.LAST_CDR_TIME in self.timelaps[key][const.QUERY_TIME_KEY]:
									self.timelaps[key][const.LAST_CDR_TIME] = self.timelaps[key][const.QUERY_TIME_KEY][const.LAST_CDR_TIME]
									self.timelaps[key][const.QUERY_TIME_KEY].__delitem__(const.LAST_CDR_TIME)
							elif kind.__eq__(const.CAUSE_CODE_KEY):
								self.timelaps[key][const.CAUSE_CODE_KEY] = self.readtempfile(file, const.CAUSE_CODE_KEY)
								if const.LAST_CDR_TIME in self.timelaps[key][const.CAUSE_CODE_KEY]:
									self.timelaps[key][const.LAST_CDR_TIME] = self.timelaps[key][const.CAUSE_CODE_KEY][const.LAST_CDR_TIME]
									self.timelaps[key][const.CAUSE_CODE_KEY].__delitem__(const.LAST_CDR_TIME)
							elif kind.__eq__(const.CPS_METRIC_KEY):
								self.timelaps[key][const.CPS_METRIC_KEY] = self.readtempfile(file, const.CPS_METRIC_KEY)
								if const.LAST_CDR_TIME in self.timelaps[key][const.CPS_METRIC_KEY]:
									self.timelaps[key][const.LAST_CDR_TIME] = self.timelaps[key][const.CPS_METRIC_KEY][const.LAST_CDR_TIME]
									self.timelaps[key][const.CPS_METRIC_KEY].__delitem__(const.LAST_CDR_TIME)

		except Exception as ex:
			self.log.write(LogTypes.ERROR, ex)

	def deletetempfiles(self, key):
		if len(key) > 0:
			kinds = [const.QUERY_TIME_KEY, const.CAUSE_CODE_KEY, const.CPS_METRIC_KEY]
			path = "./tmp"
			filename = self.parsefilenameprefix(path, const.TMP_FILE_NAME_PATTERN, key.replace("-", "").replace(":", ""))
			for kind in kinds:
				try:
					path = filename.replace('[KIND]', kind) 
					if os.path.exists(path) and os.path.isfile(path):
						os.remove(path)        
				except Exception as ex:
					self.log.write(LogTypes.ERROR, ex)

