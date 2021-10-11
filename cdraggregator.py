import os
import sys
import time
import datetime
import aggregate
import threading
from pathlib import Path
from multiprocessing import Queue
import watcher
import configuration
import const
from logger import Logger, LogTypes, OutputTargets

log = Logger()
config = configuration.Config()
watchfolder = watcher.Watcher()
queue = Queue()
aggr = aggregate.Aggerate()

def process(lines, cdrtime):
    global aggr
    global log
    try:
        for line in lines:
            data = line.strip().split(',')
            length = len(data)
            cdrtime = datetime.datetime.strptime(data[1], "%Y-%m-%d %H:%M:%S")
            code = int(data[length-2])
            querytime = int(data[length-1])
            aggr.add(cdrtime, code, querytime)
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)
    return cdrtime

def readfile(filename, cdrtime):
    fd = None
    global config
    global aggr
    global log
    while True:
        try:
            fd = open(filename, 'r')
            lines = fd.readlines()
            log.write(LogTypes.DEBUG, f"{len(lines)} line(s) read")
            cdrtime = process(lines, cdrtime)
            break
        except FileNotFoundError as ex:
            log.write(LogTypes.ERROR, ex)
            break
        except OSError as ex:
            log.write(LogTypes.ERROR, ex)
            time.sleep(5)
        except Exception as ex:
            log.write(LogTypes.ERROR, ex)
            time.sleep(5)
        finally:
            if fd is not None and fd.closed == False:
                fd.close()
    return cdrtime

def processpendingfiles(ondemandprocess:bool = False):
    global aggr
    global queue
    global log
    cdrtime = datetime.datetime.now()
    try:
        while True:
            if not queue.empty():
                file = queue.get()
                log.write(LogTypes.DEBUG, f"File being processing: {file}")
                cdrtime = readfile(file, cdrtime)
                log.write(LogTypes.DEBUG, f"Files process finished: {file}")
                updatedatfile(file)
                aggr.generate(cdrtime)
                if not ondemandprocess:
                    aggr.writeToTempFile(cdrtime)
            else:
                if ondemandprocess:
                    aggr.finish()
                    break
                time.sleep(10)
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)

def start():
    global config
    global queue
    global log
    try:
        watch = threading.Thread(target=watchfolder.start, args=([queue, config.InputFolder,log]))
        watch.start()
        process = threading.Thread(target=processpendingfiles)
        process.start()
    except KeyboardInterrupt as ex:
        log.write(LogTypes.ERROR, "Process interrupted by user")
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)

def getlastreadfiledate():
    fd = None
    date = 0
    name = ""
    try:
        fd = open(const.DAT_FILE_NAME)
        if fd.readable():
            content = fd.readline()
            if len(content) > 1:
                data = content.split("|")
                date = int(data[0])
                name = data[1]
                log.write(LogTypes.DEBUG, f"Last read file is: {name}")
            else:
                log.write(LogTypes.DEBUG, "Could not detect last processed file. All files in folder will be enqueue to processed")
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)
    finally:
        if fd is not None:
            fd.close()
    return (date, name)

def checkexistingfiles():
    try:
        global queue
        global config
        global log
        date, name = getlastreadfiledate()
        cdrfiles = dict()
        with os.scandir(config.InputFolder) as files:
            for file in files:
                if file.name.endswith('.cdr'):
                    stats = file.stat(follow_symlinks=False)
                    if stats.st_ctime_ns > date and file.name.__eq__(name) == False:
                        cdrfiles[file.name] = stats.st_ctime_ns
        cdrfiles = list(sorted(cdrfiles.items(), key=lambda t: t[1]))
        for file in cdrfiles:
            queue.put(os.path.join(config.InputFolder, file[0]))
        filecount = len(cdrfiles)
        if filecount > 0:
            log.write(LogTypes.DEBUG, f"{filecount} new file(s) detected which is created after last running time. All of them added to queue to process")
        else:
            log.write(LogTypes.DEBUG, f"There is no new files which pending to process at: {config.InputFolder}")
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)

def updatedatfile(filename):
    global log
    fd = None
    try:
        fd = open(const.DAT_FILE_NAME, "w")
        fileinfo = os.stat(filename)
        content = const.DAT_FILE_FORMAT.replace("[DATE]", str(fileinfo.st_ctime_ns))
        content = content.replace("[NAME]", filename)
        if fd.writable():
            fd.write(content)
            fd.flush()
            log.write(LogTypes.DEBUG, f"Last processed file updated. File name: {filename}")
        else:
            log.write(LogTypes.ERROR, f"Last processed file could not update. File is not writable. Possible there is no write permission. File name: {filename}")
    except FileNotFoundError as ex:
        fd = None
    except Exception as ex:
        log.write(LogTypes.ERROR, ex)
    finally:
        if fd is not None:
            fd.close()

def printgreeting():
    global config
    mode = "watch" if not config.OnDemandProcess else "test"
    log.write(LogTypes.DEBUG, f"Process has been started with parameters below in {mode} mode:")
    log.write(LogTypes.DEBUG, f"New files will be created in per {config.FileCreateInterval} minute(s)")
    log.write(LogTypes.DEBUG, f"Query times data interval is: {config.QueryTimeInterval} minute(s)")
    log.write(LogTypes.DEBUG, f"Cause codes data interval is: {config.CauseCodeInterval} minute(s)")
    log.write(LogTypes.DEBUG, f"CPS metrics data interval is: {config.CPSMetricInterval} minute(s)")

def init():
    global config
    global aggr
    global log
    error = config.load('config.json')
    if error is not None and len(error) > 0:
        log.write(LogTypes.ERROR, error)
        sys.exit(-1)
    log.changeoutputfolder(config.LogFolder)
    aggr.loaddatafromtemp()
    error = config.validate()
    if config.OnDemandProcess:
        log.changeoutput(OutputTargets.FILE)
    aggr.setconfig(config, log)
    if error is  None or len(error) == 0:
        printgreeting()
        if not config.OnDemandProcess:
            checkexistingfiles()
        return True
    else:
        log.write(LogTypes.ERROR, error)
        sys.exit(-1)

def startondemand():
    filecount = 0
    for path in Path(config.InputFolder).glob(config.InputFilePattern):
        queue.put(path)
        filecount += 1

    if not queue.empty():
        log.write(LogTypes.DEBUG, f"{filecount} file(s) found and enqueued")
        print(f"{filecount} file(s) found and enqueued")
        processpendingfiles(True)
        log.write(LogTypes.DEBUG, f"{filecount} file(s) processed. You can find log file on {config.LogFolder}")
        print(f"{filecount} file(s) processed. You can find log file on {config.LogFolder}")
    else:
        log.write(LogTypes.ERROR, f"No file found on the folder: {config.InputFolder}. Search Param: {config.InputFilePattern}")

def main():
    global  config
    if config.OnDemandProcess:
        startondemand()
    else:
        start()

if __name__ == '__main__':
    if init():
        main()