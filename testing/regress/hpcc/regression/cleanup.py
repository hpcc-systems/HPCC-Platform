import logging
import os
import time
import glob
from datetime import datetime,  timedelta

# Configure logger
logger = logging.getLogger('cleanup') 

try:
    import requests
except ImportError:
    requests = None
    logger = logging.getLogger('RegressionTestEngine')

# Custom logger formatter for serial numbering of log records
class SerialNumberFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.serialNumber = 0

    def format(self, record):
        self.serialNumber += 1
        record.msg = str(self.serialNumber) + ". " + record.msg
        return super().format(record)

# Builds and configures the cleanup logger
def buildCleanupLogger(logDir, cleanupLogger):
    cleanupLogger.setLevel(logging.INFO)
    curTime = time.strftime('%y-%m-%d-%H-%M-%S')
    logName = "cleanup" + "." + curTime + ".log"
    logPath = os.path.join(logDir, logName)
    cleanupHandler = logging.FileHandler(logPath)
    cleanupHandler.setFormatter(SerialNumberFormatter())
    cleanupLogger.addHandler(cleanupHandler)
    return cleanupLogger

# Extracts regress log files corresponding to current run of RTE
def getRegressLogs(mode, logDirPath, startTime):
    requiredStrings = ['thor', 'roxie', 'hthor', 'roxie-workunit']
    exclusionStrings = ['exclusion']  
   
    logDirPath = logDirPath + "/*.%02d-%02d-%02d-*.log"
    logDirPathStartDay = logDirPath % (startTime.year-2000,  startTime.month,  startTime.day)
    fileNames = glob.glob(logDirPathStartDay)

    nextDay = startTime + timedelta(days = 1)
    logDirPathNextDay = logDirPath % (nextDay.year-2000,  nextDay.month,  nextDay.day)
    fileNames += glob.glob(logDirPathNextDay)

    for fileName in fileNames:
        if any(string in fileName for string in exclusionStrings):
            continue

        if any(string in fileName for string in requiredStrings):
            fileTime = datetime.fromtimestamp(os.path.getmtime(fileName))
            if fileTime >= startTime:
                getWorkunitDetails(fileName, mode)
        
# Extracts workunit details from log file based on the cleanup mode
def getWorkunitDetails(logFilePath, mode):
    with open(logFilePath, 'r') as file:
        lineList = file.readlines()
        def extractUrl(line):
            index = line.find("URL")
            if index != -1:
                return line[index + 3:-1]
            else:
                return None
        if mode == "workunits":
            for line in lineList:
                url = extractUrl(line) 
                if url:
                    deleteWorkunit(url)
        elif mode == "passed":
            for i, line in enumerate(lineList):
                if "pass" in line.lower():
                    nextLine = lineList[i + 1]
                    url = extractUrl(nextLine)
                    if url:
                        deleteWorkunit(url)
                    
# Deletes workunits
def deleteWorkunit(url):
    deletionUrl = url.replace("?Widget=WUDetailsWidget&Wuid", "WsWorkunits/WUDelete.json?Wuids")
    startIndex = url.find('Wuid=')
    wuid = url[startIndex:]
    try:
        response = requests.post(deletionUrl)
        jsonResponse = response.json()
        if response.status_code == 200:
            if jsonResponse.get("WUDeleteResponse") == {}:
                logger.info("Workunit %s deleted successfully.", wuid)
            elif "ActionResults" in jsonResponse["WUDeleteResponse"]:
                errorMessage = jsonResponse["WUDeleteResponse"]["ActionResults"]["WUActionResult"][0]["Result"]
                logger.error("Failed to delete workunit %s.\n   URL:%s\n   %s Response status code: %d", wuid, url, errorMessage, response.status_code)
        else:
            logger.error("Failed to delete workunit %s.\n   URL:%s\n   Response status code: %d", wuid, url, response.status_code)
    except requests.exceptions.RequestException as e:
        logger.error("Error occurred while deleting workunit %s: %s.\n   URL: %s", wuid, str(e), url)

# Main function to instantiate custom logger and initiate workunit detail extraction
def doCleanup(logDir, cleanupMode, startTime):
    if requests != None:
        logDirPath = os.path.expanduser(logDir)
        buildCleanupLogger(logDirPath, logger)
        getRegressLogs(cleanupMode, logDirPath, startTime)
    else:
        logger.warning("The 'requests' library not found, clean-up functionality disabled.\n")

