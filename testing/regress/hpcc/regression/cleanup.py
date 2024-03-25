import logging
import os
import re
import requests
import json
import time
import glob

logger = logging.getLogger('cleanup') 

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

# Loads configuration data from JSON file
def loadConfig(filename):
    with open(filename, 'r') as file:
        return json.load(file)

# Extracts workunit details based on the cleanup mode
def getRegressLogs(mode, logDirPath):
    regressLogFiles = glob.glob(os.path.join(logDirPath, '*.log'))   
    for logFilePath in regressLogFiles:
        logFileNamePattern = r'^(thor|roxie|hthor)\.\d{2}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.log(?!.*exclusion)'
        match = re.search(logFileNamePattern, os.path.basename(logFilePath))
        if match:
            if mode == "all" or mode == "workunits":
                getWorkunitDetails(logFilePath)
            elif mode == "passed":
                getPassedWorkunitDetails(logFilePath)

# Extracts workunit details from log file
def getWorkunitDetails(logFilePath):
    with open(logFilePath, 'r') as file:
        lineList = file.readlines()
        for line in lineList:
            index = line.find("URL")
            if index != -1:
                url = line[index + 3:-1]
                startIndex = url.find('Wuid=')
                if startIndex != -1:
                    wuid = url[startIndex:]
                    deleteWorkunit(url, wuid)

# Extracts workunit details from log file for passed test cases
def getPassedWorkunitDetails(logFilePath):
    with open(logFilePath, 'r') as file:
        lineList = file.readlines()
        for i, line in enumerate(lineList):
            if re.search(r'\bpass\b', line, re.IGNORECASE):
                nextLine = lineList[i + 1]
                index = nextLine.find("URL")
                if index != -1:
                    url = nextLine[index + 3:-1]
                    startIndex = url.find('Wuid=')
                    if startIndex != -1:
                        wuid = url[startIndex:]
                        deleteWorkunit(url, wuid)

# Deletes workunits
def deleteWorkunit(url, wuid):
    deletionUrl = url.replace("?Widget=WUDetailsWidget&Wuid", "WsWorkunits/WUDelete.json?Wuids")
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
def checkCleanupMode(cleanupMode):
    configData = loadConfig('ecl-test.json')
    logDirPath = os.path.expanduser(configData['Regress']['logDir'])
    buildCleanupLogger(logDirPath, logger)

    if cleanupMode in ['all', 'passed', 'workunits']:
        getRegressLogs(cleanupMode, logDirPath)