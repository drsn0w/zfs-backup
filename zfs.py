import logging,coloredlogs
import subprocess
import sys
coloredlogs.install(level='DEBUG')

def checkExists(datasetName):
    logging.debug("checkExists checking for dataset " + datasetName)
    datasetsByte = subprocess.check_output(['zfs', 'list','-H'])
    datasets = datasetsByte.decode("utf-8")
    if datasetName in datasets:
        logging.debug("checkExists found " + datasetName)
        return True
    else:
        logging.debug("checkExists did not find " + datasetName)
        return False

def checkExistsSnapshot(datasetName):
    logging.debug("checkExists checking for dataset " + datasetName)
    datasetsByte = subprocess.check_output(['zfs', 'list', '-t snapshot''-H'])
    datasets = datasetsByte.decode("utf-8")
    if datasetName in datasets:
        logging.debug("checkExists found " + datasetName)
        return True
    else:
        logging.debug("checkExists did not find " + datasetName)
        return False

def create(datasetName):
    if checkExists(datasetName):
        logging.fatal(datasetName + " already exists! Exiting.")
        sys.exit(1)
    else:
        logging.debug("creating dataset " + datasetName)
        zfsReturnCode = subprocess.call(['zfs', 'create', datasetName])
        if zfsReturnCode != 0:
            logging.fatal("zfs create failed!")
            sys.exit(1)
            
def sendLocal(sourceSnapshot, destDataset):
    if not checkExistsSnapshot(sourceSnapshot):   
        logging.fatal(sourceSnapshot + " does not exist! Exiting.")
        sys.exit(1)

    if not checkExists(destDataset):
        logging.fatal(destDataset + "does not exist! Exiting.")
        sys.exit(1)

def sendRemote(sourceSnapshot, destHost, destDataset):
        if not checkExistsSnapshot(sourceSnapshot):   
            logging.fatal(sourceSnapshot + " does not exist! Exiting.")
            sys.exit(1)