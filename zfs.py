import logging,coloredlogs
import libzfs_core as libzfs
import libzfs_core.exceptions as lzce
import subprocess
import sys
coloredlogs.install(level='DEBUG')

def checkExists(datasetName):
    datasetNameB = str.encode(datasetName)
    return(libzfs.lzc_exists(datasetNameB))

def create(datasetName):
    datasetNameB = str.encode(datasetName)
    try:
        libzfs.lzc_create(datasetNameB)
    except lzce.FilesystemExists:
        logging.fatal(datasetName + " already exists! Exiting.")
        exit(1)
    except (lzce.NameInvalid, lzce.NameTooLong):
        logging.fatal(datasetName + " is an invalid dataset name!")

def snapshot(datasetName, snapshotName):
    logging.debug("making snapshot " + snapshotName + " in " + datasetName)

def sendLocal(sourceSnapshot, destDataset):
    logging.debug("sending " + sourceSnapshot + " to " + destDataset)
