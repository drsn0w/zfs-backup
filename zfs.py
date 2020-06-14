import logging,coloredlogs
import libzfs_core as libzfs
import libzfs_core.exceptions as lzce
import subprocess
import sys, os, threading, io
coloredlogs.install(level='DEBUG')
SEND_RECV_LOCK = threading.Lock()

def checkExists(datasetName):
    datasetNameB = str.encode(datasetName)
    return(libzfs.lzc_exists(datasetNameB))

def create(datasetName):
    datasetNameB = str.encode(datasetName)
    try:
        libzfs.lzc_create(datasetNameB)
    except lzce.FilesystemExists:
        logging.fatal(datasetName + " already exists! Exiting.")
        sys.exit(1)
    except (lzce.NameInvalid, lzce.NameTooLong):
        logging.fatal(datasetName + " is an invalid dataset name!")
        sys.exit(1)

def snapshot(datasetName, snapshotName):
    logging.debug("making snapshot " + snapshotName + " in " + datasetName)
    snapLongName = datasetName + "@" + snapshotName
    snapLongNameB = str.encode(snapLongName)
    try:
        libzfs.lzc_snapshot([snapLongNameB])
    except lzce.SnapshotFailure as bork:
        logging.fatal("unable to create snapshot: " + bork.message + "! Exiting.")
        sys.exit(1)

def sendLocal(datasetName, snapshotName, destDataset):
    logging.debug("sending " + datasetName + "@" + snapshotName + " to " + destDataset)
    logging.warning("using unsafe method here! please update with libzfs if you know how!")
    snapLongName = datasetName + "@" + snapshotName
    if checkExists(destDataset):
        logging.fatal(snapLongName + " already exists! Exiting.")
        sys.exit(1)
    else:
        ps = subprocess.Popen(('zfs', 'send', snapLongName), stdout=subprocess.PIPE)
        recvCode = subprocess.call(('zfs', 'recv', destDataset), stdin=ps.stdout)
        ps.wait()
        if recvCode != 0:
            logging.fatal("an unexpected error occured! Exiting.")
            sys.exit(1)