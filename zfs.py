import logging,coloredlogs
import libzfs_core as libzfs
import libzfs_core.exceptions as lzce
import subprocess
import sys, os, threading
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

def _zfs_send_threaded(snb, fd):
    try:
        libzfs.lzc_send(snb, None, fd)
    except Exception as bork:
        logging.fatal(bork)

def _zfs_recv_threaded(sdnb, fd):
    try:
        libzfs.lzc_receive(sdnb, fd)
    except Exception as bork:
        logging.fatal(bork)


def sendLocal(datasetName, snapshotName, destDataset):
    logging.debug("sending " + datasetName + "@" + snapshotName + " to " + destDataset)
    snapLongName = datasetName + "@" + snapshotName
    snapLongNameB = str.encode(snapLongName)
    snapLongDest = destDataset + "@" + snapshotName
    snapLongDestB = str.encode(snapLongDest)
    datasetNameB = str.encode(datasetName)
    snapshotNameB = str.encode(snapshotName)
    destDatasetB = str.encode(destDataset)

    zfdw, zfdr = os.pipe()
    logging.debug("zfdw: " + str(zfdw) + " zfdr: " + str(zfdr))
    libzfs.lzc_send(snapLongNameB, None, zfdw)
    libzfs.lzc_receive(snapLongDestB, zfdr)
    
