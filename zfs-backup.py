#!/usr/bin/python3
import coloredlogs,logging
coloredlogs.install(level='DEBUG')
import sys
import configparser
import datetime
import zfs

# Read configuration files
cfg = configparser.ConfigParser()
cfg.read("test.cfg")
configLength = len(cfg.sections())
numDatasets = configLength - 1
logging.debug("configLengthlength is " + str(configLength))
logging.info("datasets to process: " + str(numDatasets))

# Process config file
datasetsToProcess = []
for x in cfg.sections():
    if x == "ZFSBackupGlobal":
        logging.debug("not processing global config as dataset")
    else:
        if zfs.checkExists(x):
            logging.debug(x + " exists!")
            datasetsToProcess.append(x)
        else:
            logging.error(x + " does not exist. Ignoring.")

logging.info("Datasets to Process")
for x in datasetsToProcess:
    logging.info("Pool Name: " + x)
    logging.info("      Backup Pool: " + cfg[x]["backuppool"])
    logging.info("      Encrypted Backup: " + cfg[x]["encrypted"])
    logging.info("      Backup Frequency: " +cfg[x]["frequency"])

# Process datasets
for x in datasetsToProcess:
    logging.info("Processsing dataset " + x)
    dsBackupPool = cfg[x]['backuppool']
    dsBackupSet = dsBackupPool + "/" + x
    logging.debug("dsBackupPool is " + dsBackupPool)
    dsEncryptBackup = cfg[x]['encrypted']
    dsBackupFreq = cfg[x]['frequency']
    dsFirstRun = False

    # Check if backup dataset exists
    if zfs.checkExists(dsBackupSet):
        logging.debug(dsBackupSet + " exists!")
    else:
        logging.warning(dsBackupSet + " does not exist!")
        if not zfs.checkExists(dsBackupPool):
            logging.fatal(dsBackupPool + " does not exist. Please create " + dsBackupPool + " zpool on your chosen backup device.")
            sys.exit(1)

        dsFirstRun = True

    if dsFirstRun:
        logging.warning("This appears to be the first run of zfs-backup for " + x +". Is this correct? [y/N] ")
        newAnswer = input()
        if newAnswer == "y" or newAnswer == "Y":
            logging.debug("proceeding as first run")
        elif newAnswer == "n" or newAnswer == "N" or newAnswer == "":
            logging.fatal("Please check your storage devices and run zfs-backup again! Exiting.")
            sys.exit(1)
        else:
            logging.fatal("Unknown input! Exiting.")
            sys.exit(1)

        snapshotName = "zback-" + datetime.datetime.now().strftime("%b%d%Y-%H%M%S")
        #snapshotName = "borktest"
        logging.debug("snapshot name will be: " + x + "@" + snapshotName)
        logging.info("creating initial snapshot...")
        zfs.snapshot(x, snapshotName)
        logging.info("snapshot created!")
        logging.info("sending snapshot to " + dsBackupSet)
        zfs.sendLocal(x, snapshotName, dsBackupSet)
        logging.info("initial snapshot of " + x + "@" + snapshotName +" created!")
