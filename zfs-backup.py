#!/usr/bin/python3
import coloredlogs,logging
coloredlogs.install(level='DEBUG')
import sys
import configparser
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
    dsBackupPool = cfg[x]['backuppool'] + "/" + x
    logging.debug("dsBackupPool is " + dsBackupPool)
    dsEncryptBackup = cfg[x]['encrypted']
    dsBackupFreq = cfg[x]['frequency']
    dsFirstRun = False

    # Check if backup dataset exists
    if zfs.checkExists(dsBackupPool):
        logging.debug(dsBackupPool + " exists!")
    else:
        logging.warning(dsBackupPool + " does not exist and will need to be created.")
        dsFirstRun = True
        zfs.create(dsBackupPool)
