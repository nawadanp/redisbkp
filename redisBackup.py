#!/usr/bin/python

import redis
from time import sleep
from sys import exit
import datetime
import os

# Global variables
REDIS_INSTANCES = (('127.0.0.1', 6379), ('127.0.0.2', 6380))
FAILED_BACKUP = []
ROOT_BACKUP = "/data/redis/backups"
LOCK_FILE = "/var/run/redisBackup.lock"
RSYNC_BACKEND = "redis_backups"


def get_lock():
    if os.path.isfile(LOCK_FILE):
        print("Lock file already present, exit script")
        exit(2)
    else:
        os.mknod(LOCK_FILE)
    return


def release_lock():
    os.remove(LOCK_FILE)
    return


def is_redis_available(r):
    """
    Ensure redis instance is reachable and not in master status
    :param r: redis connection object
    :return: True/False
    """

    try:
        role = r.info()['role']
    except redis.exceptions.ConnectionError:
        print_log(INSTANCE_ADDR, INSTANCE_PORT, "Unable to connect, skipping instance")
        return False
    if role != "slave":
        print_log(INSTANCE_ADDR, INSTANCE_PORT, "Not slave, skipping instance")
        return False
    return True


def create_backup_directory():
    """
    Create directory tree with ROOT/YEAR/MONTH/DAY/HOUR/MINUTE
    :return: backup directory path
    """
    now = datetime.datetime.now()
    backup_directory = ROOT_BACKUP + "/%d/%d/%d/%d/%d" % (now.year, now.month, now.day, now.hour, now.minute)

    if not os.path.exists(backup_directory):
        os.makedirs(backup_directory)
    return backup_directory


def is_backup_running(r):
    """
    Check if a RDB backup is in progree
    :param r: redis connection object
    :return: True/False
    """
    if r.info()['rdb_bgsave_in_progress'] != 0:
        return True
    return False


def print_log(address, port, msg):
    print("[%s:%d] %s" % (address, port, msg))

if "__main__" == __name__:
    get_lock()  # Ensure no other process is running
    BACKUP_DIRECTORY = create_backup_directory()

    for INSTANCE in REDIS_INSTANCES:
        INSTANCE_ADDR = INSTANCE[0]
        INSTANCE_PORT = INSTANCE[1]

        it = 0
        r = redis.Redis(host=INSTANCE_ADDR, port=INSTANCE_PORT)

        if not is_redis_available(r):
            FAILED_BACKUP.append("%s:%d" % (INSTANCE_ADDR, INSTANCE_PORT))
            continue

        # Get redis root directory
        infoDir = r.config_get("dir")
        dump_dir = infoDir['dir'] + "dump.rdb"

        while is_backup_running(r):
            if it <= 30:
                print_log(INSTANCE_ADDR, INSTANCE_PORT, "Another dump is already in progress, please wait %d/30" % it)
                it += 1
                sleep(1)
            else:
                print_log(INSTANCE_ADDR, INSTANCE_PORT,
                          "Another dump is already in progress, too many retries, skipping...")
                FAILED_BACKUP.append("%s:%d" % (INSTANCE_ADDR, INSTANCE_PORT))
                continue

        print_log(INSTANCE_ADDR, INSTANCE_PORT, "Start backgroup dump")
        r.bgsave()
        sleep(1)

        it = 0
        while is_backup_running(r):
            if it <= 30:
                print_log(INSTANCE_ADDR, INSTANCE_PORT, "Dump in progress, please wait %d/30" % it)
                it += 1
                sleep(5)
            else:
                print_log(INSTANCE_ADDR, INSTANCE_PORT, "Dump took too many time, skipping...")
                FAILED_BACKUP.append("%s:%d" % (INSTANCE_ADDR, INSTANCE_PORT))
                continue
        else:
            print_log(INSTANCE_ADDR, INSTANCE_PORT, "Dump done")

        os.system("rsync -t %s::%s/%d/dump.rdb %s/%s_%d.rdb"
                  % (INSTANCE_ADDR, RSYNC_BACKEND, INSTANCE_PORT, BACKUP_DIRECTORY, INSTANCE_ADDR, INSTANCE_PORT))

    release_lock()
    exit()
