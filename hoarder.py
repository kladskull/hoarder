# /usr/bin/env python3

import os
import os.path
import sqlite3
import hashlib
import gzip
import shutil
import tempfile
import string
import random

from datetime import datetime

excludes = {
    "/Users/mcurry/projects/backup/.idea": True,
    "/Users/mcurry/projects/backup/venv": True,
    "/proc": True,
    "/tmp": True,
    "/mnt": True,
    "/dev": True,
    "/sys": True,
    "/run": True,
    "/media": True,
    "/var/cache/apt/archives": True,
}

"""
# exclude self
# omit  /proc, /boot & /dev /sys
# find . -type f -mtime -1 -exec ls -l {} \;
# find -P -ignore_readdir_race  . -type f -mtime -1 -exec ls -l {} \;

# find -P -ignore_readdir_race  . -type f \( -path dir1 -o -path dir2 -o -path dir3 \) -prune -o -mtime -1 -exec ls -l {} \;

"""


def create_schema():
    conn = sqlite3.connect('backup.db')

    # Create backup table
    try:
        conn.execute("""
            CREATE TABLE backups (
              last_backed_up INTEGER PRIMARY KEY,
              files_backed_up INTEGER
            )
        """)
    except:
        pass

    # Create file table
    try:
        conn.execute("""
            CREATE TABLE fs_objects (
              file_hash TEXT PRIMARY KEY, 
              file_name TEXT,
              file_path TEXT,
              inode INTEGER,
              dir INTEGER,
              `file` INTEGER,
              link INTEGER,
              stat_mode INTEGER,
              stat_dev INTEGER,
              stat_nlink INTEGER,
              st_uid INTEGER,
              st_gid INTEGER,
              st_size INTEGER,
              st_atime REAL,
              st_mtime REAL,
              st_ctime REAL,
              st_blocks INTEGER,
              st_blksize INTEGER,
              st_rdev INTEGER,
              st_flags INTEGER,
              st_gen INTEGER,
              st_birthtime REAL
            )
        """)
    except:
        pass

    conn.close()


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_paths(dir_path):
    directories = []

    for entry in os.scandir(dir_path):
        # full_path = os.path.join(entry.path)
        if entry.is_dir():
            if entry.path in excludes:
                # print("skipping: " + entry.path)
                continue

            # add directory holder
            if os.path.islink(dir_path):
                continue
            directories.append(entry.path)
            directories = directories + get_paths(entry.path)

    return directories


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def create_temp_dir():
    # check for errors!
    tmp_path = tempfile.gettempdir() + "/" + id_generator()
    os.mkdir(tmp_path)
    return tmp_path + "/"


def get_path_objects(path, tmp_path):
    # print("processing: " + path)
    conn = sqlite3.connect('backup.db')

    dupes = 0
    insert_counter = 0
    data = ""
    for entry in os.scandir(path):
        full_path = entry.path
        if entry.is_file():
            key = md5(full_path)
        else:
            key = hashlib.md5(full_path.encode()).hexdigest()

        if entry.is_dir():
            #d.print("Directory => " + entry.path)
            if entry.path in excludes:
                print("skipping: " + entry.path)
                continue

        first_backup = True
        p_st_atime = 0
        p_st_mtime = 0
        p_st_ctime = 0
        p_st_size = 0
        try:
            fields = "st_atime,st_mtime,st_ctime,st_size"
            cursor = conn.execute("select " + fields + " from fs_objects where file_hash='" + key + "' limit 1")
            for row in cursor:
                first_backup = False
                p_st_atime = row[0]
                p_st_mtime = row[1]
                p_st_ctime = row[2]
                p_st_size = row[3]

        # except:
        except Exception as e:
            print("no 0: " + str(e))
            # pass

        # print(str(entry.stat().st_atime) + " -> " + str(p_st_atime))

        if not first_backup and not entry.is_symlink():
            if entry.stat().st_atime == p_st_atime and entry.stat().st_mtime == p_st_mtime and entry.stat().st_ctime == p_st_ctime and entry.stat().st_size == p_st_size:
                continue
            # else:
            #    print("Found a difference -> " + entry.name)

        # compress *if* its a file
        if entry.is_file():
            if not os.path.isfile(tmp_path + key + '.gz'):
                #print(full_path)
                with open(full_path, 'rb') as f_in, gzip.open(tmp_path + key + '.gz', 'wb', 9) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            else:
                dupes += 1


        if len(data):
            data += ","

        data += "('" + key + "',"
        data += "'" + entry.name + "',"
        data += "'" + entry.path + "',"
        data += "'" + str(entry.inode()) + "',"
        data += "'" + str(entry.is_dir()) + "',"
        data += "'" + str(entry.is_file()) + "',"
        data += "'" + str(entry.is_symlink()) + "',"

        if entry.is_symlink():
            data += "'','','','','','','','','','','','','','','')"
        else:
            data += "'" + str(entry.stat().st_mode) + "',"
            data += "'" + str(entry.stat().st_dev) + "',"
            data += "'" + str(entry.stat().st_nlink) + "',"
            data += "'" + str(entry.stat().st_uid) + "',"
            data += "'" + str(entry.stat().st_gid) + "',"
            data += "'" + str(entry.stat().st_size) + "',"
            data += "'" + str(entry.stat().st_atime) + "',"
            data += "'" + str(entry.stat().st_mtime) + "',"
            data += "'" + str(entry.stat().st_ctime) + "',"
            data += "'" + str(entry.stat().st_blocks) + "',"
            data += "'" + str(entry.stat().st_blksize) + "',"
            data += "'" + str(entry.stat().st_rdev) + "',"
            data += "'','','')"

            insert_counter = 0

        try:
            conn.execute("INSERT OR REPLACE INTO fs_objects VALUES " + data)
            data = ""
        except Exception as e:
            print("no 1: " + str(e))
            # pass

    # send the files

    # commit to storage
    conn.commit()
    conn.close()
    if dupes:
        print(dupes)


def fetch_files():
    conn = sqlite3.connect('backup.db')

    for row in conn.execute('SELECT * FROM files'):
        print(row)

    conn.close()


def unix_date():
    now = datetime.now()
    return now.strftime("%s")  # seconds since epoch


if __name__ == '__main__':
    print("checking database...")
    create_schema()
    print("scanning...")
    directories = get_paths("/Users/mcurry/projects/")
    #directories.append("/Users/mcurry/projects/")
    print("processing...")
    tmp_dir = create_temp_dir()
    for path in directories:
        get_path_objects(path, tmp_dir)

    # store_files(rootdir)
    # fetch_files()

    # clean-up
    print("cleaning up...")
    print(tmp_dir)
    shutil.rmtree(tmp_dir)
