#! /bin/bash

my_password="xxxx"
date=$(date +%Y%m%d)
rootbackupdirectory="/tmp/mysqldumps/"
backupdirectory="/tmp/mysqldumps/$date"
backup_file="$date-all-databases-backup.sql"

if [ ! -d $rootbackupdirectory ] ; then
    mkdir $rootbackupdirectory
fi

if [ ! -d $backupdirectory ] ; then
    mkdir $backupdirectory
fi

if mysqldump --all-databases --user=root --password="$my_password" > "$backupdirectory/$backup_file" ; then
    echo 'SQL dump created'
fi



