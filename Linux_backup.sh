#!/bin/bash

# This checks if the number of arguments is correct
# If the number of arguments is incorrect ( $# != 2) print error message and exit
if [[ $# != 2 ]]
then
  echo "backup.sh target_directory_name destination_directory_name"
  exit
fi

# This checks if argument 1 and argument 2 are valid directory paths
if [[ ! -d $1 ]] || [[ ! -d $2 ]]
then
  echo "Invalid directory path provided"
  exit
fi

# [TASK 1]
targetDirectory=$1
destinationDirectory=$2

# [TASK 2]
echo "target directory name: $targetDirectory"
echo "destination directory name: $destinationDirectory"

# [TASK 3]
currentTS=`date +"%s"`

# [TASK 4]
backupFileName="backup-[$currentTS].tar.gz"

# 1: Go into the target directory
# [TASK 5]
origAbsPath=`pwd`

# [TASK 6]
cd $destinationDirectory
destDirAbsPath=`pwd`

# [TASK 7]
cd $origAbsPath
cd $targetDirectory

# 2: Create the backup file
# [TASK 8]
yesterdayTS=$(($currentTS - 24 * 60 * 60))

declare -a toBackup

for file in $(ls $(pwd)) # [TASK 9]
do
  # [TASK 10]
  file_last_modified_date=$(date -r $file +%s)
  if (($file_last_modified_date > $yesterdayTS))
  then
    # [TASK 11]
    toBackup+=($file)
  fi
done

# [TASK 12]
tar -czvf $backupFileName ${toBackup[@]}

# 3: Move the backup file to the destination directory
# [TASK 13]
mv $backupFileName $destDirAbsPath