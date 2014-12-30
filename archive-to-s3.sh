#!/bin/bash

DIRS=/var/lib/backuppc/pc/*/

MAX_AGE=10
TAR=/bin/tar
SPLIT=/usr/bin/split
PAR=/usr/bin/par
COMP=/bin/bzip2
SIZE=500000
compext="tar"

while true; do
	echo 'Starting new round'

	for DIR in $DIRS; do
		echo "Processing $DIR"
		NUMS=$(find $DIR -mindepth 1 -maxdepth 1 -type d -print)

		for NUM in $NUMS; do
			# echo $NUM

			backupNum=$(basename $NUM 2> /dev/null)
			host=$(basename $(dirname $NUM ) 2> /dev/null)

			if [[ $backupNum == '*' ]]; then
				echo "Skipping invalid backup : $backupNum"
				continue;
			fi

			shares=$(find $NUM -mindepth 1 -maxdepth 1 -type d -print)

			for share in $shares; do
				if [ -f $share/.glacier ]; then
					echo "Skipping share, already frozen."
					continue
				fi

				echo "Uploading backup #$backupNum for host $host"

				echo ">>> /usr/share/backuppc/bin/BackupPC_archiveHost_s3 $TAR $SPLIT $PAR" \
					"$host $backupNum $COMP $compext $SIZE $NUM $parfile $share"

				/usr/share/backuppc/bin/BackupPC_archiveHost_s3 $TAR $SPLIT $PAR \
					$host $backupNum $COMP $compext $SIZE $NUM $parfile $share

				rm -rf $share/*
				echo 'FETCH' > $share/.glacier
			done
		done
	done

	echo 'Waiting 1mn before next round'
	sleep 60
done