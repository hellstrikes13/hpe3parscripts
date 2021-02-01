#!/bin/bash
mkdir -p /opt/3parscripts
echo 'starting hpe3par scripts'
. hpe3parcpu_stats.py &
. hpe3pardisk_iops_stats.py & 
. hpe3pardisk_space_stats.py &
. hpe3pariscsi.py &
echo 'Done...'
