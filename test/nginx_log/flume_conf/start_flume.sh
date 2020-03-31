#!/usr/bin/env bash

CURRENT_DIR=$(pwd)

BIN_DIR="/data/server/flume/1.9.0/"

CHECK_PID="ps aux | grep \"${BIN_DIR}\" | grep 'flume' | grep -v grep | awk '{print \$2}'"

cd ${BIN_DIR}

FLUME_PID=$(eval ${CHECK_PID})

if [ ""x != "${FLUME_PID}"x ] ;then
  echo "Flume is running, please kill the flume process"
  cd ${CURRENT_DIR}
  exit 0
fi

nohup ./bin/flume-ng agent --conf ./conf -f ./conf/flume.conf --name myagent > ../nohup.out 2>&1 &

#等3秒后执行下一条
sleep 3

FLUME_PID=$(eval ${CHECK_PID})

if [ ""x != "${FLUME_PID}"x ] ;then
  echo "Flume is running!"
fi

cd ${CURRENT_DIR}