#!/usr/bin/env bash

CURRENT_DIR=$(pwd)

BIN_DIR="/data/server/flume/1.9.0/"

CHECK_PID="ps aux | grep \"${BIN_DIR}\" | grep 'flume' | grep -v grep | awk '{print \$2}'"

cd ${BIN_DIR}

FLUME_PID=$(eval ${CHECK_PID})

if [ ""x == "${FLUME_PID}"x ] ;then
  echo "Flume is no runnig"
  cd ${CURRENT_DIR}
  exit 0
fi

kill -9 $FLUME_PID

echo "Flume is stop!"

cd ${CURRENT_DIR}