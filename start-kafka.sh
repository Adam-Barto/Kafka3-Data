#!/bin/bash
MACHARCH=`uname -m`
MACM1="arm64"

if [[ "$MACHARCH" == "$MACM1" ]]; then
   /opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
else
   kafka-server-start /usr/local/etc/kafka/server.properties
fi
