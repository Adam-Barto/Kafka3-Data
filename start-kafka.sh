#!/bin/bash
MACHARCH=`uname -m`
MACM1='x86_64'# "arm64" Changed due to virtual machine

if [[ "$MACHARCH" == "$MACM1" ]]; then
   /opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
else
   kafka-server-start /usr/local/etc/kafka/server.properties
fi
