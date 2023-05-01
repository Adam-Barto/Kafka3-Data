#!/bin/bash
MACHARCH=`uname -m`
MACM1= 'x86_64'# "arm64" Changed due to virtual machine

if [[ "$MACHARCH" == "$MACM1" ]]; then
   /opt/homebrew/opt/kafka/bin/zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
else
   zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
fi
