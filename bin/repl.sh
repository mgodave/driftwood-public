#!/bin/bash

DIR=$(dirname $0)

JAVA_OPTS="--add-exports jdk.jshell/jdk.internal.jshell.tool=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED" \
./gradlew --no-daemon --console plain jshell "$@"
