#!/bin/sh

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set WRENCH_HOME if not already set
[ -z "$WRENCH_HOME" ] && WRENCH_HOME=`cd "$PRGDIR/.." ; pwd`
WRENCH_CLASSPATH="$WRENCH_HOME/lib"
for f in $WRENCH_HOME/lib/*.jar
do
    WRENCH_CLASSPATH=$WRENCH_CLASSPATH:$f
done

java -Duser.dir=$WRENCH_HOME -Dwrench.config.file=$WRENCH_HOME/conf/wrench.properties -classpath $WRENCH_CLASSPATH edu.ucsb.cs.wrench.Main $*