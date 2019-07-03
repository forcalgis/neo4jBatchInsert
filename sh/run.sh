#! /bin/sh

a=$1

if [ "x$JAVA_HOME" = "x" ];then
	JAVA_HOME=/home/jdk1.7.0_55
	PATH=$JAVA_HOME/bin:$PATH
	CLASSPATH==.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
	export JAVA_HOME PATH CLASSPATH
	echo [JAVA_HOME=$JAVA_HOME]
	echo [PATH=$PATH]
fi

echo JAVA_version
echo ................
java -version
echo ................


SCRIPT="$0"
HOME=`dirname "$SCRIPT"`/
HOME=`cd "$HOME"; pwd`
echo HOME=$HOME

if [ "x$MIN_MEM" = "x" ]; then
    MIN_MEM=5g
fi
if [ "x$MAX_MEM" = "x" ]; then
    MAX_MEM=5g
fi

JAVA_OPTS="$JAVA_OPTS -Xms${MIN_MEM} -Xmx${MAX_MEM}"
echo JAVA_OPTS=$JAVA_OPTS

CLASSPATH=.:$HOME/lib/*

if [ "x$a" = "x" ];then
exec "$JAVA_HOME/bin/java" $JAVA_OPTS  -Des.path.home="$HOME" -cp "$CLASSPATH" $props \
com.kensure.batchinsert.server.RelData2Neo4j \
	--mapping $HOME/config/config.json \
	--runmode history >& /dev/null &
fi
if [ "$a" = "console" ];then
exec "$JAVA_HOME/bin/java" $JAVA_OPTS  -Des.path.home="$HOME" -cp "$CLASSPATH" $props \
com.kensure.batchinsert.server.RelData2Neo4j \
	--mapping $HOME/config/config.json \
	--runmode increment 
    
fi

#--mapping $HOME/config/config.json
#--runmode history|increment
#--starttime "yyyy-MM-dd HH:mm:ss.SSS" 
#--endtime "yyyy-MM-dd HH:mm:ss.SSS"
 
