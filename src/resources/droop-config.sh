#@IgnoreInspection BashAddShebang
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
home=`cd "$bin/..">/dev/null; pwd`
this="$home/bin/$script"

# the root of the droop installation
if [ -z "$DROOP_HOME" ]; then
  DROOP_HOME="$home"
fi

#check to see if the conf dir or droop home are given as an optional arguments
while [ $# -gt 1 ]; do
  if [ "--config" = "$1" ]; then
    shift
    confdir=$1
    shift
    DROOP_CONF_DIR=$confdir
  else
    # Presume we are at end of options and break
    break
  fi
done

# Allow alternate droop conf dir location.
DROOP_CONF_DIR="${DROOP_CONF_DIR:-/etc/droop/conf}"

if [ ! -d $DROOP_CONF_DIR ]; then
  DROOP_CONF_DIR=$DROOP_HOME/conf
fi

# Source droop-env.sh for any user configured values
. "${DROOP_CONF_DIR}/droop-env.sh"

# get log directory
if [ "x${DROOP_LOG_DIR}" = "x" ]; then
  export DROOP_LOG_DIR=/var/log/droop
fi

touch "$DROOP_LOG_DIR/droop" &> /dev/null
TOUCH_EXIT_CODE=$?
if [ "$TOUCH_EXIT_CODE" = "0" ]; then
  if [ "x$DROOP_LOG_DEBUG" = "x1" ]; then
    echo "Droop log directory: $DROOP_LOG_DIR"
  fi
  DROOP_LOG_DIR_FALLBACK=0
else
  #Force DROOP_LOG_DIR to fall back
  DROOP_LOG_DIR_FALLBACK=1
fi

if [ ! -d "$DROOP_LOG_DIR" ] || [ "$DROOP_LOG_DIR_FALLBACK" = "1" ]; then
  if [ "x$DROOP_LOG_DEBUG" = "x1" ]; then
    echo "Droop log directory $DROOP_LOG_DIR does not exist or is not writable, defaulting to $DROOP_HOME/log"
  fi
  DROOP_LOG_DIR=$DROOP_HOME/log
  mkdir -p $DROOP_LOG_DIR
fi

# Add Droop conf folder at the beginning of the classpath
CP=$DROOP_CONF_DIR

# Followed by any user specified override jars
if [ "${DROOP_CLASSPATH_PREFIX}x" != "x" ]; then
  CP=$CP:$DROOP_CLASSPATH_PREFIX
fi

# Next Droop core jars
CP=$CP:$DROOP_HOME/jars/*

# Followed by Droop override dependency jars
#CP=$CP:$DROOP_HOME/jars/ext/*

# Followed by Hadoop's jar
#if [ "${HADOOP_CLASSPATH}x" != "x" ]; then
#  CP=$CP:$HADOOP_CLASSPATH
#fi

# Followed by HBase' jar
#if [ "${HBASE_CLASSPATH}x" != "x" ]; then
#  CP=$CP:$HBASE_CLASSPATH
#fi

# Followed by Droop other dependency jars
#CP=$CP:$DROOP_HOME/jars/3rdparty/*
#CP=$CP:$DROOP_HOME/jars/classb/*

# Finally any user specified
#if [ "${DROOP_CLASSPATH}x" != "x" ]; then
#  CP=$CP:$DROOP_CLASSPATH
#fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
#export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# Test for cygwin
#is_cygwin=false
#case "`uname`" in
#CYGWIN*) is_cygwin=true;;
#esac

# Test for or find JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
  if [ -e `which java` ]; then
    SOURCE=`which java`
    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
      DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
      SOURCE="$(readlink "$SOURCE")"
      [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    JAVA_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
  fi
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Droop requires Java 1.7 or later.                                    |
+======================================================================+
EOF
    exit 1
  fi
fi
# Now, verify that 'java' binary exists and is suitable for Droop.
JAVA_BIN="java"
JAVA=`find -L "$JAVA_HOME" -name $JAVA_BIN -type f | head -n 1`
if [ ! -e "$JAVA" ]; then
  echo "Java not found at JAVA_HOME=$JAVA_HOME."
  exit 1
fi
# Ensure that Java version is at least 1.7
"$JAVA" -version 2>&1 | grep "version" | egrep -e "1.4|1.5|1.6" > /dev/null
if [ $? -eq 0 ]; then
  echo "Java 1.7 or later is required to run Apache Droop."
  exit 1
fi

# Adjust paths for CYGWIN
#if $is_cygwin; then
#  DROOP_HOME=`cygpath -w "$DROOP_HOME"`
#  DROOP_CONF_DIR=`cygpath -w "$DROOP_CONF_DIR"`
#  DROOP_LOG_DIR=`cygpath -w "$DROOP_LOG_DIR"`
#  CP=`cygpath -w -p "$CP"`
#  if [ -z "$HADOOP_HOME" ]; then
#    HADOOP_HOME=${DROOP_HOME}/winutils
#  fi
#fi

# make sure allocator chunks are done as mmap'd memory (and reduce arena overhead)
export MALLOC_ARENA_MAX=4
export MALLOC_MMAP_THRESHOLD_=131072
export MALLOC_TRIM_THRESHOLD_=131072
export MALLOC_TOP_PAD_=131072
export MALLOC_MMAP_MAX_=65536

# Variables exported form this script
#export HADOOP_HOME
#export is_cygwin
export DROOP_HOME
export DROOP_CONF_DIR
export DROOP_LOG_DIR
export CP