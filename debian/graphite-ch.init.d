#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
GRAPHITE_ROOT="/var/lib/graphite"
GRAPHITE_MANAGE_SCRIPT="/usr/share/pyshared/django/bin/django-admin.py"
DAEMON="/usr/bin/python"
PID_FILE="/var/run/graphite.pid"
PYTHONPATH="/var/lib/graphite/webapp"
DARGS="$GRAPHITE_MANAGE_SCRIPT runfcgi --pythonpath=$PYTHONPATH --settings=graphite.settings method=prefork host=127.0.0.1 port=6031 pidfile=$PID_FILE workdir=$GRAPHITE_ROOT/webapp outlog=/var/log/graphite/graphite_out.log errlog=/var/log/graphite/graphite_err.log maxrequests=0 maxchildren=24 minspare=24 maxspare=24"
NAME=graphite-web
DESC="graphite web interface"
GRAPHITE_LOG_DIR="/var/log/graphite"


test -e $GRAPHITE_MANAGE_SCRIPT || exit 0

set -e

case "$1" in
  start)
    if [ ! -d $GRAPHITE_LOG_DIR ]; then
        mkdir -p $GRAPHITE_LOG_DIR 
        chown www-data:www-data $GRAPHITE_LOG_DIR 
    fi
	echo -n "Starting $DESC: "
	start-stop-daemon --start -u www-data --pidfile $PID_FILE --exec $DAEMON -- $DARGS
	echo "$NAME."
	;;
  stop)
	echo -n "Stopping $DESC: "
	start-stop-daemon --stop --quiet --pidfile $PID_FILE --retry 2 --oknodo --exec $DAEMON -- $DARGS
	echo "$NAME."
	;;
  #reload)
  restart|force-reload)
	#
	#	If the "reload" option is implemented, move the "force-reload"
	#	option to the "reload" entry above. If not, "force-reload" is
	#	just the same as "restart".
	#
	echo -n "Restarting $DESC: "
	start-stop-daemon --stop --quiet --pidfile $PID_FILE --retry 2 --oknodo --exec $DAEMON -- $DARGS
	sleep 1
	start-stop-daemon --start -u www-data --pidfile $PID_FILE --exec $DAEMON -- $DARGS
	echo "$NAME."
	;;
  *)
	N=/etc/init.d/$NAME
	# echo "Usage: $N {start|stop|restart|reload|force-reload}" >&2
	echo "Usage: $N {start|stop|restart|force-reload}" >&2
	exit 1
	;;
esac

exit 0
