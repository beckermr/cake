import sys
import time
import datetime


def diff_timestamps(start_time, end_time):
    """difference the time stamps giving end_time - start_time"""
    tdiff = (datetime.datetime.strptime(end_time.split('.')[0], "%Y-%m-%d %H:%M:%S") -
             datetime.datetime.strptime(start_time.split('.')[0], "%Y-%m-%d %H:%M:%S")).total_seconds()
    tdiff += float('0.' + end_time.split('.')[-1]) - float('0.' + start_time.split('.')[-1])
    return tdiff


def print_start(id, stime):
    """print cake starting info"""
    sys.stderr.write('================================================================================'
                     '\nCAKE: starting\nCAKE: task  %s\nCAKE: start %s\n\n'
                     % (id, time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(stime))))
    sys.stderr.flush()


def print_end(id, stime, etime, state):
    """print cake ending info"""
    sys.stderr.write("""
CAKE: ending
CAKE: task  %s
CAKE: start %s
CAKE: end   %s
CAKE: run   %gs
CAKE: state %s
""" % (id,
       time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(stime)),
       time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(etime)),
       etime - stime,
       state))
    sys.stderr.flush()
