#!/adsc/DDEA_PROTO/bin/python

import numpy as np
import datetime as dt
from datetime import datetime
from dateutil import tz
import multiprocessing as mp
import os, json, pytz, time, traceback, urllib2, simplejson, shlex

from const import VTT_BIN_FOLDER, BIN_EXT
from toolset import saveObjectBinaryFast, perdelta, str2datetime

#finland = timezone('Europe/Helsinki')
URL_PREFIFX = "http://121.78.237.160:8080" #"keti3.oktree.com"
PATH_REP = {"/": "_", "-": "_", " ": "_", ".": "_"}
PATH_PREFIX = "VTT_"

def read_vtt_data(sensor, start_time, end_time):
    global URL_PREFIFX

    filename = VTT_BIN_FOLDER + sensor + ".bin"

    req_url = URL_PREFIFX + "/dashboard/query/?start=" + \
              start_time.strftime('%Y/%m/%d-%H:%M:%S') + \
              "&end=" + end_time.strftime('%Y/%m/%d-%H:%M:%S') + \
              "&m=avg:"+sensor+"&ascii"
    print "Retreving " + sensor + " data..."

    lines = urllib2.urlopen(req_url).read().split("\n")
    times = []
    values = []

    def _parse_data_line(data_line):
        pt = shlex.split(data_line)
        if len(pt) > 1:
            ltime = datetime.fromtimestamp(int(pt[1]))

            if start_time <= ltime < end_time:
                stime = ltime.strftime('%Y-%m-%d %H:%M:%S')
                dtime = str2datetime(stime)
                dt = dtime.timetuple()

                times.append([dtime, dt[5], dt[4], dt[3], dt[6], dt[2], dt[1]])
                values.append(float(pt[2]))

    def _merge(filepath, addl):
        pass

        """
        try:
            orig = dill_load_obj(filepath)
        except:
            #import traceback;print traceback.print_exc()
            return None
        finally:
            ## concatenate two objects
            return {'ts': np.vstack((orig['ts'], addl['ts'])),
                    'value': np.hstack((orig['value'], addl['value']))}
        """

    try:
        map(lambda dl: _parse_data_line(dl), lines)
    finally:
        data = {"ts": np.array(times), "value": np.array(values)}

#        if os.path.isfile(filename):
#            data = _merge(filename, data)

        if data:
            saveObjectBinaryFast(data, filename)








def load_finland_ids():
    with open('finland_ids.csv', 'r') as f:
        lines = f.readlines()
    return map(lambda l: l.split(',')[0].strip(), lines)


def get_retrieve_url(sensor, start_time, end_time):
    url = URL_PREFIFX + "/dashboard/query/?start=" + \
              start_time.strftime('%Y/%m/%d-%H:%M:%S') + \
              "&end=" + end_time.strftime('%Y/%m/%d-%H:%M:%S') + \
              "&m=avg:"+sensor+"&ascii"
    return url


def get_sensor_data(url):
    data = urllib2.urlopen(url).read().split("\n")
    return data


def get_path_sorted(path, path_prefix, dic):
    path = str.strip(path)
    for i, j in dic.iteritems():
        path = path.replace(i, j)
    return (path_prefix + path).upper()


def construct_stub_url_list(path_prefix, path_reps, start_date, end_date, out_dir):
    sensor_list = filter(lambda u: not (u[0] == "" or u[0] == "\n" or u[0] == "-"), load_finland_ids())
    sensor_count = len(sensor_list)

    file_name = map(lambda s: get_path_sorted(s, path_prefix, path_reps), sensor_list)

    start_data_list = [start_date] * sensor_count
    end_data_list = [end_date] * sensor_count
    out_dir_list = [out_dir] * sensor_count
    return zip(sensor_list, file_name, start_data_list, end_data_list, out_dir_list)


def collect_and_save_bin(url_info):

    ts_list = list()
    value_list = list()

    def _parse_data_line(data_line):
        pt = shlex.split(data_line)
        if len(pt) > 1:
            ltime = datetime.fromtimestamp(int(pt[1]))
            stime = ltime.strftime('%Y-%m-%d %H:%M:%S')
            dtime = str2datetime(stime)
            time_tup = dtime.timetuple()

            ts_list.append([dtime, time_tup[5], time_tup[4], time_tup[3], time_tup[6], time_tup[2], time_tup[1]])
            value_list.append(float(pt[2]))


    sensor, filename, start_date, end_date, out_dir = url_info

    try:
        start = time.time()
        out_file = os.path.join(out_dir + filename + BIN_EXT)
        sensor_readings = list()

        for s, e in perdelta(start_date, end_date, dt.timedelta(days=7)):
            url = get_retrieve_url(sensor, s, e)

            try:
                data = get_sensor_data(url)
            except:
                pass

            if data and len(data):
                sensor_readings.extend(data)

        if not len(sensor_readings):
            print "---- WE'VE GOT NOTHING FOR", filename, "-----"
            return

        map(lambda dl: _parse_data_line(dl), sensor_readings)

        filedata = {"ts": np.array(ts_list), "value": np.array(value_list)}

        saveObjectBinaryFast(filedata, out_file)

        # print some stats: time and filesize
        end = time.time()
        filesize = os.path.getsize(out_file)
        print "----- COLLECTING & SAVING COMPLETE %s %s: %s (%.3f MB) in %.3f secs ------"%\
              (filename, dt.datetime.now(), out_file, filesize * 1.0 / 10**6, end - start)

    except:
        print '=== ERROR ==='
        print traceback.format_exc()


if __name__ == '__main__':

    start_time = dt.time(hour=0, minute=0, second=0)
    start_date = dt.datetime.combine(dt.date(2015, 1, 1), start_time)
    end_date = dt.datetime.combine(dt.date(2015, 2, 1), start_time)

    stub_url = construct_stub_url_list(
        PATH_PREFIX,
        PATH_REP,
        start_date,
        end_date,
        VTT_BIN_FOLDER)

    pool = mp.Pool(processes=4)
    pool.map(collect_and_save_bin, stub_url)
    pool.terminate()
    pool.join()