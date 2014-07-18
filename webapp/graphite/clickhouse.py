import requests
import itertools
import time
import string
import redis
import re
from graphite.logger import log
from collections import OrderedDict
from django.conf import settings
from graphite.storage import FindQuery

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

import requests
ch_url = 'http://localhost:8123/'

class ClickHouseReader(object):
    __slots__ = ('path','st_schemas', 'multi', 'pathExpr')

    def __init__(self, path, multi=0):
        if multi:
            self.multi = 1
            # replace graphite * to %
            self.pathExpr =  path
        else:
            self.multi = 0
            self.path = path

        # init storage schemas
        # schemas = getattr(settings, 'CH_STORAGE_SCHEMA')
        schemas = [ 'one_min=60:14;300:30;600:1000',
                    'five_sec=5:1;10:2;60:14;300:1000',
                    'one_sec=1:1;5:14;60:300' ]
        self.st_schemas = {}
        for sch in schemas:
            (name,defn) = sch.split("=")
            self.st_schemas[name] = defn.split(";")

    def multi_fetch(self, start_time, end_time):
        log.info("DEBUG:MULTI: in")
        params_hash = {}
        # fix path, convert it to query appliable to db
        self.path = FindQuery(self.pathExpr, start_time, end_time).pattern.replace('*','%').replace('?','_')        
        params_hash['query'], time_step = self.gen_query(start_time, end_time)
        log.info("DEBUG:MULTI: got query %s and time_step %d" % (params_hash['query'], time_step))

        start_t = time.time()
        start_t_g = time.time()
        dps = requests.get(ch_url, params = params_hash).text
        log.info("DEBUG:OPT: data fetch in %.3f" % (time.time() - start_t))
        start_t = time.time()
        if len(dps) == 0:
            log.info("WARN: empty response from db, nothing to do here")
            return []
        else:
             log.info("DEBUG:MULTI: got data")
#            log.info("DEBUG:MULTI: got data, size %d" % (len(dps.split("\n")))) 

        # fill values array to fit (end_time - start_time)/time_step
        data = {}
        for dp in dps.split("\n"):
            dp = dp.strip()
            if len(dp) == 0:
                continue
            arr = dp.split("\t")
            # and now we have 3 field insted of two, first field is path
            path = arr[0].strip()
            dp_ts = arr[1].strip()
            dp_val = arr[2].strip()
            data.setdefault(path, {})[dp_ts] = float(dp_val)
        log.info("DEBUG:OPT: parsed output in %.3f" % (time.time() - start_t))
        log.info("DEBUG:MULTI: got %d keys" % len(data.keys()))
        #log.info("DEBUG: data = \n %s \n" % data)

        result = []
        start_t = time.time()
        time_info = start_time, end_time, time_step
        sorted_t = 0
        for path in data.keys():
            # fill output with nans when there is no datapoints
            filled_data = self.get_filled_data(data[path], start_time, end_time, time_step)
    #        log.info("DEBUG:OPT: filled data in %.3f" % (time.time() - start_t))

            # sort data
            start_t = time.time()
            sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
            class tmp_node_obj():
                def __init__(self, path):
                    self.path = path
            result.append((tmp_node_obj(path), (time_info, sorted_data)))
            sorted_t += time.time() - start_t
        log.info("DEBUG:OPT: sorted in %.3f" % sorted_t)
        log.info("DEBUG:OPT: all in %.3f" % (time.time() - start_t_g))
#        log.info("DEBUG: result \n #######################\n%s\n#######################\n" % result)

        return result

    def fetch(self, start_time, end_time):
        params_hash = {}
        params_hash['query'], time_step = self.gen_query(start_time, end_time)
        log.info("DEBUG:SINGLE: got query %s and time_step %d" % (params_hash['query'], time_step))

        start_t = time.time()
        start_t_g = time.time()
        dps = requests.get(ch_url, params = params_hash).text
        #log.info("DEBUG:OPT: data fetch in %.3f" % (time.time() - start_t))
        start_t = time.time()

        if len(dps) == 0:
            log.info("WARN: empty response from db, nothing to do here")
            return []

        # fill values array to fit (end_time - start_time)/time_step
        data = {}
        for dp in dps.split("\n"):
            dp = dp.strip()
            if len(dp) == 0:
                continue
            arr = dp.split("\t")
            dp_ts = arr[0].strip()
            dp_val = arr[1].strip()
            data[dp_ts] = float(dp_val)
#        log.info("DEBUG:OPT: parsed output in %.3f" % (time.time() - start_t))

        start_t = time.time()
        # fill output with nans when there is no datapoints
        filled_data = self.get_filled_data(data, start_time, end_time, time_step)
#        log.info("DEBUG:OPT: filled data in %.3f" % (time.time() - start_t))

        # sort data
        start_t = time.time()
        sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
#        log.info("DEBUG:OPT: sorted in %.3f" % (time.time() - start_t))
#        log.info("DEBUG:OPT: all in %.3f" % (time.time() - start_t_g))

        time_info = start_time, end_time, time_step
        return time_info, sorted_data

    def gen_query(self, stime, etime):
        # find metric type and set coeff
        coeff = 60
        agg = 0
        for t in self.st_schemas.keys():
            if re.match(r'^%s.*$' % t, self.path):
                # calc retention interval for stime
                delta = 0
                loop_index = 0
                for item in self.st_schemas[t]:
                    (seconds, days) = item.split(":")
                    delta += int(days)*86400
                    if stime > (time.time() - delta):
                        if loop_index == 0:
                            # if start_time is in first retention interval
                            # than no aggregation needed
                            agg = 0
                            coeff = int(seconds)
#                            log.info("No agg needed")
                        else:
                            agg = 1
                            coeff = int(seconds)
                        break
                    loop_index += 1
                if agg == 0 and (stime < (time.time() - delta)):
                    # start_time for requested period is even earlier than
                    # take last retention
                    seconds = self.st_schemas[t][-1].split(":")[0]
                    agg = 1
                    coeff = int(seconds)

                break # break the outer loop, we have already found matching schema
        path_expr = ""
        if self.multi:
            path_expr = "like(Path, '%s')" % self.path
            if agg == 0:
                query = """SELECT Path, Time,Value FROM graphite WHERE %s\
                            AND Time > %d AND Time < %d ORDER BY Time""" % (path_expr, stime, etime) 
            else:
                query = """SELECT min(Path), min(Time),avg(Value) FROM graphite WHERE %s\
                            AND toInt32(kvantT) > %d AND toInt32(kvantT) < %d
                            GROUP BY Path, toDateTime(intDiv(toUInt32(Time),%d)*%d) as kvantT
                            ORDER BY kvantT""" % (path_expr, stime, etime, coeff, coeff) 
        else:    
            path_expr = "Path = '%s'" % self.path
            if agg == 0:
                query = """SELECT Time,Value FROM graphite WHERE %s\
                            AND Time > %d AND Time < %d ORDER BY Time""" % (path_expr, stime, etime) 
            else:
                query = """SELECT min(Time),avg(Value) FROM graphite WHERE %s\
                            AND toInt32(kvantT) > %d AND toInt32(kvantT) < %d
                            GROUP BY Path, toDateTime(intDiv(toUInt32(Time),%d)*%d) as kvantT
                            ORDER BY kvantT""" % (path_expr, stime, etime, coeff, coeff) 
        return query, coeff

    def get_filled_data(self, data, stime, etime, step):
        # some stat about how datapoint manage to fit timestamp map 
        ts_hit = 0
        ts_miss = 0
        start_t = time.time() # for debugging timeouts
        stime = stime - (stime % step)
        data_ts_min = int(min(data.keys()))
        data_stime = data_ts_min - (data_ts_min % step)
        filled_data = {}

        data_keys = sorted(data.keys())
#        log.info("DEBUG: got data, keys %s, step %s" % (len(data_keys), step))
        data_index = 0
        p_start_t_g = time.time()
        search_time = 0
        for ts in xrange(stime, etime, step):
            if ts < data_stime:
                # we have no data for this timestamp, nothing to do here
                filled_data[ts] = None
                continue

            ts = unicode(ts)
            if data.has_key(ts):
                filled_data[ts] = data[ts]
                data_index += 1
                ts_hit += 1
                continue
            else:
                p_start_t = time.time()
                for i in xrange(data_index, len(data_keys)):
                    ts_tmp = int(data_keys[i])
                    if ts_tmp > int(ts) and (ts_tmp - int(ts)) < step:
                        filled_data[ts] = data[data_keys[data_index]]
                        data_index += 1
                        ts_miss += 1
                        break
                    elif ts_tmp < ts:
                        data_index += 1
                        continue
                    elif ts_tmp > ts:
                        ts_miss += 1
                        filled_data[ts] = None
                        break
#                        for ts_tmp in xrange(int(ts), int(ts)+step, 1):
#                        ts_tmp = unicode(ts_tmp)
#                        if data.has_key(ts_tmp):
#                            filled_data[ts] = data[ts_tmp]
#                            data_index += 1
#                            ts_miss += 1
#                            continue
#
#                for ts_tmp in data_keys:
#                    if ts_tmp > ts and (ts_tmp - ts) < step:
#                        filled_data[ts] = data[ts_tmp]
#                        ts_miss += 1
#                        continue
#                    elif ts_tmp < ts:
#                        data_keys.pop()
#                        continue
#                    elif ts_tmp > ts:
#                        filled_data[ts] = None
                search_time += time.time() - p_start_t
            # loop didn't break on continue statements, set it default NaN value
            if not filled_data.has_key(ts):
                ts_miss += 1
                filled_data[ts] = None
#        log.info("DEBUG:OPT: loop in %.3f, search in %.3f" % ((time.time() - start_t), search_time))

#        log.info("DEBUG:OPT: filled data in %.3f" % (time.time() - start_t))
#        log.info("DEBUG: hit %d, miss %d" % (ts_hit, ts_miss))
        return filled_data

    def get_intervals(self):
        # TODO use cyanite info
        start = 0
        end = max(start, time.time())
        return IntervalSet([Interval(start, end)])

#    def get_intervals(self):
#        log.info("DEBUG:get_intervals: in")
#        # TODO use cyanite info
#        start_t = time.time()
#        req = requests.get(ch_url, params={ 'query': "SELECT min(Time), max(Time) FROM default.graphite WHERE Path = '%s'" % self.path}).text
#        #log.info("DEBUG:OPT: got intervals in %.3f" % (time.time() - start_t))
#
#        if len(req) == 0:
#            return IntervalSet([])
#        res = req.split("\t")
#        if len(res) != 2:
#            return IntervalSet([])
#
#        log.info("DEBUG:get_intervals: out")
#        return IntervalSet([Interval(int(res[0]), int(res[1]))])


class ClickHouseFinder(object):
    def find_nodes(self, query):
        try:
            r = redis.StrictRedis(host='localhost', port=6379, db=0)
        except Exception, e:
            log.info("FATAL :( failed to connect to redis: %s" % e)
        q = query.pattern
        start_t = time.time()
        metrics = r.smembers("metrics:index")
        log.info("DEBUG:OPT: got find in %.3f" % (time.time() - start_t))
        q = q.replace(".","\.").replace("*",".*").replace("?",".")
        qre = re.compile(r'^%s$' % q)

        start_t = time.time()
        out = []
        for m in metrics:
            res = qre.match(m)
            if res:
                dot = string.find(m, '.', len(query.pattern) - 1)
                if dot > 0:
                    out.append(m[:dot+1])
                else:
                    out.append(m)
        log.info("DEBUG:OPT: merged in %.3f" % (time.time() - start_t))
        for v in out:
            if v[-1] == ".":
                yield BranchNode(v[:-1])
            else:
                yield LeafNode(v,ClickHouseReader(v))
#
#        # replace graphite * to %
#        q = query.pattern.replace('*','%')
#        q = q.replace('?','_')
#
#        params_hash = { 'query': "SELECT distinct(Path) FROM graphite WHERE like(Path, '%s')" % q }
#        start_t = time.time()
#        paths = requests.get(ch_url,params=params_hash).text
#        log.info("DEBUG:OPT: got find in %.3f" % (time.time() - start_t))
#        start_t = time.time()
#        if len(paths) == 0:
#            return
#
#        paths_hash = {}
#        for path in paths.split("\n"):
#            path = path.strip()
#            if len(path) == 0:
#                continue
#            # find first dot in metric definition and return 
#            # only next layer of metrics, not all possible matches
#            dot = string.find(path, '.', len(query.pattern) - 1)
#            if dot > 0:
#                paths_hash[path[:dot+1]] = 1
#            else:
#                paths_hash[path] = 1
#
#        log.info("DEBUG:OPT: merged in %.3f" % (time.time() - start_t))
#        # now we have a layer of metric, if metric end in . than is's a Branch.
#        # Leaf otherwise. 
#        for v in paths_hash.keys():
#            if v[-1] == ".":
#                yield BranchNode(v[:-1])
#            else:
#                yield LeafNode(v,ClickHouseReader(v))
