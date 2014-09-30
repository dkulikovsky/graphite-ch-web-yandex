import requests
import itertools
import time
import string
import re
import sphinxapi
import ConfigParser
from pprint import pprint
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

class ClickHouseReader(object):
    __slots__ = ('path','schema', 'periods', 'multi', 'pathExpr', 'storage')

    def __init__(self, path, storage="", multi=0):
        self.storage = storage
        if multi:
            self.multi = 1
            self.pathExpr =  path
        else:
            self.multi = 0
            self.path = path
        self.schema = {}
        self.periods = []
        self.load_storage_schema()
        try:
            self.storage = getattr(settings, 'CLICKHOUSE_SERVER')
        except:
            log.info("Storage is not defined in settings, using default 127.0.0.1")
            self.storage = "127.0.0.1"

    def load_storage_schema(self):
        conf = "/etc/cacher/storage_schema.ini"
        config = ConfigParser.ConfigParser()
        try:
            config.read(conf)
        except Exception, e:
            log.info("Failed to read conf file %s, reason %s" % (conf, e))
            return
        if not config.sections():
            log.info("absent or corrupted config file %s" % conf)
            return 
        
        schema = {}
        periods = []
        for section in config.sections():
            if section == 'main':
                periods = [ int(x) for x in config.get("main", "periods").split(",") ]
                continue
            if not schema.has_key(section):
                schema[section] = {}
                schema[section]['ret'] = []
                schema[section]['patt'] = config.get(section, 'pattern')
            v = config.get(section, "retentions")
            for item in v.split(","):
                schema[section]['ret'].append(int(item))
        self.schema = schema
        self.periods = periods
        log.info("DEBUG: got schema [ %s ], got periods [ %s ]" % (pprint(schema), pprint(periods)))
        return 

    def multi_fetch(self, start_time, end_time):
        start_t_g = time.time()
        # fix path, convert it to query appliable to db
        self.path = FindQuery(self.pathExpr, start_time, end_time).pattern
        data, time_step = self.get_multi_data(start_time, end_time)
        # fullfill data fetched from storages to fit timestamps 
        result = []
        start_t = time.time()
        time_info = start_time, end_time, time_step
        sorted_t = 0
        for path in data.keys():
            # fill output with nans when there is no datapoints
            filled_data = self.get_filled_data(data[path], start_time, end_time, time_step)
            start_t = time.time()
            sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
            class tmp_node_obj():
                def __init__(self, path):
                    self.path = path
            result.append((tmp_node_obj(path), (time_info, sorted_data)))
            sorted_t += time.time() - start_t
        log.info("DEBUG:OPT: sorted in %.3f" % sorted_t)
        log.info("DEBUG:OPT: all in %.3f" % (time.time() - start_t_g))
        return result

    def get_multi_data(self, start_time, end_time):
        query_hash, time_step = self.gen_multi_query(start_time, end_time)
        data = {}
        # query_hash now have only one storage beceause clickhouse has distributed table engine
        for storage in query_hash.keys():
            log.info("DEBUG:MULTI: got storage %s, query %s and time_step %d" % (storage, query_hash[storage], time_step))
            start_t = time.time()
            start_t_g = time.time()

            url = "http://%s:8123" % storage
            dps = requests.post(url, query_hash[storage]).text
            log.info("DEBUG:OPT: data fetch in %.3f" % (time.time() - start_t))
            start_t = time.time()
            if len(dps) == 0:
                log.info("WARN: empty response from db, nothing to do here")
            else:
                log.info("DEBUG:MULTI: got data from %s" % storage)
    
            # fill values array to fit (end_time - start_time)/time_step
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
        return data, time_step


    def gen_multi_query(self, stime, etime):
        coeff, agg = self.get_coeff(stime, etime)
        metrics = sphinx_search(self.path)
        storage_hash = {}
        # a bit of legacy code, where search could result in several queries in different databases
        # but now clickhouse has distributed table and multistorage arch is legacy now
        storage_hash[self.storage] = []
        for m in metrics.keys():
            storage_hash[self.storage].append(m)
        
        query_hash = {}
        for storage in storage_hash.keys():
            log.info("DEBUG:MULTI_QUERY: got %d metrics for %s storage" % (len(storage_hash[storage]), storage))
            metrics_arr = [ "'%s'" % m.strip() for m in storage_hash[storage]]
            path_expr = "Path IN ( %s )" % ", ".join(metrics_arr)
            if agg == 0:
                query = """SELECT Path, Time,Value FROM graphite_d WHERE %s\
                            AND Time > %d AND Time < %d ORDER BY Time""" % (path_expr, stime, etime) 
            else:
                query = """SELECT min(Path), min(Time),avg(Value) FROM graphite_d WHERE %s\
                            AND toInt32(kvantT) > %d AND toInt32(kvantT) < %d
                            GROUP BY Path, toDateTime(intDiv(toUInt32(Time),%d)*%d) as kvantT
                            ORDER BY kvantT""" % (path_expr, stime, etime, coeff, coeff) 
            query_hash[storage] = query
        return query_hash, coeff


    def fetch(self, start_time, end_time):
        params_hash = {}
        params_hash['query'], time_step = self.gen_query(start_time, end_time)
        log.info("DEBUG:SINGLE: got query %s and time_step %d" % (params_hash['query'], time_step))
        url = "http://%s:8123" % self.storage
        dps = requests.get(url, params = params_hash).text
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

        # fill output with nans when there is no datapoints
        filled_data = self.get_filled_data(data, start_time, end_time, time_step)

        # sort data
        sorted_data = [ filled_data[i] for i in sorted(filled_data.keys()) ]
        time_info = start_time, end_time, time_step
        return time_info, sorted_data

    def get_coeff(self, stime, etime):
        # find metric type and set coeff
        coeff = 60
        agg = 0
        for t in self.schema.keys():
            if re.match(r'^%s.*$' % self.schema[t]['patt'], self.path):
                # calc retention interval for stime
                delta = 0
                loop_index = 0
                for seconds in self.schema[t]['ret']:
                    # ugly month average 365/12 ~= 30.5
                    # TODO: fix to real delta
                    delta += int(self.periods[loop_index]) * 30.5 * 86400
                    if stime > (time.time() - delta):
                        if loop_index == 0:
                            # if start_time is in first retention interval
                            # than no aggregation needed
                            agg = 0
                            coeff = int(seconds)
                        else:
                            agg = 1
                            coeff = int(seconds)
                        break
                    loop_index += 1
                if agg == 0 and (stime < (time.time() - delta)):
                    # start_time for requested period is even earlier than defined periods
                    # take last retention
                    seconds = self.schema[t]['ret'][-1]
                    agg = 1
                    coeff = int(seconds)
                break # break the outer loop, we have already found matching schema

        log.info("DEBUG: got coeff: %d, agg = %d" % (coeff, agg))
        return coeff, agg

    def gen_query(self, stime, etime):
        coeff, agg = self.get_coeff(stime, etime)
        path_expr = "Path = '%s'" % self.path
        if agg == 0:
            query = """SELECT Time,Value FROM graphite_d WHERE %s\
                        AND Time > %d AND Time < %d ORDER BY Time""" % (path_expr, stime, etime) 
        else:
            query = """SELECT min(Time),avg(Value) FROM graphite_d WHERE %s\
                        AND toInt32(kvantT) > %d AND toInt32(kvantT) < %d
                        GROUP BY Path, toDateTime(intDiv(toUInt32(Time),%d)*%d) as kvantT
                        ORDER BY kvantT""" % (path_expr, stime, etime, coeff, coeff) 
        return query, coeff

    def get_filled_data(self, data, stime, etime, step):
        # some stat about how datapoint manage to fit timestamp map 
        ts_hit = 0
        ts_miss = 0
        ts_fail = 0
        start_t = time.time() # for debugging timeouts
        stime = stime - (stime % step)
        data_ts_min = int(min(data.keys()))
        data_stime = data_ts_min - (data_ts_min % step)
        filled_data = {}

        data_keys = sorted(data.keys())
        data_index = 0
        p_start_t_g = time.time()
        search_time = 0
        log.info("stime %d, etime %s, step %d, point %d" % (stime, etime, step, len(data)))
        for ts in xrange(stime, etime, step):
            if ts < data_stime:
                # we have no data for this timestamp, nothing to do here
                filled_data[ts] = None
                ts_fail += 1
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
                    if ts_tmp >= int(ts) and (ts_tmp - int(ts)) < step:
                        filled_data[ts] = data[data_keys[data_index]]
                        data_index += 1
                        ts_miss += 1
                        break
                    elif ts_tmp < int(ts):
                        data_index += 1
                        continue
                    elif ts_tmp > int(ts):
                        ts_fail += 1
                        filled_data[ts] = None
                        break
                search_time += time.time() - p_start_t
            # loop didn't break on continue statements, set it default NaN value
            if not filled_data.has_key(ts):
#                ts_fail += 1
                filled_data[ts] = None
#        log.info("DEBUG:OPT: loop in %.3f, search in %.3f" % ((time.time() - start_t), search_time))

#        log.info("DEBUG:OPT: filled data in %.3f" % (time.time() - start_t))
        log.info("DEBUG: hit %d, miss %d, fail %d" % (ts_hit, ts_miss, ts_fail))
        return filled_data

    def get_intervals(self):
        # TODO use cyanite info
        start = 0
        end = max(start, time.time())
        return IntervalSet([Interval(start, end)])


class ClickHouseFinder(object):
    def find_nodes(self, query):
        q = query.pattern
        metrics = sphinx_search(q)
        out = []
        for m in metrics.keys():
            dot = string.find(m, '.', len(q) - 1)
            if dot > 0:
                out.append(m[:dot+1])
            else:
                out.append(m)
        for v in out:
            if v[-1] == ".":
                yield BranchNode(v[:-1])
            else:
                yield LeafNode(v,ClickHouseReader(v, storage=metrics[v]))


def sphinx_query(query):
    start_star = 0
    end_star = 0
    if re.match(r'^\*.*', query):
        start_star = 1
    if re.match(r'.*\*$', query):
        end_star = 1
    
    # replace braces with simple wildcard
    if re.search('{.*}', query):
        query = re.sub('{.*}', "*", query)

    query_arr = [ q for q in re.split("\?|\*", query) if q ]
    if len(query_arr) > 2:
        for i in xrange(1, len(query_arr)-1):
            query_arr[i] = "*%s*" % query_arr[i]

    if len(query_arr) >= 2:
        if start_star:
            query_arr[0] = "*%s*" % query_arr[0]
        else:
            query_arr[0] = "%s*" % query_arr[0]
    
        if end_star:
            query_arr[-1] = "*%s*" % query_arr[-1]
        else:
            query_arr[-1] = "*%s" % query_arr[-1]

    # this will only work on querys like one_min.bs01g_rt.timings.t9?
    # querys like one_min.bs0?g_rt.... will be splitted in more than one element in query_arr
    # all other querys with single word will work fine without any replaces
    if len(query_arr) == 1:
        res_query = query.replace("?","*")
    else:
        res_query = " ".join(query_arr)

    return res_query

def re_query(query):
    q = query.replace(".","\.").replace("*",".*").replace("?",".")
    # if query has braces, replace it with regexp analagoue
    q = q.replace("{", "(?:").replace("}",")").replace(",","|")
    return q

def sphinx_search(query):
    re_q = re.compile(r'^%s$' % re_query(query))
    sphx_q = sphinx_query(query)
    client = sphinxapi.SphinxClient()
    sphinx_server = getattr(settings, 'SPHINX_SERVER')
    client.SetServer(sphinx_server, 9312)
    client.SetLimits(0,100000)
    try:
        res = client.Query(sphx_q)
    except Exception, e:
        return {}
    output = {}
    if res['status'] == 0 and res['total_found'] > 0:
        for item in res['matches']:
            m = item['attrs']['metric']
            if re_q.match(m):
                output[m] = 1
    log.info("DEBUG: got %d metrics" % len(output))
    return output
