"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import sys
import time
import threading
from graphite.logger import log
from graphite.storage import STORE
from graphite.readers import FetchInProgress
from django.conf import settings
from graphite.clickhouse import ClickHouseReader
from django.core.cache import cache
from hashlib import md5

class TimeSeries(list):
  def __init__(self, name, start, end, step, values, consolidate='average'):
    list.__init__(self, values)
    self.name = name
    self.start = start
    self.end = end
    self.step = step
    self.consolidationFunc = consolidate
    self.valuesPerPoint = 1
    self.options = {}


  def __iter__(self):
    if self.valuesPerPoint > 1:
      return self.__consolidatingGenerator( list.__iter__(self) )
    else:
      return list.__iter__(self)


  def consolidate(self, valuesPerPoint):
    self.valuesPerPoint = int(valuesPerPoint)


  def __consolidatingGenerator(self, gen):
    buf = []
    for x in gen:
      buf.append(x)
      if len(buf) == self.valuesPerPoint:
        while None in buf: buf.remove(None)
        if buf:
          yield self.__consolidate(buf)
          buf = []
        else:
          yield None
    while None in buf: buf.remove(None)
    if buf: yield self.__consolidate(buf)
    else: yield None
    raise StopIteration


  def __consolidate(self, values):
    usable = [v for v in values if v is not None]
    if not usable: return None
    if self.consolidationFunc == 'sum':
      return sum(usable)
    if self.consolidationFunc == 'average':
      return float(sum(usable)) / len(usable)
    if self.consolidationFunc == 'max':
      return max(usable)
    if self.consolidationFunc == 'min':
      return min(usable)
    raise Exception("Invalid consolidation function!")


  def __repr__(self):
    return 'TimeSeries(name=%s, start=%s, end=%s, step=%s)' % (self.name, self.start, self.end, self.step)


  def getInfo(self):
    """Pickle-friendly representation of the series"""
    return {
      'name' : self.name,
      'start' : self.start,
      'end' : self.end,
      'step' : self.step,
      'values' : list(self),
    }


# Data retrieval API
def fetchData(requestContext, pathExpr):

  seriesList = []
  startTime = int( time.mktime( requestContext['startTime'].timetuple() ) )
  endTime   = int( time.mktime( requestContext['endTime'].timetuple() ) )
  def _fetchData(pathExpr,startTime, endTime, requestContext, seriesList):
    matching_nodes = STORE.find(pathExpr, startTime, endTime, local=requestContext['localOnly'], reqkey=requestContext['request_key'])
    matching_nodes = list(matching_nodes)
    if len(matching_nodes) > 1:
        request_hash = md5("%s_%s_%s" % (pathExpr, startTime, endTime)).hexdigest()
        cached_result = cache.get(request_hash)
        if cached_result:
	    log.info("DEBUG:fetchData: got result from cache for %s_%s_%s" % (pathExpr, startTime, endTime))
            fetches = cached_result
        else:
	    log.info("DEBUG:fetchData: no cache for %s_%s_%s" % (pathExpr, startTime, endTime))
            fetches = ClickHouseReader(pathExpr, multi=1, reqkey=requestContext['request_key']).multi_fetch(startTime, endTime)
            try:
                cache.add(request_hash, fetches)
            except Exception as err:
                log.exception("Failed save data in memcached:", str(err))
    elif len(matching_nodes) == 1:
        fetches = [(matching_nodes[0], matching_nodes[0].fetch(startTime, endTime))]
    else:
        fetches = []

    for node, results in fetches:
      if isinstance(results, FetchInProgress):
        results = results.waitForResults()

      if not results:
        log.info("render.datalib.fetchData :: no results for %s.fetch(%s, %s)" % (node, startTime, endTime))
        continue

      try:
          (timeInfo, values) = results
      except ValueError, e:
          e = sys.exc_info()[1]
          raise Exception("could not parse timeInfo/values from metric '%s': %s" % (node.path, e))
      (start, end, step) = timeInfo

      series = TimeSeries(node.path, start, end, step, values)
      series.pathExpression = pathExpr #hack to pass expressions through to render functions
      seriesList.append(series)

    # Prune empty series with duplicate metric paths to avoid showing empty graph elements for old whisper data
    names = set([ series.name for series in seriesList ])
    for name in names:
      series_with_duplicate_names = [ series for series in seriesList if series.name == name ]
      empty_duplicates = [ series for series in series_with_duplicate_names if not nonempty(series) ]

      if series_with_duplicate_names == empty_duplicates and len(empty_duplicates) > 0: # if they're all empty
        empty_duplicates.pop() # make sure we leave one in seriesList

      for series in empty_duplicates:
        seriesList.remove(series)

    return seriesList
  
  retries = 1 # start counting at one to make log output and settings more readable
  while True:
    try:
      seriesList = _fetchData(pathExpr,startTime, endTime, requestContext, seriesList)
      return seriesList
    except Exception, e:
      if retries >= settings.MAX_FETCH_RETRIES:
        log.exception("Failed after %i retry! See: %s" % (settings.MAX_FETCH_RETRIES, e))
        raise Exception("Failed after %i retry! See: %s" % (settings.MAX_FETCH_RETRIES, e))
      else:
        log.exception("Got an exception when fetching data! See: %s Will do it again! Run: %i of %i" %
                     (e, retries, settings.MAX_FETCH_RETRIES))
        retries += 1


def nonempty(series):
  for value in series:
    if value is not None:
      return True

  return False
