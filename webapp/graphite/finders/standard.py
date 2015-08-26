import os
import re
import fnmatch
from os.path import islink, isdir, isfile, realpath, join, dirname, basename
from glob import glob
from ceres import CeresTree, CeresNode
from graphite.conductor import Conductor
from graphite.node import BranchNode, LeafNode
from graphite.readers import CeresReader, WhisperReader, GzippedWhisperReader, RRDReader
from graphite.util import find_escaped_pattern_fields
from os.path import isdir, isfile, join, basename
from django.conf import settings
from graphite.logger import log
from graphite.node import BranchNode, LeafNode
from graphite.readers import WhisperReader, GzippedWhisperReader, RRDReader
from graphite.util import find_escaped_pattern_fields
from . import fs_to_metric, get_real_metric_path, match_entries

re_braces = re.compile(r'({[^{},]*,[^{}]*})')
re_conductor = re.compile(r'(^%[\w@-]+)$')
conductor = Conductor()

def braces_glob(s):
  match = re_braces.search(s)

  if not match:
    return glob(s)

  res = set()
  sub = match.group(1)
  open_pos, close_pos = match.span(1)

  for bit in sub.strip('{}').split(','):
    res.update(braces_glob(s[:open_pos] + bit + s[close_pos:]))

  return list(res)

def conductor_glob(pattern):
  parts = pattern.split('/')
  pos = 0
  found = False
  for part in parts:
    if re_conductor.match(part):
      found = True
      break
    pos += 1
  if not found:
    return braces_glob(pattern)
  cexpr = parts[pos]
  hosts = conductor.expandExpression(cexpr)

  if not hosts:
    return braces_glob(pattern)
  hosts = [host.replace('.','_') for host in hosts]

  if len(hosts) == 1:
    braces_expr = hosts[0]
  else:
    braces_expr = '{' + ','.join(hosts) + '}'
  parts[pos] = braces_expr

  return braces_glob('/'.join(parts))


class CeresFinder:
  def __init__(self, directory=None):
    directory = directory or settings.CERES_DIR
    self.directory = directory
    self.tree = CeresTree(directory)

  def find_nodes(self, query):
    log.info("running ceres finder %s" % query)
    for fs_path in conductor_glob( self.tree.getFilesystemPath(query.pattern) ):
      metric_path = self.tree.getNodePath(fs_path)

      if CeresNode.isNodeDir(fs_path):
        ceres_node = self.tree.getNode(metric_path)

        if ceres_node.hasDataForInterval(query.startTime, query.endTime):
          real_metric_path = get_real_metric_path(fs_path, metric_path)
          reader = CeresReader(ceres_node, real_metric_path)
          yield LeafNode(metric_path, reader)

      elif isdir(fs_path):
        yield BranchNode(metric_path)


class StandardFinder:
  DATASOURCE_DELIMETER = '::RRD_DATASOURCE::'

  def __init__(self, directories=None):
    directories = directories or settings.STANDARD_DIRS
    self.directories = directories

  def find_nodes(self, query):
    log.info("running blablabla RRd")
    clean_pattern = query.pattern.replace('\\', '')
    pattern_parts = clean_pattern.split('.')

    for root_dir in self.directories:
      for absolute_path in self._find_paths(root_dir, pattern_parts):
        if basename(absolute_path).startswith('.'):
          continue

        if self.DATASOURCE_DELIMETER in basename(absolute_path):
          (absolute_path, datasource_pattern) = absolute_path.rsplit(self.DATASOURCE_DELIMETER, 1)
        else:
          datasource_pattern = None

        relative_path = absolute_path[ len(root_dir): ].lstrip('/')
        metric_path = fs_to_metric(relative_path)
        real_metric_path = get_real_metric_path(absolute_path, metric_path)

        metric_path_parts = metric_path.split('.')
        for field_index in find_escaped_pattern_fields(query.pattern):
          metric_path_parts[field_index] = pattern_parts[field_index].replace('\\', '')
        metric_path = '.'.join(metric_path_parts)

        # Now we construct and yield an appropriate Node object
        if isdir(absolute_path):
          yield BranchNode(metric_path)

        elif isfile(absolute_path):
          if absolute_path.endswith('.wsp') and WhisperReader.supported:
            reader = WhisperReader(absolute_path, real_metric_path)
            yield LeafNode(metric_path, reader)

          elif absolute_path.endswith('.wsp.gz') and GzippedWhisperReader.supported:
            reader = GzippedWhisperReader(absolute_path, real_metric_path)
            yield LeafNode(metric_path, reader)

          elif absolute_path.endswith('.rrd') and RRDReader.supported:
            if datasource_pattern is None:
              yield BranchNode(metric_path)

            else:
              for datasource_name in RRDReader.get_datasources(absolute_path):
                if match_entries([datasource_name], datasource_pattern):
                  reader = RRDReader(absolute_path, datasource_name)
                  yield LeafNode(metric_path + "." + datasource_name, reader)

  def _find_paths(self, current_dir, patterns):
    """Recursively generates absolute paths whose components underneath current_dir
    match the corresponding pattern in patterns"""
    pattern = patterns[0]
    patterns = patterns[1:]
    try:
      entries = os.listdir(current_dir)
    except OSError as e:
      log.exception(e) 
      entries = []

    subdirs = [e for e in entries if isdir( join(current_dir,e) )]
    matching_subdirs = match_entries(subdirs, pattern)

    if len(patterns) == 1 and RRDReader.supported: #the last pattern may apply to RRD data sources
      files = [e for e in entries if isfile( join(current_dir,e) )]
      rrd_files = match_entries(files, pattern + ".rrd")

      if rrd_files: #let's assume it does
        datasource_pattern = patterns[0]

        for rrd_file in rrd_files:
          absolute_path = join(current_dir, rrd_file)
          yield absolute_path + self.DATASOURCE_DELIMETER + datasource_pattern

    if patterns: #we've still got more directories to traverse
      for subdir in matching_subdirs:

        absolute_path = join(current_dir, subdir)
        for match in self._find_paths(absolute_path, patterns):
          yield match

    else: #we've got the last pattern
      files = [e for e in entries if isfile( join(current_dir,e) )]
      matching_files = match_entries(files, pattern + '.*')

      for basename in matching_files + matching_subdirs:
        yield join(current_dir, basename)
