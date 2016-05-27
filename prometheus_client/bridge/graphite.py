#!/usr/bin/python
from __future__ import unicode_literals

import logging
import re
import socket
import time
import threading

from .. import core

# Roughly, have to keep to what works as a file name.
# We also remove periods, so labels can be distinguished.
_INVALID_GRAPHITE_CHARS = re.compile(r"[^a-zA-Z0-9_-]")

LOG = logging.getLogger('graphite-bridge')

def _sanitize(s):
    return _INVALID_GRAPHITE_CHARS.sub('_', s)


class _RegularPush(threading.Thread):
    def __init__(self, pusher, interval, prefix):
        super(_RegularPush, self).__init__()
        self._pusher = pusher
        self._interval = interval
        self._prefix = prefix

    def run(self):
        wait_until = time.time()
        while True:
            while True:
                now = time.time()
                if now >= wait_until:
                    # May need to skip some pushes.
                    while wait_until < now:
                        wait_until += self._interval
                    break
                # time.sleep can return early.
                time.sleep(wait_until - now)
            try:
                self._pusher.push(prefix=self._prefix)
            except IOError:
                LOG.error("Push failed")


class GraphiteBridge(object):
    def __init__(self, address, registry=core.REGISTRY, timeout_seconds=30, _time=time, label_templates={}):
        self._address = address
        self._registry = registry
        self._timeout = timeout_seconds
        self._time = _time
        self._label_templates = label_templates

    def push(self, prefix=''):
        now = int(self._time.time())
        output = []

        prefixstr = prefix or ''

        for metric in self._registry.collect():
            for name, labels, value in metric.samples:
                label_dict = dict(
                            (_sanitize(k), _sanitize(v))
                            for k, v in sorted(labels.items()))
                label_dict['name'] = name
                if name in self._label_templates:
                    labelstr = self._label_templates[name].format( **label_dict)
                else:
                    labelstr = '.'.join([_sanitize(name)] +
                    ['{0}.{1}'.format(
                            _sanitize(k), _sanitize(v))
                            for k, v in sorted(labels.items())])
                output.append('{0}.{1} {2} {3}\n'.format(
                    prefixstr, labelstr, float(value), now))


        try:
            conn = socket.create_connection(self._address, self._timeout)
            conn.sendall(''.join(output).encode('ascii'))
            conn.close()
        except Exception as e:
            LOG.error('Could not connect to graphite at %s', self._address)

    def start(self, interval=60.0, prefix=''):
        t = _RegularPush(self, interval, prefix)
        t.daemon = True
        t.start()
