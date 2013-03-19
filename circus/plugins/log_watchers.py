from __future__ import print_function
import sys
import json
from circus.plugins import CircusPlugin


class OutputStream(CircusPlugin):

    name = 'stream_observer'

    def __init__(self, *args, **config):
        super(OutputStream, self).__init__(*args, **config)
        self.topics = config.get('topics', [])

    def handle_recv(self, data):
        topic, message = data
        if topic in self.topics:
            sys.stdout.write(json.loads(message))
