from __future__ import print_function
import os
import sys
import json
import random

from datetime import datetime

from circus import logger
from circus.plugins import CircusPlugin


class FileStream(CircusPlugin):

    name = 'filestream'

    def __init__(self, *args, **config):
        '''
        File writer handler which writes output to a file, allowing rotation
        behaviour based on Python's ``logging.handlers.RotatingFileHandler``.

        By default, the file grows indefinitely. You can specify particular
        values of max_bytes and backup_count to allow the file to rollover at
        a predetermined size.

        Rollover occurs whenever the current log file is nearly max_bytes in
        length. If backup_count is >= 1, the system will successively create
        new files with the same pathname as the base file, but with extensions
        ".1", ".2" etc. appended to it. For example, with a backup_count of 5
        and a base file name of "app.log", you would get "app.log",
        "app.log.1", "app.log.2", ... through to "app.log.5". The file being
        written to is always "app.log" - when it gets filled up, it is closed
        and renamed to "app.log.1", and if files "app.log.1", "app.log.2" etc.
        exist, then they are renamed to "app.log.2", "app.log.3" etc.
        respectively.

        If max_bytes is zero, rollover never occurs.
        '''
        super(FileStream, self).__init__(*args, **config)
        self._filename = config['filename']
        self._max_bytes = int(config.get('max_bytes', 0))
        self._backup_count = int(config.get('backup_count', 0))
        self._file = None
        self._buffer = []
        self.topic = config.get('topic', [])

    def _open(self):
        return open(self._filename, 'a+')

    def handle_init(self):
        self._file = self._open()

    def handle_recv(self, data):
        topic, message = data
        if topic == self.topic:
            message = '%s: %s' % (topic, json.loads(message))
            if self._should_rollover(message):
                self._do_rollover()
            self._file.write(message)
            self._file.flush()

    def handle_stop(self):
        self._file.close()

    def _do_rollover(self):
        """
        Do a rollover, as described in __init__().
        """
        if self._file:
            self._file.close()
            self._file = None
        if self._backup_count > 0:
            for i in range(self._backup_count - 1, 0, -1):
                sfn = "%s.%d" % (self._filename, i)
                dfn = "%s.%d" % (self._filename, i + 1)
                if os.path.exists(sfn):
                    logger.debug("Log rotating %s -> %s" % (sfn, dfn))
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self._filename + ".1"
            if os.path.exists(dfn):
                os.remove(dfn)
            os.rename(self._filename, dfn)
            logger.debug("Log rotating %s -> %s" % (self._filename, dfn))
        self._file = self._open()

    def _should_rollover(self, raw_data):
        """
        Determine if rollover should occur.

        Basically, see if the supplied raw_data would cause the file to exceed
        the size limit we have.
        """
        if self._file is None:                 # delay was set...
            self._file = self._open()
        if self._max_bytes > 0:                   # are we rolling over?
            self._file.seek(0, 2)  # due to non-posix-compliant Windows feature
            if self._file.tell() + len(raw_data) >= self._max_bytes:
                return 1
        return 0


class FancyStdoutStream(CircusPlugin):
    """
    Write output from watchers using different colors along with a
    timestamp.

    If no color is selected a color will be chosen at random. The
    available ascii colors are:

      - red
      - green
      - yellow
      - blue
      - magenta
      - cyan
      - white

    You may also configure the timestamp format as defined by
    datetime.strftime. The default is: ::

      %Y-%M-%d %H:%M:%S

    Here is an example: ::

      [watcher:foo]
      cmd = python -m myapp.server
      stdout_stream.class = FancyStdoutStream
      stdout_stream.color = green
      stdout_stream.time_format = '%Y/%M/%d | %H:%M:%S'
    """

    # colors in order according to the ascii escape sequences
    colors = ['red', 'green', 'yellow', 'blue',
              'magenta', 'cyan', 'white']

    # Where we write output
    out = sys.stdout

    # Generate a datetime object
    now = datetime.now

    def __init__(self, color=None, time_format=None, *args, **kwargs):
        self.time_format = time_format or '%Y-%M-%d %H:%M:%S'

        # If no color is provided we pick one at random
        if color not in self.colors:
            color = random.choice(self.colors)

        self.color_code = self.colors.index(color) + 1

    def prefix(self, pid):
        """
        Create a prefix for each line.

        This includes the ansi escape sequence for the color. This
        will not work on windows. For something more robust there is a
        good discussion over on Stack Overflow:

        http://stackoverflow.com/questions/287871/print-in-terminal-with-colors-using-python
        """
        time = self.now().strftime(self.time_format)

        # start the coloring with the ansi escape sequence
        color = '\033[0;3%s;40m' % self.color_code

        prefix = '{topic} {time} [{pid}] | '.format(topic=self.topic,
                                                    pid=pid,
                                                    time=time)
        return color + prefix

    def __call__(self, data):
        for line in data['data'].split('\n'):
            if line:
                self.out.write(self.prefix(data['pid']))
                self.out.write(line)
                # stop coloring
                self.out.write('\033[0m\n')
                self.out.flush()

    def handle_recv(self, data):
        topic, message = data
        if topic == self.topic:
            self(json.loads(message))
