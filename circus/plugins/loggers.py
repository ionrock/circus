from __future__ import print_function
import os
import json

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
