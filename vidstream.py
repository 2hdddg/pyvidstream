from subprocess import PIPE, Popen
from threading import Thread
from Queue import Queue, Empty
import signal
import re
from collections import namedtuple
import logging


QmapFrame = namedtuple('QmapFrame', ['type', 'qmap'])

_logger = logging.getLogger(__name__)
if not len(_logger.handlers):
    _logger.addHandler(logging.StreamHandler())
    _logger.setLevel(logging.INFO)


class QmapParser:
    """ Parses ffprobe 'debug pq' output
    """
    def __init__(self, collect):
        self._type = None
        self._qmap = []
        self._collect = collect

        self.noise = 0


    def parse_line(self, line):
        """Parses a line of ffprobe output.
        line -- should be on format output by ffprobe
                '-debug qp' command
        return True if parsing should continue
               False if parsing should stop
        """

        m = re.match('^\[.*\] All info found$', line)
        if m:
            self.noise = 0
            _logger.info("Parser encountered end of stream")
            return False

        m = re.match('^\[.*\] New frame, type: ([IPB])$', line)
        if m:
            cont = True
            if self._type:
                frame = QmapFrame(self._type, self._qmap)
                _logger.info("Parsed frame, collecting..")
                cont = self._collect(frame)
            self._type = m.group(1)
            self._qmap = []
            self.noise = 0

            return cont

        m = re.match('^\[.*\] (\d*)$', line)
        if m:
            digits = m.group(1)
            for i in xrange(0, len(digits), 2):
                qp = int(digits[i:i+2])
                self._qmap.append(qp)
            self.noise = 0

            return True

        # When not matching, increase noise level
        self.noise = self.noise + 1
        _logger.debug("Unknown line, increasing noise to %d:\n %s", self.noise, line)

        return True


def _put_line_in_queue(f, queue):
    for line in iter(f.readline, ''):
        queue.put(line)


def _process_output(process, f, parser, line_timeout=3, max_num_timeouts=3, max_noise=70):
    queue = Queue()
    thread = Thread(target=_put_line_in_queue, args=(f, queue))
    thread.start()
    num_timeouts = 0

    while True:
        try:
            line = queue.get(timeout=line_timeout)
        except Empty:
            """ Timed out while waiting for a new line in queue,
            this could mean that the stream is alive but just
            slow. """
            if process.poll() is not None:
                _logger.error("Watched process exited with %d, aborting", process.returncode)
                break
            else:
                num_timeouts = num_timeouts + 1
                _logger.warning("Got line timeout number %d of %d" % (num_timeouts, max_num_timeouts))
                if num_timeouts > max_num_timeouts:
                    _logger.error("Reached max number of timeouts, aborting")
                    break
        else:
            if not parser.parse_line(line):
                break

            if parser.noise > max_noise:
                _logger.error("Exceeded noise level %d, max is %d, aborting", (parser.noise, max_noise))
                break

    process.send_signal(signal.SIGINT)


def get_n_qmaps(n, source, line_timeout=3):
    """ Retrieves n number of frames from specified source.
    Retrieved frames has type (I/P/B) and a qmap (array of
    qp values)

    n -- number of frames to retrieve
    source -- url or path to video.
              Example: rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov
    line_timeout -- number of seconds to wait for input

    return tuple of success code and array of frames
    """
    frames = []
    def collect(frame):
        frames.append(frame)
        done = len(frames) == n
        if done:
            _logger.info("Collected %d frames, done" % n)
        # Return value indicates if parser should  continue
        return not done


    command = ['ffprobe',
               '-debug', 'qp']
    command.append(source)

    ffprobe = Popen(command, stderr=PIPE, bufsize=1)
    parser = QmapParser(collect=collect)

    """ Setting max_num_timeouts to n to allow one timeout per frame for low framerates.
    max_noise is set to account for very noisy start of stream, could be set to a lower
    value if we decide to ignore some in the start.
    """
    _process_output(ffprobe, ffprobe.stderr, parser, line_timeout=line_timeout, max_num_timeouts=n, max_noise=70)

    return (len(frames)==n, frames)


def get_n_gops(n, source, line_timeout=3):
    pass


if __name__ == '__main__':
    # Should succeed in getting 6 frames
    result = get_n_qmaps(n=6, source="rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov")
    print "ok" if result[0] and len(result[1]) == 6 else "nok"
    # Should fail when trying to get more than 6 frames
    result = get_n_qmaps(n=7, source="rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov")
    print "ok" if not result[0] and len(result[1]) == 6 else "nok"


