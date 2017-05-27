from subprocess import PIPE, Popen
from threading import Thread
from Queue import Queue, Empty
import signal
import re
from collections import namedtuple
import logging
import json


""" A frame with a bunch of properties
"""
Frame = namedtuple('Frame', ['type', 'key_frame', 'width', 'height'])

""" A group of pictures defined as a sequence
of frames starting with an I frame that is also
a keyframe (IDR) and all frames after that until
next (but not including) I IDR frame.
frames -- array of frames with the first one
          always being an I IDR frame.
"""
GOP = namedtuple('GOP', ['frames'])

""" A frame with a qmap only has type (I/P/B) and
a qmap that is an array of ints representing the
qp value per macroblock
"""
QmapFrame = namedtuple('QmapFrame', ['type', 'qmap'])


_logger = None
def _init_logging():
    global _logger
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
            _logger.debug("Parser found start of new frame")
            cont = True
            if self._type:
                frame = QmapFrame(self._type, self._qmap)
                cont = self._collect(frame)
            self._type = m.group(1)
            self._qmap = []
            self.noise = 0

            return cont

        m = re.match('^\[.*\] (\d*)$', line)
        if m:
            _logger.debug("Parser found qmap digits")
            digits = m.group(1)
            for i in xrange(0, len(digits), 2):
                qp = int(digits[i:i+2])
                self._qmap.append(qp)
            self.noise = 0

            return True

        # When not matching, increase noise level
        self.noise = self.noise + 1
        _logger.debug("Parser unknown line, increasing noise to %d:\n %s", self.noise, line)

        return True


class FrameParser:
    """ Parses ffprobe --show_frames compact json output
    """
    def __init__(self, collect):
        self.noise = 0
        self._collect = collect

    def _parse_json(self, j):
        if j['media_type'] != 'video':
            return True

        key_frame = j['key_frame'] == 1
        pict_type = j['pict_type']
        width = j['width']
        height = j['height']

        frame = Frame(type=pict_type, key_frame=key_frame, width=width, height=height)

        # Parsing ok, reset noise
        self.noise = 0

        return self._collect(frame)

    def parse_line(self, line):
        line = line.strip().rstrip(',')

        if line == '}':
            _logger.info("Parser encountered end of stream")
            return False

        try:
            j = json.loads(line)
        except ValueError as e:
            self.noise = self.noise + 1
            _logger.debug("Parser unknown line, increasing noise to %d:\n %s", self.noise, line)
            return True

        return self._parse_json(j)


def _put_line_in_queue(f, queue):
    # To be executed in a separate thread
    for line in iter(f.readline, ''):
        queue.put(line)


def _process_output(process, f, parser, line_timeout=3, max_num_timeouts=3, max_noise=70):
    # Will contain lines to parse
    queue = Queue()

    # Read lines from separate thread
    thread = Thread(target=_put_line_in_queue, args=(f, queue))
    thread.start()

    # Fail when max_num_timeouts reached
    num_timeouts = 0

    while True:
        try:
            line = queue.get(timeout=line_timeout)
            _logger.debug("Got line to be parsed")
        except Empty:
            """ Timed out while waiting for a new line in queue,
            this could mean that the stream is alive or slow... """
            if process.poll() is not None:
                _logger.error("Watched process exited with %d, aborting", process.returncode)
                break
            else:
                num_timeouts = num_timeouts + 1
                _logger.warn("Got line timeout number %d of %d", num_timeouts, max_num_timeouts)
                if num_timeouts > max_num_timeouts:
                    _logger.error("Reached max number of timeouts, aborting")
                    break
        else:
            try:
                cont = parser.parse_line(line)
            except:
                _logger.exception("Parser exception")
                break

            if not cont:
                break

            if parser.noise > max_noise:
                _logger.error("Exceeded noise level %d, max is %d, aborting", parser.noise, max_noise)
                break

    # Let the process finish up nicely
    process.send_signal(signal.SIGINT)
    # Could also wait and terminate to ensure exited


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
    _init_logging()
    frames = []

    def collect(frame):
        frames.append(frame)
        _logger.debug("Collected frame %d", len(frames))
        done = len(frames) == n
        if done:
            _logger.info("Collected %d frames, done" % n)
        # Return value indicates if parser should  continue
        return not done


    command = ['ffprobe',
               '-v', 'quiet',
               '-show_frames', # Need something...
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


def get_n_frames(n, source, line_timeout=30):
    _init_logging()
    frames = []

    def collect(frame):
        frames.append(frame)
        _logger.debug("Collected frame %d", len(frames))
        done = len(frames) == n
        if done:
            _logger.info("Collected %d frames, done" % n)
        # Return value indicates if parser should  continue
        return not done


    command = ['ffprobe',
               '-show_frames',
               '-v', 'quiet',
               '-print_format', 'json=compact=1']
    command.append(source)

    ffprobe = Popen(command, stdout=PIPE, bufsize=1)
    parser = FrameParser(collect=collect)

    _process_output(ffprobe, ffprobe.stdout, parser, line_timeout=line_timeout, max_num_timeouts=n, max_noise=70)

    return (len(frames)==n, frames)


def get_n_gops(n, source, line_timeout=30):
    _init_logging()
    gops = []
    state = { 'frames': None, 'gops': gops }

    def collect(frame):

        if frame.key_frame and frame.type == 'I':
            # Start of new gop
            if state['frames']:
                gop = GOP(frames=state['frames'])
                state['gops'].append(gop)

            state['frames'] = []
            state['frames'].append(frame)
            done = len(state['gops']) == n
            if done:
                _logger.info("Collected %d gops, done" % n)

            return not done

        if state['frames']:
            _logger.debug("Collecting frame to gop")
            state['frames'].append(frame)
        else:
            _logger.info("Skipping frame before start of first gop")

        return True


    command = ['ffprobe',
               '-show_frames',
               '-v', 'quiet',
               '-print_format', 'json=compact=1']
    command.append(source)

    ffprobe = Popen(command, stdout=PIPE, bufsize=1)
    parser = FrameParser(collect=collect)

    _process_output(ffprobe, ffprobe.stdout, parser, line_timeout=line_timeout, max_num_timeouts=n, max_noise=70)

    return (len(gops)==n, gops)


if __name__ == '__main__':
    result = get_n_qmaps(n=6, source="rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov")
    print "ok" if result[0] and len(result[1]) == 6 else "nok"
    result = get_n_frames(10, source="rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov")
    print "ok" if result[0] and len(result[1]) == 10 else "nok"
    result = get_n_gops(2, source="rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov")
    print "ok" if result[0] and len(result[1]) == 2 else "nok"

