from subprocess import PIPE, Popen
from threading import Thread
from Queue import Queue, Empty
import signal
import re
from collections import namedtuple


QmapFrame = namedtuple('QmapFrame', ['type', 'qmap'])


class QmapParser:
    def __init__(self):
        self._type = None
        self._qmap = []
        self.frames = []


    def parse_line(self, line):
        """Parses a line of ffprobe output.
        line -- should be on format output by ffprobe
                '-debug qp' command
        return True if parsing should continue
               False if parsing should stop
        """
        m = re.match('^\[.*\] All info found$', line)
        if m:
            return False

        m = re.match('^\[.*\] New frame, type: ([IPB])$', line)
        if m:
            if self._type:
                frame = QmapFrame(self._type, self._qmap)
                self.frames.append(frame)
                print frame
            self._type = m.group(1)
            self._qmap = []

            return True

        m = re.match('^\[.*\] (\d*)$', line)
        if m:
            digits = m.group(1)
            for i in xrange(0, len(digits), 2):
                qp = int(digits[i:i+2])
                self._qmap.append(qp)

        return True


def _put_line_in_queue(f, queue):
    for line in iter(f.readline, ''):
        queue.put(line)


def get_qmap_from_n_frames(n, source, line_timeout=3):
    """ Retrieves n number of frames from specified source.
    Retrieved frames has type (I/P/B) and a qmap (array of
    qp values)

    n -- number of frames to retrieve
    source -- url or path to video.
              Example: rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov
    line_timeout -- number of seconds to wait for input

    return tuple of success code and array of frames
    """
    command = ['ffprobe',
               '-debug', 'qp']
    command.append(source)

    ffprobe = Popen(command, stderr=PIPE, bufsize=1)
    queue = Queue()
    thread = Thread(target=_put_line_in_queue, args=(ffprobe.stderr, queue))
    thread.start()
    parser = QmapParser()
    num_timeouts = 0

    while len(parser.frames) < n:
        try:
            line = queue.get(timeout=line_timeout)
        except Empty:
            """ Timed out while waiting for a new line in queue,
            this could mean that the stream is alive but just
            slow. """
            if ffprobe.poll() is not None:
                print "ffprobe exited unexpected"
                break
            else:
                print "Timeout while waiting for line"
                num_timeouts = num_timeouts + 1
                if num_timeouts > n:
                    break
        else:
            if not parser.parse_line(line):
                print "End of stream"
                break

    ffprobe.send_signal(signal.SIGINT)

    return (len(parser.frames)==n, parser.frames)


if __name__ == '__main__':
    result = get_qmap_from_n_frames(n=6, source="rtsp://184.72.239.149/vod/mp4:BigBuckBunny_175k.mov")
    if not result[0]:
        print "Failed to retrieve sufficient number of frames, got %d" % len(result[1])
    else:
        print "Got correct number of frames: %d" % len(result[1])

