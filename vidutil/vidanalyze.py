class FrameSeqException(Exception):
    def __init__(self, msg):
        self.msg = msg


def split_frames_by_missing(frames):
    """ Splits frames into an array of frames
    sequences where each sequence does NOT
    contain any missing frames.
    If no frames are missing an array with one
    entry (frames) are returned.
    """
    if len(frames) == 0:
        return [[]]

    f = frames[0]
    expected_num = f.coded_picture_number + 1
    cur = [f]
    seq = []

    for f in frames[1:]:
        next_num = f.coded_picture_number

        if next_num == expected_num:
            cur.append(f)
            expected_num = f.coded_picture_number + 1
        elif next_num > expected_num:
            seq.append(cur)
            cur = [f]
            expected_num = f.coded_picture_number + 1
        else:
            s = "Unexpected coded_picture_number %d " \
                "should be larger than or equal to %d" % \
                (next_num, expected_num)
            e = FrameSeqException(s)
            raise e

    seq.append(cur)

    return seq


def are_frames_missing(frames):
    """ Checks that there are no gaps in coded
    picure number. Does NOT check that there is
    a constant time between frames.
    """
    splits = split_frames_by_missing(frames)

    return len(splits) > 1


def is_fps_fixed(frames):
    """ Checks if there is a constant time
    between frames (fixed) or not.
    Does NOT check for gaps in coded picture
    number (lost frames).
    """
    pass


def calculate_fps(frames):
    """ Calculates an average fps based on
    the frames.
    """
    pass


def calculate_bitrate(frames):
    """ Calculates an average bitrate based
    on the frames.
    """
    pass


def are_gops_fixed(gops):
    """ Checks if each gop consists of the same
    number of frames. Needs more than one gop.
    """
    pass


def calculate_gop_size(gops):
    """ Calculates an average gop size (number of frames
    including first key frame) based on the gops.
    """
    pass
