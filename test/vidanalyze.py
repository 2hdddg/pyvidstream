from unittest import TestCase
from vidutil.vidstream import Frame
import vidutil.vidanalyze as a


def _num_f(num):
    f = Frame(type='P', key_frame=False, width=1, height=1,
              coded_picture_number=num)
    return f


class TestSplitFramesMissing(TestCase):
    def test_no_missing(self):
        """ Verifies that a list of frames with no missing
        frames are not split.
        """
        frames = [
            _num_f(1),
            _num_f(2),
            _num_f(3)
        ]

        splitted = a.split_frames_by_missing(frames)

        self.assertEqual(len(splitted), 1)
        self.assertListEqual(frames, splitted[0])

    def test_one_missing(self):
        """ Verifies that a list of frames with a missing
        frame in the middle are split into two parts.
        """
        frames = [
            _num_f(1),
            _num_f(2),
            _num_f(4),
            _num_f(5)
        ]

        splitted = a.split_frames_by_missing(frames)

        self.assertEqual(len(splitted), 2)
        self.assertListEqual(frames[0:2], splitted[0])
        self.assertListEqual(frames[2:4], splitted[1])

    def test_two_missing(self):
        """ Verifies that a list of frames with two missing
        frames are split into three parts.
        """
        frames = [
            _num_f(1),
            _num_f(4),
            _num_f(5),
            _num_f(9),
            _num_f(10),
            _num_f(11),
        ]

        splitted = a.split_frames_by_missing(frames)

        self.assertEqual(len(splitted), 3)
        self.assertListEqual(frames[0:1], splitted[0])
        self.assertListEqual(frames[1:3], splitted[1])
        self.assertListEqual(frames[3:6], splitted[2])

    def test_empty(self):
        """ Verifies that an empty list is returned
        as an empty list.
        """
        splitted = a.split_frames_by_missing([])

        self.assertEqual(len(splitted), 1)
        self.assertListEqual([], splitted[0])

    def test_number_out_of_order(self):
        """ Test that an exception is thrown if the
        numbers are out of order
        """
        frames = [
            _num_f(2),
            _num_f(1)
        ]

        with self.assertRaises(a.FrameSeqException):
            a.split_frames_by_missing(frames)

    def test_same_number(self):
        """ Test that an exception is thrown if same
        number occures twice in a row
        """
        frames = [
            _num_f(2),
            _num_f(2)
        ]

        with self.assertRaises(a.FrameSeqException):
            a.split_frames_by_missing(frames)


class TestAreFramesMissing(TestCase):
    def test_no_missing(self):
        """ Tests that False is returned when no
        frames are missing
        """
        frames = [
            _num_f(2),
            _num_f(3),
            _num_f(4),
        ]

        self.assertFalse(a.are_frames_missing(frames))

    def test_missing(self):
        """ Tests that True is returned when
        frames are missing
        """
        frames = [
            _num_f(2),
            _num_f(4),
            _num_f(5),
        ]

        self.assertTrue(a.are_frames_missing(frames))
