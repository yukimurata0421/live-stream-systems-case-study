from __future__ import annotations

import unittest
from pathlib import Path

from stream_core.engine.delivery_pipeline import (
    BufferPolicy, DeliveryBlocked, PacketBuffer, producer_args, sender_args,
)


def packet(pid: int, payload: bytes, *, start: bool = False, keyframe: bool = False) -> bytes:
    head = bytes([0x47, (0x40 if start else 0) | (pid >> 8), pid & 255, 0x30 if keyframe else 0x10])
    body = bytes([1, 0x40]) + payload if keyframe else payload
    return (head + body).ljust(188, b'\xff')


PAT = packet(0, bytes.fromhex('0000b00d0001c100000001f00000000000'), start=True)
PMT = packet(0x1000, bytes.fromhex('0002b0170001c10000e100f0001be100f0000fe101f00000000000'), start=True)
IDR = packet(0x100, b'\x00\x00\x01\xe0' + b'keyframe', start=True, keyframe=True)
DELTA = packet(0x100, b'delta-frame')
AUDIO = packet(0x101, b'audio-frame')


class DeliveryBufferTests(unittest.TestCase):
    def setUp(self):
        self.now = 10.0
        self.buffer = PacketBuffer(clock=lambda: self.now)

    def test_waits_for_codec_tables_and_video_keyframe(self):
        self.buffer.feed(DELTA + AUDIO)
        cursor, data, reset = self.buffer.read(None)
        self.assertIsNone(cursor)
        self.assertEqual(data, b'')
        self.buffer.feed(PAT + PMT + IDR + AUDIO)
        cursor, data, reset = self.buffer.read(None)
        self.assertTrue(reset)
        self.assertEqual(data, PAT + PMT + IDR + AUDIO)
        self.assertEqual(cursor, 6)

    def test_partial_packets_are_reassembled(self):
        data = PAT + PMT + IDR
        self.buffer.feed(data[:191])
        self.buffer.feed(data[191:])
        self.assertEqual(self.buffer.read(None)[1], data)

    def test_reads_each_packet_once_after_preamble(self):
        self.buffer.feed(PAT + PMT + IDR + AUDIO)
        cursor, _, _ = self.buffer.read(None)
        self.assertEqual(self.buffer.read(cursor)[1], b'')
        self.buffer.feed(DELTA)
        self.assertEqual(self.buffer.read(cursor)[1], DELTA)

    def test_overflow_drops_old_data_and_resumes_at_new_keyframe(self):
        b = PacketBuffer(BufferPolicy(maximum_bytes=188 * 64), clock=lambda: self.now)
        b.feed(PAT + PMT + IDR)
        cursor, _, _ = b.read(None)
        b.feed(DELTA * 80 + IDR + AUDIO)
        next_cursor, data, reset = b.read(cursor)
        self.assertTrue(reset)
        self.assertEqual(data, PAT + PMT + IDR + AUDIO)
        self.assertLessEqual(b.snapshot()['buffer_bytes'], 188 * 64)
        self.assertGreater(b.snapshot()['dropped_packets'], 0)

    def test_age_limit_removes_old_keyframe(self):
        self.buffer.feed(PAT + PMT + IDR)
        self.now += 3.1
        self.buffer.feed(DELTA)
        self.assertFalse(self.buffer.snapshot()['keyframe_available'])
        self.assertEqual(self.buffer.read(None)[1], b'')

    def test_stale_and_closed_input_do_not_serve_old_data(self):
        self.buffer.feed(PAT + PMT + IDR)
        self.now += 3.001
        with self.assertRaisesRegex(DeliveryBlocked, 'INPUT_STALE'):
            self.buffer.read(None)
        self.buffer.close()
        with self.assertRaisesRegex(DeliveryBlocked, 'PRODUCER_CLOSED'):
            self.buffer.read(None)

    def test_backward_clock_blocks_delivery(self):
        self.buffer.feed(PAT + PMT + IDR)
        self.now -= 1
        with self.assertRaisesRegex(DeliveryBlocked, 'CLOCK_REGRESSION'):
            self.buffer.read(None)

    def test_invalid_alignment_and_large_chunk_are_rejected(self):
        with self.assertRaisesRegex(DeliveryBlocked, 'CHUNK_TOO_LARGE'):
            self.buffer.feed(b'x' * (188 * 257))
        with self.assertRaisesRegex(DeliveryBlocked, 'ALIGNMENT_INVALID'):
            self.buffer.feed(b'x' * 188)

    def test_producer_is_local_only_and_keeps_original_encoder_arguments(self):
        old = ['-f', 'flv', 'rtmps://a.rtmps.youtube.com/live2/dummy']
        original = ['ffmpeg', '-c:v', 'h264_nvenc', '-b:v', '3400k', *old]
        new = producer_args(original, old)
        self.assertEqual(new[:5], original[:5])
        self.assertEqual(new[-1], 'pipe:1')
        self.assertFalse(any('rtmp' in v for v in new))
        with self.assertRaisesRegex(DeliveryBlocked, 'NOT_EXACT'):
            producer_args(original, ['-f', 'flv', 'wrong'])

    def test_sender_uses_copy_tls_validation_and_one_destination(self):
        args = sender_args('rtmps://relay.example:1936/relay', ca_file=Path('/run/relay.crt'))
        self.assertEqual(args[args.index('-c') + 1], 'copy')
        self.assertEqual(args[args.index('-tls_verify') + 1], '1')
        self.assertEqual(args[args.index('-ca_file') + 1], '/run/relay.crt')
        self.assertEqual(sum('rtmps://' in value for value in args), 1)
        for bad in ['rtmp://relay.example:1936/relay', 'rtmps://relay.example:1935/relay',
                    'rtmps://user:pass@relay.example:443/relay', 'rtmps://relay.example/relay?key=bad']:
            with self.subTest(destination=bad), self.assertRaises(DeliveryBlocked):
                sender_args(bad)


if __name__ == '__main__':
    unittest.main()
