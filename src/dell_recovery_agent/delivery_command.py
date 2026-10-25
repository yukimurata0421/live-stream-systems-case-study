"""Recognize one RTMPS FLV output, including FFmpeg's internal FIFO muxer."""


def is_delivery_command(raw: bytes) -> bool:
    argv = [v.decode('utf-8', errors='replace') for v in raw.split(b'\0') if v]
    if not argv or argv[0] != 'ffmpeg' or not argv[-1].startswith('rtmps://'):
        return False
    if sum(v.startswith(('rtmp://', 'rtmps://')) for v in argv) != 1:
        return False
    formats = [argv[i + 1] for i, value in enumerate(argv[:-1]) if value == '-f']
    if not formats:
        return False
    if formats[-1] == 'flv':
        return True
    if formats[-1] != 'fifo':
        return False
    inner = [argv[i + 1] for i, value in enumerate(argv[:-1]) if value == '-fifo_format']
    return inner == ['flv']
