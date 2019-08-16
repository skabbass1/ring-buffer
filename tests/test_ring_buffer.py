import struct
import random
import string

import pytest

from ring_buffer.ring_buffer import (
    RingBuffer,
    WRITE_INDEX_BUFFER_SIZE,
    READ_INDEX_BUFFER_SIZE,
    ReaderCollisionError,
    WriterCollisionError,
)


def test_creates_buffers_with_right_size_and_name(buffer):
    assert buffer.data_buff.name == "test"
    assert len(buffer.data_buff.buf) == 100

    assert buffer.write_index_buff.name == "test-writer"
    assert len(buffer.write_index_buff.buf) == WRITE_INDEX_BUFFER_SIZE

    assert buffer.read_index_buff.name == "test-reader"
    assert len(buffer.read_index_buff.buf) == READ_INDEX_BUFFER_SIZE


def test_attach_with_existing_buffer(buffer):
    new_buff = RingBuffer(name=buffer.name, size=0, create=False)

    assert new_buff.data_buff.name == "test"
    assert len(new_buff.data_buff.buf) == 100

    assert new_buff.write_index_buff.name == "test-writer"
    assert len(new_buff.write_index_buff.buf) == WRITE_INDEX_BUFFER_SIZE

    assert new_buff.read_index_buff.name == "test-reader"
    assert len(new_buff.read_index_buff.buf) == READ_INDEX_BUFFER_SIZE


def test_writer_and_watermark_offsets_when_no_wrap_around(buffer):
    for m in _messages(6):
        buffer.put(m)

    assert buffer.writer_pos() == 96
    assert buffer.watermark_pos() == 96
    assert buffer.reader_pos() == 0


def test_writer_and_watermark_offsets_when_wrap_around(buffer):
    buffer.read_index_buff.buf[:4] = (32).to_bytes(4, byteorder="little")
    for i, m in enumerate(_messages(7)):
        buffer.put(m)

    assert buffer.writer_pos() == 16
    assert buffer.watermark_pos() == 96
    assert buffer.reader_pos() == 32


def test_writer_does_not_go_past_reader_when_no_wrap_around(buffer):
    with pytest.raises(ReaderCollisionError) as excinfo:
        for i, m in enumerate(_messages(10)):
            buffer.put(m)

    assert (
        str(excinfo.value)
        == "Reader cursor at 0. Writer cursor attemping to write [0:16]"
    )


def test_writer_does_not_go_past_reader_when_wrap_around(buffer):
    buffer.read_index_buff.buf[:4] = (32).to_bytes(4, byteorder="little")
    with pytest.raises(ReaderCollisionError) as excinfo:
        for i, m in enumerate(_messages(10)):
            buffer.put(m)

    assert (
        str(excinfo.value)
        == "Reader cursor at 32. Writer cursor attemping to write [32:48]"
    )


def test_reader_reads_data_and_moves_offset(buffer):
    read_buff = RingBuffer(name=buffer.name, size=0, create=False)

    messages = _messages(3)
    expected = [_decode_message(m) for m in messages]
    for m in messages:
        buffer.put(m)

    got = [_decode_message(read_buff.get()) for _ in range(3)]
    assert got == expected

    assert read_buff.reader_pos() == 48

def test_reader_reads_data_and_moves_offset_when_writer_wraps_around(buffer):
    read_buff = RingBuffer(name=buffer.name, size=0, create=False)

    messages = _messages(8)
    for m in messages[:6]:
        buffer.put(m)

    [read_buff.get() for _ in range(6)]

    for m in messages[6:]:
        buffer.put(m)

    got = [_decode_message(read_buff.get()) for _ in range(2)]
    expected = [_decode_message(m) for m in messages[6:]]
    assert got == expected

    assert read_buff.reader_pos() == 32

def test_reader_does_not_go_past_writer_when_writer_wraps_around(buffer):
    read_buff = RingBuffer(name=buffer.name, size=0, create=False)

    messages = _messages(8)
    for m in messages[:6]:
        buffer.put(m)

    [read_buff.get() for _ in range(6)]

    for m in messages[6:7]:
        buffer.put(m)

    with pytest.raises(WriterCollisionError) as excinfo:
        [read_buff.get() for _ in range(2)]

    assert str(excinfo.value) == "Writer cursor at 16. Reader cursor attemping to read [16 :32]"

def test_reader_does_not_go_past_writer(buffer):
    read_buff = RingBuffer(name=buffer.name, size=0, create=False)

    messages = _messages(3)
    for m in messages:
        buffer.put(m)

    with pytest.raises(WriterCollisionError) as excinfo:
        [read_buff.get() for _ in range(5)]

    assert str(excinfo.value) == "Writer cursor at 48. Reader cursor attemping to read [48 :64]"


def _messages(count):
    messages = []
    for _ in range(count):
        symbol = "".join(
            [random.choice(string.ascii_letters) for _ in range(4)]
        ).encode()
        price = round(random.uniform(100, 115), 2)
        quantity = random.randint(10, 100)
        b = struct.pack("<4sdi", symbol, price, quantity)
        messages.append(b)
    return messages


def _decode_message(buff):
    return struct.unpack_from("<4sdi", buff, 0)


@pytest.fixture(name="buffer")
def create_buffer():
    buff = RingBuffer(name="test", size=100, create=True)

    yield buff

    buff.destroy()
