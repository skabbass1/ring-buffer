from multiprocessing import shared_memory

WRITE_INDEX_BUFFER_SIZE = 8
READ_INDEX_BUFFER_SIZE = 4
DATA_BLOCK_SIZE = 16


class ReaderCollisionError(Exception):
    pass


class WriterCollisionError(Exception):
    pass


class RingBuffer:
    def __init__(self, name, size, create):
        self.name = name
        self.size = size
        self.create = create

        self.data_buff = None
        self.write_index_buff = None
        self.read_index_buff = None

        self._create_or_attach_buff()

    def put(self, b):
        if self._wrap_around(self.writer_pos()):
            start_offset = 0
            end_offset = start_offset + DATA_BLOCK_SIZE
            if self._overtaking_read(
                start_offset, self.reader_pos(), self.watermark_pos()
            ):
                raise ReaderCollisionError(
                    f"Reader cursor at {self.reader_pos()}. Writer cursor attemping to write [{start_offset}:{end_offset}]"
                )
            else:
                self._put(b, start_offset, end_offset)

                self.write_index_buff.buf[4:8] = self.writer_pos().to_bytes(
                    4, byteorder="little"
                )
                self.write_index_buff.buf[:4] = end_offset.to_bytes(
                    4, byteorder="little"
                )
        else:
            start_offset = self.writer_pos()
            end_offset = start_offset + DATA_BLOCK_SIZE

            if self._overtaking_read(
                start_offset, self.reader_pos(), self.watermark_pos()
            ):
                raise ReaderCollisionError(
                    f"Reader cursor at {self.reader_pos()}. Writer cursor attemping to write [{start_offset}:{end_offset}]"
                )

            self._put(b, start_offset, end_offset)

            self.write_index_buff.buf[:4] = end_offset.to_bytes(4, byteorder="little")
            if self.watermark_pos() < end_offset:
                self.write_index_buff.buf[4:8] = end_offset.to_bytes(
                    4, byteorder="little"
                )

    def get(self):
        rpos = self.reader_pos()
        wpos = self.writer_pos()
        wmpos = self.watermark_pos()

        if wpos < rpos:
            if wmpos - rpos >= DATA_BLOCK_SIZE:
                data = self._get(rpos)
                self.read_index_buff.buf[:4] = (rpos + DATA_BLOCK_SIZE).to_bytes(
                    4, byteorder="little"
                )
                return data
            else:
                if wpos >= DATA_BLOCK_SIZE:
                    data = self._get(0)
                    self.read_index_buff.buf[:4] = DATA_BLOCK_SIZE.to_bytes(
                        4, byteorder="little"
                    )
                    return data
                else:
                    raise WriterCollisionError(
                        f"Writer cursor at {wpos}. Reader cursor attemping to read [0 :{DATA_BLOCK_SIZE}]"
                    )
        else:
            if wpos - rpos >= DATA_BLOCK_SIZE:
                data = self._get(rpos)
                self.read_index_buff.buf[:4] = (rpos + DATA_BLOCK_SIZE).to_bytes(
                    4, byteorder="little"
                )
                return data
            else:
                raise WriterCollisionError(
                    f"Writer cursor at {wpos}. Reader cursor attemping to read [{rpos} :{rpos + DATA_BLOCK_SIZE}]"
                )

    def writer_pos(self):
        return int.from_bytes(self.write_index_buff.buf[:4], byteorder="little")

    def watermark_pos(self):
        return int.from_bytes(self.write_index_buff.buf[4:8], byteorder="little")

    def reader_pos(self):
        return int.from_bytes(self.read_index_buff.buf[:4], byteorder="little")

    def close(self):
        self.data_buff.close()
        self.write_index_buff.close()
        self.read_index_buff.close()

    def destroy(self):
        self.data_buff.close()
        self.data_buff.unlink()

        self.write_index_buff.close()
        self.write_index_buff.unlink()

        self.read_index_buff.close()
        self.read_index_buff.unlink()

    def _put(self, data, start_offset, end_offset):
        self.data_buff.buf[start_offset:end_offset] = data

    def _get(self, start_offset):
        return self.data_buff.buf[start_offset : start_offset + DATA_BLOCK_SIZE]

    def _wrap_around(self, writer_pos):
        return writer_pos + DATA_BLOCK_SIZE > self.size

    def _overtaking_read(self, writer_pos, reader_pos, water_mark_pos):
        if writer_pos < reader_pos:
            return reader_pos < writer_pos + DATA_BLOCK_SIZE
        elif writer_pos == reader_pos:
            return writer_pos < water_mark_pos
        else:
            return False

    def _create_or_attach_buff(self):
        self.data_buff = shared_memory.SharedMemory(
            name=self.name, size=self.size, create=self.create
        )
        self.write_index_buff = shared_memory.SharedMemory(
            name=self._writer_buff_name(),
            size=WRITE_INDEX_BUFFER_SIZE,
            create=self.create,
        )
        self.read_index_buff = shared_memory.SharedMemory(
            name=self._reader_buff_name(),
            size=READ_INDEX_BUFFER_SIZE,
            create=self.create,
        )

    def _writer_buff_name(self):
        return self.name + "-writer"

    def _reader_buff_name(self):
        return self.name + "-reader"
