import shutil

from wal_e import pipebuf


def copyfileobj(src, dst, length=None):
    """Copy length bytes from fileobj src to fileobj dst.
       If length is None, copy the entire content.
    """
    BUFSIZE = pipebuf.PIPE_BUF_BYTES

    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst, BUFSIZE)
        return

    blocks, remainder = divmod(length, BUFSIZE)
    for b in xrange(blocks):
        buf = src.read(BUFSIZE)
        if len(buf) < BUFSIZE:
            raise IOError("end of file reached")
        dst.write(buf)

    if remainder != 0:
        buf = src.read(remainder)
        if len(buf) < remainder:
            raise IOError("end of file reached")
        dst.write(buf)
    return
