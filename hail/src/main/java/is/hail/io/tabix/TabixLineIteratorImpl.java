package is.hail.io.tabix;

import htsjdk.samtools.seekablestream.ISeekableStreamFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.seekablestream.SeekableStreamFactory;
import htsjdk.samtools.util.BlockCompressedInputStream;

import java.io.IOException;

public class TabixLineIteratorImpl implements TabixLineIterator {
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    private BlockCompressedInputStream mFp;

    private int i;
    //private int n_seeks;
    private TPair64[] off;
    private long curr_off;
    private boolean iseof;

    public TabixLineIteratorImpl(final String filePath, final TPair64[] _off) throws IOException {
        ISeekableStreamFactory factory = SeekableStreamFactory.getInstance();
        SeekableStream stream = factory.getBufferedStream(factory.getStreamFor(filePath));
        mFp = new BlockCompressedInputStream(stream);
        i = -1;
        //n_seeks = 0;
        curr_off = 0;
        iseof = false;
        off = _off;
    }

    @Override
    public String next() throws IOException {
        if (iseof) return null;
        for (;;) {
            if (curr_off == 0 || !TabixReader.less64(curr_off, off[i].v)) { // then jump to the next chunk
                if (i == off.length - 1) break; // no more chunks
                if (i >= 0) assert (curr_off == off[i].v); // otherwise bug
                if (i < 0 || off[i].v != off[i + 1].u) { // not adjacent chunks; then seek
                    mFp.seek(off[i + 1].u);
                    curr_off = mFp.getFilePointer();
                    //++n_seeks;
                }
                ++i;
            }
            String s;
            if ((s = TabixReader.readLine(mFp, DEFAULT_BUFFER_SIZE)) != null) {
                curr_off = mFp.getFilePointer();
                if (s.isEmpty() || s.charAt(0) == '#') continue;

                // FYI we filter outside
                return s; // overlap; return
            } else
                break; // end of file
        }
        iseof = true;
        return null;
    }
}
