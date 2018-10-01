package is.hail.io.tabix;

import htsjdk.samtools.seekablestream.ISeekableStreamFactory;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.seekablestream.SeekableStreamFactory;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.tribble.util.ParsingUtils;
import htsjdk.tribble.util.TabixUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TabixReader {
    private final String mFilePath;
    private final String mIndexPath;
    private final BlockCompressedInputStream mFp;

    private int mPreset;
    private int mSc;
    private int mBc;
    private int mEc;
    private int mMeta;

    //private int mSkip; (not used)
    private String[] mSeq;

    private Map<String, Integer> mChr2tid;

    private static int MAX_BIN = 37450;
    //private static int TAD_MIN_CHUNK_GAP = 32768; (not used)
    private static int TAD_LIDX_SHIFT = 14;
    /** default buffer size for <code>readLine()</code> */
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    protected static class TIndex {
        HashMap<Integer, TPair64[]> b; // binning index
        long[] l; // linear index
    }

    protected TIndex[] mIndex;

    static boolean less64(final long u, final long v) { // unsigned 64-bit comparison
        return (u < v) ^ (u < 0) ^ (v < 0);
    }

    /**
     * @param filePath path to the data file/uri
     */
    public TabixReader(final String filePath) throws IOException {
        this(filePath, null, SeekableStreamFactory.getInstance().getBufferedStream(SeekableStreamFactory.getInstance().getStreamFor(filePath)));
    }

    /**
     * @param filePath path to the of the data file/uri
     * @param indexPath Full path to the index file. Auto-generated if null
     */
    public TabixReader(final String filePath, final String indexPath) throws IOException {
        this(filePath, indexPath, SeekableStreamFactory.getInstance().getBufferedStream(SeekableStreamFactory.getInstance().getStreamFor(filePath)));
    }

    /**
     * @param filePath Path to the data file  (used for error messages only)
     * @param stream Seekable stream from which the data is read
     */
    public TabixReader(final String filePath, SeekableStream stream) throws IOException {
        this(filePath, null, stream);
    }

    /**
     * @param filePath Path to the data file (used for error messages only)
     * @param indexPath Full path to the index file. Auto-generated if null
     * @param stream Seekable stream from which the data is read
     */
    public TabixReader(final String filePath, final String indexPath, SeekableStream stream) throws IOException {
        mFilePath = filePath;
        mFp = new BlockCompressedInputStream(stream);
        if(indexPath == null){
            mIndexPath = ParsingUtils.appendToPath(filePath, TabixUtils.STANDARD_INDEX_EXTENSION);
        } else {
            mIndexPath = indexPath;
        }
        readIndex();
    }

    /** return the source (filename/URL) of that reader */
    public String getSource()
    {
        return this.mFilePath;
    }

    private static int reg2bins(final int beg, final int _end, final int[] list) {
        int i = 0, k, end = _end;
        if (beg >= end) return 0;
        if (end >= 1 << 29) end = 1 << 29;
        --end;
        list[i++] = 0;
        for (k = 1 + (beg >> 26); k <= 1 + (end >> 26); ++k) list[i++] = k;
        for (k = 9 + (beg >> 23); k <= 9 + (end >> 23); ++k) list[i++] = k;
        for (k = 73 + (beg >> 20); k <= 73 + (end >> 20); ++k) list[i++] = k;
        for (k = 585 + (beg >> 17); k <= 585 + (end >> 17); ++k) list[i++] = k;
        for (k = 4681 + (beg >> 14); k <= 4681 + (end >> 14); ++k) list[i++] = k;
        return i;
    }

    public static int readInt(final InputStream is) throws IOException {
        byte[] buf = new byte[4];
        is.read(buf);
        return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public static long readLong(final InputStream is) throws IOException {
        final byte[] buf = new byte[8];
        is.read(buf);
        return ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * reads a line with a defined buffer-size
     *
     * @param is the input stream
     * @param bufferCapacity the buffer size, must be greater than 0
     * @return the line or null if there is no more input
     * @throws IOException
     */
    static String readLine(final InputStream is, final int bufferCapacity) throws IOException {
        final StringBuffer buf = new StringBuffer(bufferCapacity);
        int c;
        while ((c = is.read()) >= 0 && c != '\n')
            buf.append((char) c);
        if (c < 0) return null;
        return buf.toString();
    }



    /**
     * Read the Tabix index from a file
     *
     * @param fp File pointer
     */
    private void readIndex(final SeekableStream fp) throws IOException {
        if (fp == null) return;
        final  BlockCompressedInputStream is = new BlockCompressedInputStream(fp);
        byte[] buf = new byte[4];

        is.read(buf, 0, 4); // read "TBI\1"
        mSeq = new String[readInt(is)]; // # sequences
        mChr2tid = new HashMap<String, Integer>( this.mSeq.length );
        mPreset = readInt(is);
        assert(mPreset == 2);

        mSc = readInt(is);
        assert(mSc == 0);

        mBc = readInt(is);
        assert(mBc == 1);

        mEc = readInt(is);
        assert(mEc == 1);

        mMeta = readInt(is);
        assert(mMeta == '#');

        readInt(is);//unused
        // read sequence dictionary
        int i, j, k, l = readInt(is);
        buf = new byte[l];
        is.read(buf);
        for (i = j = k = 0; i < buf.length; ++i) {
            if (buf[i] == 0) {
                byte[] b = new byte[i - j];
                System.arraycopy(buf, j, b, 0, b.length);
                final String contig = new String(b);
                mChr2tid.put(contig, k);
                mSeq[k++] = contig;
                j = i + 1;
            }
        }
        // read the index
        mIndex = new TIndex[mSeq.length];
        for (i = 0; i < mSeq.length; ++i) {
            // the binning index
            int n_bin = readInt(is);
            mIndex[i] = new TIndex();
            mIndex[i].b = new HashMap<Integer, TPair64[]>(n_bin);
            for (j = 0; j < n_bin; ++j) {
                int bin = readInt(is);
                TPair64[] chunks = new TPair64[readInt(is)];
                for (k = 0; k < chunks.length; ++k) {
                    long u = readLong(is);
                    long v = readLong(is);
                    chunks[k] = new TPair64(u, v); // in C, this is inefficient
                }
                mIndex[i].b.put(bin, chunks);
            }
            // the linear index
            mIndex[i].l = new long[readInt(is)];
            for (k = 0; k < mIndex[i].l.length; ++k)
                mIndex[i].l[k] = readLong(is);
        }
        // close
        is.close();
    }

    /**
     * Read the Tabix index from the default file.
     */
    private void readIndex() throws IOException {
        final ISeekableStreamFactory ssf = SeekableStreamFactory.getInstance();
        readIndex(ssf.getBufferedStream(ssf.getStreamFor(mIndexPath), 128000));
    }

    /**
     * Read one line from the data file.
     */
    public String readLine() throws IOException {
        return readLine(mFp, DEFAULT_BUFFER_SIZE);
    }

    /** return chromosome ID or -1 if it is unknown */
    public int chr2tid(final String chr) {
        final Integer tid = mChr2tid.get(chr);
        return tid==null?-1:tid;
    }

    /** return the chromosomes in that tabix file */
    public Set<String> getChromosomes()
    {
        return Collections.unmodifiableSet(this.mChr2tid.keySet());
    }

    /**
     * Parse a region in the format of "chr1", "chr1:100" or "chr1:100-1000"
     *
     * @param reg Region string
     * @return An array where the three elements are sequence_id,
     *         region_begin and region_end. On failure, sequence_id==-1.
     */
    public int[] parseReg(final String reg) { // FIXME: NOT working when the sequence name contains : or -.
        String chr;
        int colon, hyphen;
        int[] ret = new int[3];
        colon = reg.indexOf(':');
        hyphen = reg.indexOf('-');
        chr = colon >= 0 ? reg.substring(0, colon) : reg;
        ret[1] = colon >= 0 ? Integer.parseInt(reg.substring(colon + 1, hyphen >= 0 ? hyphen : reg.length())) - 1 : 0;
        ret[2] = hyphen >= 0 ? Integer.parseInt(reg.substring(hyphen + 1)) : 0x7fffffff;
        ret[0] = this.chr2tid(chr);
        return ret;
    }

    public TPair64[] queryPairs(final int tid, final int beg, final int end) {
        TPair64[] off, chunks;
        long min_off;
        if(tid< 0 || tid>=this.mIndex.length) return new TPair64[0];
        TIndex idx = mIndex[tid];
        int[] bins = new int[MAX_BIN];
        int i, l, n_off, n_bins = reg2bins(beg, end, bins);
        if (idx.l.length > 0)
            min_off = (beg >> TAD_LIDX_SHIFT >= idx.l.length) ? idx.l[idx.l.length - 1] : idx.l[beg >> TAD_LIDX_SHIFT];
        else min_off = 0;
        for (i = n_off = 0; i < n_bins; ++i) {
            if ((chunks = idx.b.get(bins[i])) != null)
                n_off += chunks.length;
        }
        if (n_off == 0) return new TPair64[0];
        off = new TPair64[n_off];
        for (i = n_off = 0; i < n_bins; ++i)
            if ((chunks = idx.b.get(bins[i])) != null)
                for (int j = 0; j < chunks.length; ++j)
                    if (less64(min_off, chunks[j].v))
                        off[n_off++] = new TPair64(chunks[j]);
        Arrays.sort(off, 0, n_off);
        // resolve completely contained adjacent blocks
        for (i = 1, l = 0; i < n_off; ++i) {
            if (less64(off[l].v, off[i].v)) {
                ++l;
                off[l].u = off[i].u;
                off[l].v = off[i].v;
            }
        }
        n_off = l + 1;
        // resolve overlaps between adjacent blocks; this may happen due to the merge in indexing
        for (i = 1; i < n_off; ++i)
            if (!less64(off[i - 1].v, off[i].u)) off[i - 1].v = off[i].u;
        // merge adjacent blocks
        for (i = 1, l = 0; i < n_off; ++i) {
            if (off[l].v >> 16 == off[i].u >> 16) off[l].v = off[i].v;
            else {
                ++l;
                off[l].u = off[i].u;
                off[l].v = off[i].v;
            }
        }
        n_off = l + 1;
        // return
        TPair64[] ret = new TPair64[n_off];
        for (i = 0; i < n_off; ++i) {
            if (off[i] != null) ret[i] = new TPair64(off[i].u, off[i].v); // in C, this is inefficient
        }
        if (ret.length == 0 || (ret.length == 1 && ret[0] == null))
            return new TPair64[0];
        return ret;
    }

    // ADDED BY JTR
    public void close() {
        if(mFp != null) {
            try {
                mFp.close();
            } catch (IOException e) {

            }
        }
    }

    @Override
    public String toString() {
        return "TabixReader: filename:"+getSource();
    }
}
