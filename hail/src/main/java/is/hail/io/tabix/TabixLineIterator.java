package is.hail.io.tabix;

import java.io.IOException;

public interface TabixLineIterator
{
    /** return null when there is no more data to read */
    public String next() throws IOException;
}
