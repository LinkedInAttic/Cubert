/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.io;

import java.io.IOException;
import java.io.InputStream;

public class BlockInputStream extends InputStream
{
    private final InputStream in;
    private long remaining;
    private final long blocksize;

    public BlockInputStream(InputStream in, long blocksize)
    {
        this.in = in;
        this.remaining = blocksize;
        this.blocksize = blocksize;
    }

    @Override
    public int read() throws IOException
    {
        if (remaining == 0)
            return -1;
        int data = in.read();

        if (data != -1)
            remaining--;

        return data;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        long longlen = len;

        if (remaining == 0)
            return -1;

        if (longlen > remaining)
        {
            len = (int) remaining;
        }

        int actualRead = in.read(b, off, len);
        remaining -= actualRead;

        return actualRead;
    }

    @Override
    public long skip(long n) throws IOException
    {
        if (n > remaining)
        {
            n = remaining;
        }

        long actualSkip = in.skip(n);
        remaining -= actualSkip;
        return actualSkip;
    }

    @Override
    public int available() throws IOException
    {
        if (remaining > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;

        return (int) remaining;
    }

    @Override
    public void close() throws IOException
    {
        remaining = 0;
        in.close();
    }

    @Override
    public synchronized void mark(int readlimit)
    {
        in.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException
    {
        in.reset();
    }

    @Override
    public boolean markSupported()
    {
        return in.markSupported();
    }

    public long getBytesRead()
    {
        return (blocksize - remaining);
    }
}
