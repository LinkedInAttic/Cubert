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

package com.linkedin.cubert.memory;

import java.io.IOException;
import java.io.InputStream;

/**
 * PagedByteArrayInputStream
 *      Input Stream that reads from a PagedByteArray
 *      Warning: This Object is not thread safe
 *
 * Created by spyne on 7/29/14.
 */
public class PagedByteArrayInputStream extends InputStream
{
    /**
     * the data store from which the read occurs
     */
    private final PagedByteArray pba;
    /**
     * Maintains the current read position
     */
    private int offset = 0;

    /**
     * Create an Input Stream to read from a PagedByteArray
     * @param pba the data source
     */
    public PagedByteArrayInputStream(PagedByteArray pba)
    {
        this.pba = pba;
    }

    /**
     * Read the next byte
     *
     * @return the next byte
     * @throws IOException
     */
    @Override
    public int read() throws IOException
    {
        try
        {
            /**
             *  The & 0xff is necessary.
             *  It converts a negative number in byte (-129 to 127)
             *  into a positive number (0 - 255). Returning a positive byte is expected in
             *  classes like DataInputStream which OR (|) the stream data to retrieve the
             *  original data.
             **/
            return pba.read(offset++) & 0xff;
        } catch (IndexOutOfBoundsException e)
        {
            throw new IOException(e);
        }
    }

    /**
     * Read the next sequence of bytes from the current position
     *
     * @param dest the destination byte array
     * @param startOffset the start position of the dest array to which data is to be read
     * @param len the number of bytes to be read
     * @return the number of bytes read
     */
    @Override
    public int read(byte dest[], int startOffset, int len)
    {
        final int nBytes = pba.read(dest, startOffset, len, offset);
        offset += nBytes;
        return nBytes;
    }

    @Override
    public synchronized void reset() throws IOException
    {
        offset = 0;
    }

    @Override
    public long skip(long n) throws IOException
    {
        if (n < 0)
        {
            return 0;
        }

        final int size = pba.size();
        if( offset + n >= size)
        {
            offset = size;
            return size - offset;
        }
        else
        {
            offset += n;
            return n;
        }
    }

    @Override
    public int available() throws IOException
    {
        return pba.size() - offset;
    }
}
