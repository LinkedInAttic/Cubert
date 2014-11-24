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
import java.io.OutputStream;

/**
 * PagedByteArrayOutputStream
 *      Output Stream that writes into a PagedByteArray
 *      Warning: This Object is not thread safe
 *
 * Created by spyne on 7/29/14.
 */
public class PagedByteArrayOutputStream extends OutputStream
{
    /**
     * The data is stored into a PagedByteArray
     */
    private final PagedByteArray pagedByteArray;
    /**
     * Maintains the current write position of the stream
     */
    private int offset = 0;

    /**
     * Create a PagedByteArrayOutputStream for writing
     *
     * @param chunkSize the size of a chunk
     */
    public PagedByteArrayOutputStream(int chunkSize)
    {
        pagedByteArray = new PagedByteArray(chunkSize);
    }

    /**
     * Write a byte into the current location
     *
     * @param b byte to be written
     * @throws IOException
     */
    @Override
    public void write(int b) throws IOException
    {
        pagedByteArray.write((byte) b, offset++);
    }

    /**
     * Write a sequence of bytes
     * @param src The source byte array
     * @param startOffset The position from which data is read from src array
     * @param len the number of bytes to be written
     * @throws IOException
     */
    @Override
    public void write(byte[] src, int startOffset, int len) throws IOException
    {
        pagedByteArray.write(src, startOffset, len, offset);
        offset += len;
    }

    /**
     *
     * @return the data store object
     */
    public PagedByteArray getPagedByteArray()
    {
        return pagedByteArray;
    }

    /**
     * Returns the size of the output stream
     *
     * @return the size of the output stream
     */
    public int size()
    {
        return offset;
    }

    /**
     * Resets the output stream. Clears all previous data
     */
    public void reset()
    {
        offset = 0;
        pagedByteArray.clear();
    }
}
