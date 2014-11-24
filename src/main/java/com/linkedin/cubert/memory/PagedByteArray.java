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

import java.util.ArrayList;
import java.util.List;

/**
 * PagedByteArray
 *      is a paged extension of a byte array which substiantially reduces data copy and memory reallocation.
 *
 * Created by spyne on 7/29/14.
 */
public class PagedByteArray
{
    /**
     * Data is store in chunks of byte arrays stored in an ArrayList.
     */
    final List<byte[]> chunks = new ArrayList<byte[]>();

    /**
     * The size of each chunk. This is fixed once the object is initialized.
     */
    final int chunkSize;

    /**
     * This maintains the max index that was written
     * which is used to determine the size of the data = maxIndex + 1
     */
    int maxIndex = -1;

    /**
     * Create a PagedByteArray object.
     *
     * @param chunkSize Size of each chunk in bytes
     */
    public PagedByteArray(int chunkSize)
    {
        if (chunkSize < 0)
        {
            throw new IllegalArgumentException("Negative chunk size: " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    /**
     * Read a byte at a specific index
     *
     * @param index index of the element in the array
     * @return value of the byte
     */
    public byte read(final int index)
    {
        final int chunkIdx = getChunkIdx(index);
        final int pos = index % chunkSize;
        return chunks.get(chunkIdx)[pos];
    }

    /**
     * Read a sequence of bytes.
     *
     * @param dest The destination byte array where the data is to be copied into
     * @param destOffset The start position in the destination array where the data is copied to
     * @param length The number of bytes to be copied
     * @param index The start position in the PagedByteArray where the read starts
     * @return the number of bytes copied
     */
    public int read(final byte[] dest, final int destOffset, final int length, final int index)
    {
        final int lastIndex = index + length - 1;
        final int lastChunkIdx = getChunkIdx(lastIndex);

        int chunkIdx = getChunkIdx(index);
        int pos = index % chunkSize;

        if (chunkIdx == lastChunkIdx)
        {
            System.arraycopy(chunks.get(chunkIdx), pos, dest, destOffset, length);
        }
        else
        {
            /* Read the current chunk completely */
            int read = chunkSize - pos;
            System.arraycopy(chunks.get(chunkIdx++), pos, dest, destOffset, read);

            /* Read intermediate complete chunks */
            int rem = length - read;
            while (rem > chunkSize)
            {
                System.arraycopy(chunks.get(chunkIdx++), 0, dest, destOffset + read, chunkSize);
                read += chunkSize;
                rem -= chunkSize;
            }

            /* Read the last portion left */
            System.arraycopy(chunks.get(chunkIdx), 0, dest, destOffset + read, rem);
        }

        return length;
    }

    /**
     * API to write a byte at position
     *
     * @param b the value of the byte
     * @param index position 0..(size - 1)
     */
    public void write(final byte b, final int index)
    {
        final int chunkIdx = getChunkIdx(index);
        final int pos = index % chunkSize;
        ensureCapacity(chunkIdx);

        chunks.get(chunkIdx)[pos] = b;
        if (maxIndex < index)
            maxIndex = index;
    }


    /**
     * API to write a chunk of bytes
     *
     * @param bArray the source byte array
     * @param srcOffset the offset from which data is to be copied from
     * @param length number of bytes to write
     * @param index position 0..(size - 1)
     */
    public void write(final byte bArray[], final int srcOffset, final int length, final int index)
    {
        final int lastIndex = index + length - 1;
        final int lastChunkIdx = getChunkIdx(lastIndex);
        ensureCapacity(lastChunkIdx);

        int chunkIdx = getChunkIdx(index);
        int pos = index % chunkSize;

        if (chunkIdx == lastChunkIdx)
        {
            System.arraycopy(bArray, srcOffset, chunks.get(chunkIdx), pos, length);
        }
        else
        {
            /* Fill up the current chunk */
            int written = chunkSize - pos;
            System.arraycopy(bArray, srcOffset, chunks.get(chunkIdx++), pos, written);

            /* If data spills over multiple chunks. */
            int rem = length - written;
            while (rem > chunkSize)
            {
                System.arraycopy(bArray, srcOffset + written, chunks.get(chunkIdx++), 0, chunkSize);
                rem -= chunkSize;
                written += chunkSize;
            }

            /* Write the last portion */
            System.arraycopy(bArray, srcOffset + written, chunks.get(chunkIdx), 0, rem);
        }
        if (maxIndex < lastIndex)
            maxIndex = lastIndex;
    }

    /**
     * Get the Chunk Index for a given offset
     *
     * @param offset index of the element in the array
     * @return the Chunk Index
     */
    int getChunkIdx(int offset)
    {
        return offset / chunkSize;
    }

    /**
     * Ensures that the PagedByteArray can store at least chunkIdx + 1 chunks
     * @param chunkIdx the chunk Index
     */
    private void ensureCapacity(int chunkIdx)
    {
        /* allocate till data.size() == chunkIdx + 1 */
        while (chunks.size() <= chunkIdx)
        {
            chunks.add(new byte[chunkSize]);
        }
    }

    /**
     * Since the size of a pagedByteArray is determined by the bytes written as opposed to the total
     * memory allocated. The size is computed by observing the max index that was written to.
     *
     * @return the size of t
     */
    public int size()
    {
        return maxIndex + 1;
    }

    /**
     * Interface to reuse the data structure.
     */
    public void clear()
    {
        chunks.clear();
        maxIndex = -1;
    }
}
