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

import com.linkedin.cubert.utils.MemoryStats;
import org.junit.Test;
import org.testng.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 *
 * Created by spyne on 7/29/14.
 */
public class TestPagedByteArray
{
    final long seed = 12345L;
    @Test
    public void testReadWrite() throws Exception
    {
        PagedByteArray pba = new PagedByteArray(100);

        final byte[] b ={ 56, 67, 78, 89 };
        final int someOffset = 72;
        pba.write(b[0], someOffset);
        pba.write(b[1], someOffset + 1);
        pba.write(b[2], someOffset + 2);
        pba.write(b[3], someOffset);

        Assert.assertEquals(b[3], pba.read(someOffset));
        Assert.assertEquals(b[1], pba.read(someOffset + 1));
        Assert.assertEquals(b[2], pba.read(someOffset + 2));
    }

    @Test
    public void testReadWriteArray() throws Exception
    {
        final int n = 100;
        byte[] bytes = new byte[n];
        final Random rand = new Random(seed);
        rand.nextBytes(bytes);

        PagedByteArray pba = new PagedByteArray(100);

        final int someOffset = 172;
        pba.write(bytes, 0, bytes.length, someOffset);

        byte[] result = new byte[n];
        pba.read(result, 0, bytes.length, someOffset);
        Assert.assertEquals(result, bytes);
    }

    @Test
    public void testInputOutputStream() throws Exception
    {
        /* running with chunk size less than array size */
        streamTest(1000, 243, 100);

        /* running with chunk size greater than array size */
        streamTest(1200, 223, 300);

    }

    private void streamTest(int nEntries, int arraySize, int chunkSize) throws IOException
    {
        PagedByteArrayOutputStream pbaos = new PagedByteArrayOutputStream(chunkSize);
        PagedByteArrayInputStream pbais = new PagedByteArrayInputStream(pbaos.getPagedByteArray());

        byte[][] bytesMat = new byte[nEntries][arraySize];
        byte[]   bytes    = new byte[nEntries];

        final Random rand = new Random(seed);
        for (int i = 0; i < nEntries; ++i)
        {
            rand.nextBytes(bytesMat[i]);
        }
        rand.nextBytes(bytes);

        /* Write all values. */
        for (int i = 0; i < nEntries; ++i)
        {
            pbaos.write(bytesMat[i]);
            pbaos.write(bytes[i]);
        }

        /* read and match */
        byte[] readBytes = new byte[arraySize];
        for (int i = 0; i < nEntries; ++i)
        {
            int l = pbais.read(readBytes);
            byte b = (byte) pbais.read();

            Assert.assertEquals(l, bytesMat[i].length);
            Assert.assertEquals(readBytes, bytesMat[i]);
            Assert.assertEquals(b, bytes[i]);
        }
    }

    /**
     * Benchmark between PagedByteArrayOutputStream and ByteArrayOutputStream
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException
    {
//        PagedByteArrayOutputStream os = new PagedByteArrayOutputStream(1 << 20);
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        final long start = System.nanoTime();
        for (long i = 0; i < (1L << 29); i++)
        {
            os.write((int) i % 128);
            if (i % (1<<24) == 0)
            {
                System.out.println((i / (1 << 20)) + " MB Written");
                MemoryStats.print("");
//                System.gc();
            }

        }
        System.out.println("TestPagedByteArray.main: " + ((System.nanoTime() - start)/ 1E6));
    }
}
