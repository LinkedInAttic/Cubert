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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.linkedin.cubert.utils.print;

/**
 * 
 * @author Maneesh Varshney
 * 
 */
public class VariableLengthEncoder
{
    private static final byte CODE_POS_ZERO = 0x1;
    private static final byte CODE_NEG_ZERO = 0x2;
    private static final byte CODE_NULL = 0x4;

    // 0x0 = 0
    // 0x80 = 10000000
    // 0xC0 = 11000000
    // 0xE0 = 11100000
    // 0xF0 = 11110000
    // 0xF8 = 11111000
    // 0xFC = 11111100
    // 0xFE = 11111110
    // 0xFF = 11111111
    private static final int[] intLeftBitsMasks = new int[] { 0x0, 0x80, 0xC0, 0xE0,
            0xF0, 0xF8, 0xFC, 0xFE, 0xFF };

    private static final byte[] buffer = new byte[10];

    private static final IntWritable intWritable = new IntWritable();
    private static final LongWritable longWritable = new LongWritable();
    private static final FloatWritable floatWritable = new FloatWritable();
    private static final DoubleWritable doubleWritable = new DoubleWritable();

    /*** Public methods ***/
    public static final void encodeNullInteger(OutputStream out) throws IOException
    {
        // null is encoded as -0 (sign bit = 1, value = 0).
        // The sign bit for integer is the first bit
        out.write(0x80);
    }

    public static final void encodeInteger(int num, OutputStream out) throws IOException
    {
        encodeInteger(num, out, 1);
    }

    public static final IntWritable decodeInteger(InputStream in) throws IOException
    {
        int firstByte = in.read();

        // special case of null: null is encoded as -0 (0x80)
        if (firstByte == 0x80)
            return null;

        boolean isNegative = ((firstByte & 0x80) != 0);

        int num = extractInteger(firstByte, in, 1);
        intWritable.set(isNegative ? -num : num);
        return intWritable;
    }

    public static final void encodeNullLong(OutputStream out) throws IOException
    {
        // null is encoded as -0 (sign bit = 1, value = 0).
        // The sign bit for integer is the first bit
        out.write(0x80);
    }

    public static final void encodeLong(long num, OutputStream out) throws IOException
    {
        encodeLong(num, out, 1);
    }

    public static final LongWritable decodeLong(InputStream in) throws IOException
    {
        int firstByte = in.read();

        // special case of null: null is encoded as -0 (0x80)
        if (firstByte == 0x80)
            return null;

        boolean isNegative = ((firstByte & 0x80) != 0);
        long num = extractLong(firstByte, in, 1);

        longWritable.set(isNegative ? -num : num);
        return longWritable;
    }

    public static final void encodeNullFloat(OutputStream out) throws IOException
    {
        out.write(CODE_NULL);
    }

    public static final void encodeFloat(float num, OutputStream out) throws IOException
    {
        // IEEE specifies floats can be 0.0 or -0.0. Handle this first
        if (num == 0.0)
        {
            if (Float.floatToIntBits(num) == 0) // this is +0.0
                out.write(CODE_POS_ZERO);
            else
                // this is -0.0
                out.write(CODE_NEG_ZERO);
            return;
        }

        if ((num - (int) num) != 0)
        {
            int i = Float.floatToIntBits(num);
            fillIntBuffer(i);
            out.write(buffer, 0, 5);
        }
        else
        {
            encodeInteger((int) num, out, 2);
        }
    }

    public static final FloatWritable decodeFloat(InputStream in) throws IOException
    {
        int firstByte = in.read();

        switch (firstByte)
        {
        case CODE_POS_ZERO:
            floatWritable.set(0.0f);
            break;
        case CODE_NEG_ZERO:
            floatWritable.set(-0.0f);
            break;
        case CODE_NULL:
            return null;
        case 0:
            floatWritable.set(Float.intBitsToFloat(readInt(0, in, 4)));
            break;
        default:
            boolean isNegative = ((firstByte & 0x40) != 0);
            int out = extractInteger(firstByte, in, 2);
            out = isNegative ? -out : out;
            floatWritable.set((float) out);
        }

        // if (firstByte == 0)
        // {
        // int i = readInt(0, in, 4);
        // out = Float.intBitsToFloat(i);
        // }
        // else
        // {
        // boolean isNegative = ((firstByte & 0x40) != 0);
        // out = (float) extractInteger(firstByte, in, 2);
        //
        // out = isNegative ? -out : out;
        // }
        // floatWritable.set(out);
        return floatWritable;
    }

    public static final void encodeNullDouble(OutputStream out) throws IOException
    {
        out.write(CODE_NULL);
    }

    public static final void encodeDouble(double num, OutputStream out) throws IOException
    {
        // IEEE specifies doubles can be 0.0 or -0.0. Handle this first
        if (num == 0.0)
        {
            if (Double.doubleToLongBits(num) == 0) // this is +0.0
                out.write(CODE_POS_ZERO);
            else
                // this is -0.0
                out.write(CODE_NEG_ZERO);
            return;
        }

        if ((num - (long) num) != 0)
        {
            long l = Double.doubleToLongBits(num);
            fillLongBuffer(l);
            out.write(buffer, 1, 9);
        }
        else
        {
            encodeLong((long) num, out, 2);
        }
    }

    public static final DoubleWritable decodeDouble(InputStream in) throws IOException
    {
        int firstByte = in.read();

        switch (firstByte)
        {
        case CODE_POS_ZERO:
            doubleWritable.set(0.0d);
            break;
        case CODE_NEG_ZERO:
            doubleWritable.set(-0.0d);
            break;
        case CODE_NULL:
            return null;
        case 0:
            doubleWritable.set(Double.longBitsToDouble(readLong(0, in, 8)));
            break;
        default:
            boolean isNegative = ((firstByte & 0x40) != 0);
            long num = extractLong(firstByte, in, 2);

            num = isNegative ? -num : num;
            doubleWritable.set((double) num);
        }

        // double out;
        // if (firstByte == 0)
        // {
        // long l = readLong(0, in, 8);
        // out = Double.longBitsToDouble(l);
        // }
        // else
        // {
        // boolean isNegative = ((firstByte & 0x40) != 0);
        // long num = extractLong(firstByte, in, 2);
        //
        // out = isNegative ? -num : num;
        // }
        // doubleWritable.set(out);
        return doubleWritable;
    }

    /*** Private helper methods ***/
    private static final void fillIntBuffer(int num)
    {
        buffer[0] = 0;
        buffer[1] = (byte) (num >>> 24);
        buffer[2] = (byte) (num >>> 16);
        buffer[3] = (byte) (num >>> 8);
        buffer[4] = (byte) (num >>> 0);
    }

    private static final void fillLongBuffer(long num)
    {
        buffer[0] = 0;
        buffer[1] = 0;
        buffer[2] = (byte) (num >>> 56);
        buffer[3] = (byte) (num >>> 48);
        buffer[4] = (byte) (num >>> 40);
        buffer[5] = (byte) (num >>> 32);
        buffer[6] = (byte) (num >>> 24);
        buffer[7] = (byte) (num >>> 16);
        buffer[8] = (byte) (num >>> 8);
        buffer[9] = (byte) (num >>> 0);
    }

    private static final int readInt(int num, InputStream in, int length) throws IOException
    {
        switch (length)
        {
        case 1:
            return (num << 8) | in.read();
        case 2:
            return (num << 16) | (in.read() << 8) | in.read();
        case 3:
            return (num << 24) | (in.read() << 16) | (in.read() << 8) | in.read();
        case 4:
            return (in.read() << 24) | (in.read() << 16) | (in.read() << 8) | in.read();
        }

        return num;
    }

    private static final long readLong(long num, InputStream in, int length) throws IOException
    {
        switch (length)
        {
        case 1:
            return (num << 8) | (((long) in.read()) << 0);
        case 2:
            return (num << 16) | (((long) in.read()) << 8) | (((long) in.read()) << 0);
        case 3:
            return (num << 24) | (((long) in.read()) << 16) | (((long) in.read()) << 8)
                    | (((long) in.read()) << 0);
        case 4:
            return (num << 32) | (((long) in.read()) << 24) | (((long) in.read()) << 16)
                    | (((long) in.read()) << 8) | (((long) in.read()) << 0);
        case 5:
            return (num << 40) | (((long) in.read()) << 32) | (((long) in.read()) << 24)
                    | (((long) in.read()) << 16) | (((long) in.read()) << 8)
                    | (((long) in.read()) << 0);
        case 6:
            return (num << 48) | (((long) in.read()) << 40) | (((long) in.read()) << 32)
                    | (((long) in.read()) << 24) | (((long) in.read()) << 16)
                    | (((long) in.read()) << 8) | (((long) in.read()) << 0);
        case 7:
            return (num << 56) | (((long) in.read()) << 48) | (((long) in.read()) << 40)
                    | (((long) in.read()) << 32) | (((long) in.read()) << 24)
                    | (((long) in.read()) << 16) | (((long) in.read()) << 8)
                    | (((long) in.read()) << 0);
        case 8:
            return (((long) in.read()) << 56) | (((long) in.read()) << 48)
                    | (((long) in.read()) << 40) | (((long) in.read()) << 32)
                    | (((long) in.read()) << 24) | (((long) in.read()) << 16)
                    | (((long) in.read()) << 8) | (((long) in.read()) << 0);

        }
        return num;
    }

    private static final int encodePositiveInteger(int num, int numHeaderBits)
    {
        fillIntBuffer(num);

        int offset = 4;
        for (int i = 1; i <= 4; i++)
        {
            if (buffer[i] != 0)
            {
                offset =
                        ((buffer[i] & intLeftBitsMasks[5 - i + numHeaderBits]) != 0)
                                ? i - 1 : i;
                buffer[offset] |= (intLeftBitsMasks[4 - offset] >>> numHeaderBits);
                break;
            }
        }

        return offset;
    }

    private static final int encodePositiveLong(long num, int numHeaderBits)
    {
        fillLongBuffer(num);

        // first handle cases that definitely require length encoding of two bytes
        if (buffer[2] != 0)
        {
            buffer[0] = (byte) (0xFF >>> numHeaderBits);
            buffer[1] = (byte) intLeftBitsMasks[1 + numHeaderBits];
            return 0;
        }
        if (buffer[3] != 0)
        {
            buffer[1] = (byte) (0xFF >>> numHeaderBits);
            buffer[2] = (byte) intLeftBitsMasks[numHeaderBits];
            return 1;
        }

        if (buffer[4] != 0 && numHeaderBits == 2)
        {
            buffer[2] = (byte) (0xFF >>> numHeaderBits);
            buffer[3] = (byte) intLeftBitsMasks[1];
            return 2;
        }

        int offset = 9;
        for (int i = 4; i <= 9; i++)
        {
            if (buffer[i] != 0)
            {
                offset =
                        ((buffer[i] & intLeftBitsMasks[10 - i + numHeaderBits]) != 0)
                                ? i - 1 : i;
                buffer[offset] |= (intLeftBitsMasks[9 - offset] >>> numHeaderBits);
                break;
            }
        }

        return offset;

    }

    private static final int getLength(int firstByte, int numHeaderBits, int maxSetBits)
    {
        int length;
        for (length = maxSetBits; length >= 0; length--)
        {
            if ((firstByte & intLeftBitsMasks[length + numHeaderBits]) == intLeftBitsMasks[length
                    + numHeaderBits])
                break;
        }

        return length;
    }

    private final static int extractInteger(int firstByte,
                                            InputStream in,
                                            int numHeaderBits) throws IOException
    {
        int num = firstByte;
        firstByte |= intLeftBitsMasks[numHeaderBits];
        int length = getLength(firstByte, numHeaderBits, 4);
        num &= ~intLeftBitsMasks[length + numHeaderBits];
        return readInt(num, in, length);
    }

    private static final long extractLong(int firstByte, InputStream in, int numHeaderBits) throws IOException
    {
        long num = firstByte;

        firstByte |= intLeftBitsMasks[numHeaderBits];
        int length;

        if (firstByte == 0xFF)
        {
            // need to look into the next byte to read the length
            int secondByte = in.read();
            length = getLength(secondByte, 0, 3);
            num = ((long) secondByte) & ~intLeftBitsMasks[length];
            length += 7 - numHeaderBits;
        }
        else
        {
            length = getLength(firstByte, numHeaderBits, numHeaderBits == 2 ? 6 : 7);
            num &= ~intLeftBitsMasks[length + numHeaderBits];
        }

        return readLong(num, in, length);
    }

    private static final void encodeInteger(int num, OutputStream out, int numHeaderBits) throws IOException
    {
        int headerBit = numHeaderBits == 2 ? 0x80 : 0;
        int signBit = 0;
        if (num < 0)
        {
            num = -num;
            signBit = (numHeaderBits == 2) ? 0x40 : 0x80;
        }

        int offset = encodePositiveInteger(num, numHeaderBits);
        buffer[offset] = (byte) (headerBit | signBit | buffer[offset]);
        out.write(buffer, offset, 5 - offset);
    }

    private static final void encodeLong(long num, OutputStream out, int numHeaderBits) throws IOException
    {
        int headerBit = numHeaderBits == 2 ? 0x80 : 0;

        int signBit = 0;
        if (num < 0)
        {
            num = -num;
            signBit = (numHeaderBits == 2) ? 0x40 : 0x80;
        }

        int offset = encodePositiveLong(num, numHeaderBits);
        buffer[offset] = (byte) (headerBit | signBit | buffer[offset]);

        out.write(buffer, offset, 10 - offset);
    }

    static final void test(Number num) throws IOException
    {
        final String[] prefix =
                new String[] { "00000000", "0000000", "000000", "00000", "0000", "000",
                        "00", "0" };

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if (num instanceof Integer)
            encodeInteger(num.intValue(), bos);
        else if (num instanceof Long)
            encodeLong(num.longValue(), bos);
        else if (num instanceof Float)
            encodeFloat(num.floatValue(), bos);
        else if (num instanceof Double)
            encodeDouble(num.doubleValue(), bos);

        byte[] bytes = bos.toByteArray();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++)
        {
            String str = Integer.toBinaryString(bytes[i]);
            if (str.length() > 8)
                str = str.substring(str.length() - 8);
            if (str.length() < 8)
                str = prefix[str.length()] + str;
            sb.append(str);
            sb.append(" ");
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        Number dec = null;
        if (num instanceof Integer)
            dec = decodeInteger(bis).get();
        else if (num instanceof Long)
            dec = decodeLong(bis).get();
        else if (num instanceof Float)
            dec = decodeFloat(bis).get();
        else if (num instanceof Double)
            dec = decodeDouble(bis).get();

        print.f("%-5s %s %-16s %-16s",
                Boolean.toString(num.equals(dec)),
                sb.toString(),
                num.toString(),
                dec.toString());
    }

    private static void test(Number[] array) throws IOException
    {

    }

    public static void main(String[] args) throws IOException
    {

        test((float) 2147483647);
        test((float) -2147483647);
        ArrayList<Number> list = new ArrayList<Number>();

        for (int i = 0; i <= 32; i++)
        {
            int num = 0;
            for (int j = 0; j < i; j++)
                num |= 1 << j;

            list.add(num);
            list.add(-num);
            list.add((float) num);
            list.add(-(float) num);
        }

        for (int i = 0; i <= 64; i++)
        {
            long num = 0;
            for (int j = 0; j < i; j++)
                num |= 1L << j;

            list.add(-num);
            list.add(num);
            list.add((double) num);
            list.add(-(double) num);
        }

        test(list.toArray(new Number[] {}));

        Number[] specials =
                new Number[] { Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE,
                        Long.MIN_VALUE, Double.NaN, Double.POSITIVE_INFINITY,
                        Double.NEGATIVE_INFINITY, Double.MAX_VALUE, Double.MIN_VALUE,
                        Float.MIN_VALUE, Float.MAX_VALUE, Float.POSITIVE_INFINITY,
                        Float.NEGATIVE_INFINITY };
        test(specials);

    }
}
