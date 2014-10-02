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

package com.linkedin.cubert.block;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.io.BlockInputStream;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.io.rubix.RubixMemoryBlock;
import com.linkedin.cubert.operator.PivotBlockOperator;
import com.linkedin.cubert.utils.Pair;
import com.linkedin.cubert.utils.print;

public class BlockUtils
{
    private static final int BLOCK_BUFFER_SIZE = 20 * 1024;
    private static final boolean emptyForMissing = true;
    private static final Map<IndexEntry, ByteBuffer> inMemoryBlockCache =
            new HashMap<IndexEntry, ByteBuffer>();
    private static final Map<String, Map<Object, Pair<Integer, Integer>>> columnIndexCache =
            new HashMap<String, Map<Object, Pair<Integer, Integer>>>();

    @SuppressWarnings("unchecked")
    public static Block loadBlock(BlockProperties props,
                                  IndexEntry indexEntry,
                                  Configuration conf,
                                  JsonNode json,
                                  BlockSerializationType serializationType,
                                  boolean isInMemoryBlock) throws IOException,
            ClassNotFoundException,
            InstantiationException,
            IllegalAccessException,
            InterruptedException
    {
        Block block;
        if (indexEntry == null)
        {
            if (emptyForMissing)
                return new EmptyBlock(props);

            throw new IOException(String.format("Index entry is null"));
        }

        // populate props
        props.setBlockId(indexEntry.getBlockId());
        props.setNumRecords(indexEntry.getNumRecords());

        // Open the file and seek to the offset for this block
        Path file = new Path(indexEntry.getFile());
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fsin = fs.open(file, BLOCK_BUFFER_SIZE);
        fsin.seek(indexEntry.getOffset());

        // Gather information needed to read this block
        Class<Tuple> valueClass =
                (Class<Tuple>) TupleFactory.getInstance().newTuple().getClass();
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);

        // Load the block now
        if (isInMemoryBlock)
        {
            print.f("LOADING IN MEMORY the block %d", indexEntry.getBlockId());

            ByteBuffer byteBuffer = inMemoryBlockCache.get(indexEntry);

            if (byteBuffer == null)
            {
                int read = 0;
                byte[] data = new byte[(int) indexEntry.getLength()];
                while (read != data.length)
                {
                    read += fsin.read(data, read, data.length - read);
                }
                fsin.close();

                byteBuffer = ByteBuffer.wrap(data);

                inMemoryBlockCache.put(indexEntry, byteBuffer);
            }
            else
            {
                print.f("REUSED FROM CACHE!!");
                byteBuffer.rewind();
            }

            block =
                    new RubixMemoryBlock(props,
                                         conf,
                                         byteBuffer,
                                         valueClass,
                                         codec,
                                         serializationType);
            block.configure(json);
            return block;
        }
        else
        {
            print.f("STREAMING the block %d", indexEntry.getBlockId());
            InputStream in = new BlockInputStream(fsin, indexEntry.getLength());

            if (codec != null)
            {
                in = codec.createInputStream(in);
            }

            block =
                    new CubertBlock(props, new BlockIterator<Tuple>(conf,
                                                                    in,
                                                                    valueClass,
                                                                    serializationType,
                                                                    props.getSchema()));
            block.configure(json);

            print.f("Loaded block id=%d from file=%s offset=%d length=%d",
                    indexEntry.getBlockId(),
                    file.toString(),
                    indexEntry.getOffset(),
                    indexEntry.getLength());

            return block;
        }
    }

    public static int getBlockId(Tuple partitionKey)
    {
        long h = partitionKey.hashCode();
        return (int) (h < 0 ? -h : h);
    }

    public static Map<Object, Pair<Integer, Integer>> generateColumnIndex(RubixMemoryBlock inputBlock,
                                                                          String lookupColumn) throws IOException,
            InterruptedException,
            ExecException
    {
        String indexKey = inputBlock.getProperties().getBlockId() + ":" + lookupColumn;
        if (columnIndexCache.get(indexKey) != null)
        {
            System.out.println("CACHED COLUMN INDEX for " + indexKey);
            return columnIndexCache.get(indexKey);
        }

        int lookupColumnIndex =
                inputBlock.getProperties().getSchema().getIndex(lookupColumn);
        PivotBlockOperator pivotedBlock = new PivotBlockOperator();
        String[] lookupColumns = new String[1];
        lookupColumns[0] = lookupColumn;

        pivotedBlock.setInput(inputBlock, lookupColumns, false);
        // inputBlock.markRewindPosition();

        Map<Object, Pair<Integer, Integer>> coord2offsets =
                new HashMap<Object, Pair<Integer, Integer>>();
        while (true)
        {
            Block thisBlock = pivotedBlock.next();
            if (thisBlock == null)
                break;

            Tuple tuple = thisBlock.next();
            if (null == tuple)
                continue; // Would this condition happen?

            Object coordinate = tuple.get(lookupColumnIndex);

            int start = inputBlock.getCurrentTuplePosition();

            // Run though the block (in-memory)
            while (null != (tuple = thisBlock.next()))
                ;

            int end = inputBlock.getCurrentTuplePosition();

            if (end != start)
                coord2offsets.put(coordinate, new Pair<Integer, Integer>(start, end));
        }
        columnIndexCache.put(indexKey, coord2offsets);
        return coord2offsets;
        //
        // inputBlock.rewind();
    }

}
