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

package com.linkedin.cubert.io.rubix;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.io.CompactSerializer;
import com.linkedin.cubert.plan.physical.CubertStrings;
import com.linkedin.cubert.utils.print;

/**
 * Writes data in rubix binary file format.
 * 
 * The rubix file format stores all values together in the beginning of the file, followed
 * by all keys together at the end of the file. This RecordWriter caches all keys in
 * memory, and dumps them to file when all record values are written. It is, therefore,
 * expected that the cumulative size of keys does not exceed the available memory of the
 * process.
 * <p>
 * The data is organized in the following manner:
 * <p>
 * {@literal File :=  [Value Section] [Key Section] <int = Key Section size>}
 * <p>
 * {@literal Value Section := [Value] [Value] [Value] ...}
 * <p>
 * {@literal Key Section := <string = key class> <string = value class> <BlockSchema of the file>
 * <parition keys> <sort keys> [Key <long=offset of first value in value section> <number
 * of records for this key>]}
 * <p>
 * Each non-null key is considered as an InputSplit. As an example, say, the client
 * emitted the following key-values:
 * <ul>
 * <li>key1, value1</li>
 * 
 * <li>null, value2</li>
 * 
 * <li>null, value3</li>
 * 
 * <li>key2, value4</li>
 * </ul>
 * <p>
 * The file will be stored as:
 * <p>
 * value1 value2 value3 value4 key1 offset_value1 blockid1 output_record_count1 key2
 * offset_value4 blockid2 output_record_count2
 * <p>
 * There are two input splits, one for each non-null key. The the first split, the mapper
 * will receive the following pairs of key-values: (key1, value1), (null, value2), (null,
 * value3). In the second split, the mapper will receive (key2, value4).
 * 
 * @author Maneesh Varshney
 * 
 * @author spyne
 * 
 */
public class RubixRecordWriter<K, V> extends RecordWriter<K, V>
{
    /* Main output stream (connected to output file) */
    private final FSDataOutputStream out;

    /* Serializer for value objects */
    private final Serializer<V> valueSerializer;
    private final Serializer<K> keySerializer;

    /* Used to compress values. Dumps to out stream */
    private final CompressionOutputStream compressedStream;

    private final ByteArrayOutputStream keySectionStream = new ByteArrayOutputStream();
    private final DataOutput keySectionOut = new DataOutputStream(keySectionStream);

    private JsonNode metadataJson;

    private int blockCount = 0;
    private long outputRecordCount = -1;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public RubixRecordWriter(Configuration conf,
                             FSDataOutputStream out,
                             Class keyClass,
                             Class valueClass,
                             CompressionCodec codec) throws IOException
    {
        this.out = out;

        final SerializationFactory serializationFactory = new SerializationFactory(conf);
        keySerializer = serializationFactory.getSerializer(keyClass);

        ObjectMapper mapper = new ObjectMapper();
        metadataJson =
                mapper.readValue(conf.get(CubertStrings.JSON_METADATA), JsonNode.class);
        ((ObjectNode) metadataJson).put("keyClass", keyClass.getCanonicalName());
        ((ObjectNode) metadataJson).put("valueClass", valueClass.getCanonicalName());
        BlockSchema schema = new BlockSchema(metadataJson.get("schema"));

        if (conf.getBoolean(CubertStrings.USE_COMPACT_SERIALIZATION, false)
                && schema.isFlatSchema())
        {
            valueSerializer = new CompactSerializer<V>(schema);
            ((ObjectNode) metadataJson).put("serializationType",
                                            BlockSerializationType.COMPACT.toString());
        }
        else
        {
            valueSerializer = serializationFactory.getSerializer(valueClass);
            ((ObjectNode) metadataJson).put("serializationType",
                                            BlockSerializationType.DEFAULT.toString());
        }

        keySerializer.open(keySectionStream);

        if (codec == null)
        {
            valueSerializer.open(out);
            compressedStream = null;
        }
        else
        {
            compressedStream = codec.createOutputStream(out);
            valueSerializer.open(compressedStream);
        }

    }

    @Override
    public void write(K key, V value) throws IOException,
            InterruptedException
    {
        if (key != null)
        {
            handleNewKey(key);
        }

        if (value != null)
        {
            outputRecordCount++;
            valueSerializer.serialize(value);
        }
    }

    private void handleNewKey(K key) throws IOException
    {
        /* Increment block count when encountering new key */
        blockCount++;

        if (compressedStream != null)
        {
            compressedStream.finish();
        }

        // write the count for the previous block now;
        if (outputRecordCount >= 0)
        {
            keySectionOut.writeLong(outputRecordCount);
        }
        outputRecordCount = 0;

        @SuppressWarnings("unchecked")
        K partitionKey = (K) ((Tuple) key).get(0);
        long blockId = (Long) ((Tuple) key).get(1);

        keySerializer.serialize(partitionKey);
        keySectionOut.writeLong(out.getPos());
        keySectionOut.writeLong(blockId);

        if (compressedStream != null)
        {
            compressedStream.resetState();
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException
    {
        if (compressedStream != null)
        {
            compressedStream.finish();
        }

        ((ObjectNode) metadataJson).put("numberOfBlocks", blockCount);

        // write out the number of records for the last block
        keySectionOut.writeLong(outputRecordCount);
        byte[] trailerBytes = keySectionStream.toByteArray();

        long trailerstartOffset = out.getPos();

        out.writeUTF(metadataJson.toString());
        out.writeInt(trailerBytes.length);
        out.write(trailerBytes);
        long trailerSize = out.getPos() - trailerstartOffset;
        out.writeLong(trailerstartOffset);
        print.f("Trailer size withe metadata + keys is %d, only keys is %d",
                trailerSize,
                trailerBytes.length);

        out.close();
    }
}
