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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.utils.ClassCache;

public class RubixInputSplit<K, V> extends InputSplit implements Writable, Configurable
{
    final static int MAX_LOCATIONS = 5;

    private K key;
    private Path filename;
    private long offset;
    private long length;
    private long blockId;
    private long numRecords;
    private Class<K> keyClass;
    private Class<V> valueClass;
    private Serializer<K> keySerializer;
    private Configuration conf;
    private BlockSchema schema;
    private BlockSerializationType blockSerializationType;

    private String[] hostnames = null;

    public RubixInputSplit()
    {

    }

    public RubixInputSplit(Configuration conf,
                           Path filename,
                           K key,
                           long offset,
                           long length,
                           long blockId,
                           long numRecords,
                           Class<K> keyClass,
                           Class<V> valueClass,
                           BlockSchema schema,
                           BlockSerializationType blockSerializationType)
    {
        this.conf = conf;
        this.key = key;
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.blockId = blockId;
        this.numRecords = numRecords;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.schema = schema;
        this.blockSerializationType = blockSerializationType;

        SerializationFactory serializationFactory = new SerializationFactory(conf);
        keySerializer = serializationFactory.getSerializer(keyClass);
    }

    @Override
    public long getLength() throws IOException,
            InterruptedException
    {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException,
            InterruptedException
    {
        if (hostnames == null)
        {
            /* Obtain the FileSystem object and get the FileStatus objects for the split */
            FileSystem fileSystem = FileSystem.get(conf);
            FileStatus fileStatus = fileSystem.getFileStatus(filename);
            /*
             * Obtain the Block locations for the split. This also provides the offset and
             * length information for each block
             */
            final BlockLocation[] blockLocations =
                    fileSystem.getFileBlockLocations(fileStatus, offset, length);
            /**
             * Collect all hosts in a map and populate the number of bytes to be read from
             * each host
             */
            Long l;
            Map<String, Long> hostMap = new HashMap<String, Long>();
            for (BlockLocation bl : blockLocations)
            {
                final long start = bl.getOffset() < offset ? offset : bl.getOffset();
                final long end =
                        (offset + length) < (bl.getOffset() + bl.getLength()) ? offset
                                + length : bl.getOffset() + bl.getLength();
                final long nRelevantBytes = end - start;
                for (String host : bl.getHosts())
                {
                    hostMap.put(host, ((l = hostMap.get(host)) == null ? 0 : l)
                            + nRelevantBytes);
                }
            }
            /* Sort them in decreasing order of maximum number of relevant bytes */
            final Set<Map.Entry<String, Long>> entries = hostMap.entrySet();
            final Map.Entry<String, Long>[] hostLengthPairs =
                    entries.toArray(new Map.Entry[entries.size()]);

            Arrays.sort(hostLengthPairs, new Comparator<Map.Entry<String, Long>>()
            {
                @Override
                public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2)
                {
                    return (int) (e2.getValue() - e1.getValue());
                }
            });

            /* Populate the hostnames object */
            final int nHost = Math.min(hostLengthPairs.length, MAX_LOCATIONS);
            hostnames = new String[nHost];
            for (int i = 0; i < nHost; ++i)
            {
                hostnames[i] = hostLengthPairs[i].getKey();
            }
        }
        return hostnames;
    }

    public K getKey()
    {
        return key;
    }

    public Path getFilename()
    {
        return filename;
    }

    public long getOffset()
    {
        return offset;
    }

    public Class<V> getValueClass()
    {
        return valueClass;
    }

    public BlockSchema getSchema()
    {
        return schema;
    }

    public long getBlockId()
    {
        return blockId;
    }

    public long getNumRecords()
    {
        return numRecords;
    }

    public BlockSerializationType getBlockSerializationType()
    {
        return blockSerializationType;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        keySerializer.open(bos);
        keySerializer.serialize(key);
        byte[] keyBytes = bos.toByteArray();

        out.writeInt(keyBytes.length);
        out.write(keyBytes);
        out.writeUTF(filename.toString());
        out.writeLong(offset);
        out.writeLong(length);
        out.writeLong(blockId);
        out.writeLong(numRecords);
        out.writeUTF(keyClass.getName());
        out.writeUTF(valueClass.getName());

        ObjectMapper mapper = new ObjectMapper();
        out.writeUTF(mapper.writeValueAsString(schema.toJson()));

        out.writeInt(blockSerializationType.ordinal());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException
    {
        int keyBytesLen = in.readInt();
        byte[] keyBytes = new byte[keyBytesLen];
        in.readFully(keyBytes, 0, keyBytesLen);

        filename = new Path(in.readUTF());
        offset = in.readLong();
        length = in.readLong();
        blockId = in.readLong();
        numRecords = in.readLong();
        try
        {
            keyClass = (Class<K>) ClassCache.forName(in.readUTF());
            valueClass = (Class<V>) ClassCache.forName(in.readUTF());

            SerializationFactory serializationFactory = new SerializationFactory(conf);
            Deserializer<K> keyDeserializer =
                    serializationFactory.getDeserializer(keyClass);

            ByteArrayInputStream bis = new ByteArrayInputStream(keyBytes);
            keyDeserializer.open(bis);

            key = keyDeserializer.deserialize(null);

            ObjectMapper mapper = new ObjectMapper();
            schema = new BlockSchema(mapper.readValue(in.readUTF(), JsonNode.class));
            blockSerializationType = BlockSerializationType.values()[in.readInt()];
        }
        catch (ClassNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.conf = conf;
    }

    @Override
    public Configuration getConf()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String toString()
    {
        return String.format("RubixInputSplit [key=%s, filename=%s, offset=%s, length=%s]",
                             key,
                             filename,
                             offset,
                             length);
    }

}
