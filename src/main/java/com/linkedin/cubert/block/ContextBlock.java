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

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.ClassCache;

/**
 * A block which is backed by input data from Mapper or Reducer.
 *
 * @author Maneesh Varshney
 *
 */
public class ContextBlock implements Block
{
    private final CommonContext context;
    private TupleCreator tupleCreator;
    private BlockProperties props;

    private final long blockId;
    private final Tuple partitionKey;
    private final long numRecords;

    public ContextBlock(CommonContext context)
    {
        this(context, null, -1, -1);
    }

    public ContextBlock(CommonContext context, long blockId)
    {
        this(context, null, blockId, -1);
    }

    public ContextBlock(CommonContext context,
                        Tuple partitionKey,
                        long blockId,
                        long numRecords)
    {
        this.context = context;
        this.partitionKey = partitionKey;
        this.blockId = blockId;
        this.numRecords = numRecords;
    }

    @Override
    public void configure(JsonNode json) throws JsonParseException,
            JsonMappingException,
            IOException
    {
        try
        {
            if (json.has("tupleCreator"))
            {
                tupleCreator =
                        (TupleCreator) ClassCache.forName(json.get("tupleCreator")
                                                         .getTextValue()).newInstance();
            }
            else
            {
                tupleCreator =
                        StorageFactory.get(JsonUtils.getText(json, "type"))
                                      .getTupleCreator();
            }

        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        tupleCreator.setup(json);

        BlockSchema schema = new BlockSchema(json.get("schema"));

        props = new BlockProperties(null, schema, (BlockProperties) null);
        if (numRecords != -1)
            props.setNumRecords(numRecords);
        props.setBlockId(blockId);
        props.setPartitionKey(partitionKey);

    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (!context.nextKeyValue())
            return null;

        return tupleCreator.create(context.getCurrentKey(), context.getCurrentValue());
    }

    @Override
    public void rewind() throws IOException
    {
        throw new IOException("rewind is not supported for MapContextIterator");
    }

    @Override
    public BlockProperties getProperties()
    {
        return props;
    }
}
