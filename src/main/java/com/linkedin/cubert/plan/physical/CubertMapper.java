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

package com.linkedin.cubert.plan.physical;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.CommonContext;
import com.linkedin.cubert.block.ContextBlock;
import com.linkedin.cubert.io.MultiMapperSplit;
import com.linkedin.cubert.io.SerializerUtils;
import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.TeeOperator;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.print;

/**
 * Executes the physical plan of a job at the Mapper.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CubertMapper extends Mapper<Object, Object, Object, Object>
{

    @Override
    public void run(Context context) throws IOException,
            InterruptedException
    {
        print.f("Mapper init  ----------------------------------");
        Configuration conf = context.getConfiguration();

        FileCache.create(conf);
        PhaseContext.create(context, conf);

        ObjectMapper mapper = new ObjectMapper();
        // read the map configuration
        ArrayNode mapCommands =
                mapper.readValue(conf.get(CubertStrings.JSON_MAP_OPERATOR_LIST),
                                 ArrayNode.class);

        int multiMapperIndex = 0;

        if (context.getInputSplit() instanceof MultiMapperSplit)
        {
            // identify the input, output and operators for this mapper
            MultiMapperSplit mmSplit = (MultiMapperSplit) context.getInputSplit();
            multiMapperIndex = mmSplit.getMultiMapperIndex();
        }
        JsonNode inputJson = mapCommands.get(multiMapperIndex).get("input");
        ArrayNode operatorsJson =
                (ArrayNode) mapCommands.get(multiMapperIndex).get("operators");

        JsonNode outputJson = null;
        if (conf.get(CubertStrings.JSON_SHUFFLE) != null)
        {
            outputJson =
                    mapper.readValue(conf.get(CubertStrings.JSON_SHUFFLE), JsonNode.class);
        }
        else
        {
            outputJson =
                    mapper.readValue(conf.get(CubertStrings.JSON_OUTPUT), JsonNode.class);
        }

        long blockId = conf.getLong("MY_BLOCK_ID", -1);
        long numRecords = conf.getLong("MY_NUM_RECORDS", -1);

        Tuple partitionKey = null;
        if (conf.get("MY_PARTITION_KEY") != null)
        {
            try
            {
                byte[] bytes =
                        (byte[]) SerializerUtils.deserializeFromString(conf.get("MY_PARTITION_KEY"));
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                partitionKey = TupleFactory.getInstance().newTuple();
                partitionKey.readFields(new DataInputStream(bis));
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }

        // Create input block
        CommonContext commonContext = new MapContext(context);
        Block input = new ContextBlock(commonContext, partitionKey, blockId, numRecords);
        input.configure(inputJson);

        // Create phase executor
        PhaseExecutor exec =
                new PhaseExecutor(inputJson.get("name").getTextValue(),
                                  input,
                                  outputJson.get("name").getTextValue(),
                                  operatorsJson,
                                  conf);

        BlockWriter writer =
                StorageFactory.get(JsonUtils.getText(outputJson, "type"))
                              .getBlockWriter();
        writer.configure(outputJson);

        long start = System.currentTimeMillis();
        Block outputBlock;
        while ((outputBlock = exec.next()) != null)
        {
            writer.write(outputBlock, commonContext);
            print.f("Executed operator chain for a block in %d ms",
                    System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
        }

        // HACK!! Asking the TeeOperator to close the files that were opened
        TeeOperator.closeFiles();

        print.f("Mapper complete ----------------------------------");

    }

    public static final class MapContext implements CommonContext
    {
        private final Context context;

        public MapContext(Context context)
        {
            this.context = context;
        }

        @Override
        public boolean nextKeyValue() throws IOException,
                InterruptedException
        {
            return context.nextKeyValue();
        }

        @Override
        public Object getCurrentKey() throws IOException,
                InterruptedException
        {
            return context.getCurrentKey();
        }

        @Override
        public Object getCurrentValue() throws IOException,
                InterruptedException
        {
            return context.getCurrentValue();
        }

        @Override
        public void write(Object key, Object value) throws IOException,
                InterruptedException
        {
            context.write(key, value);
        }

    }
}
