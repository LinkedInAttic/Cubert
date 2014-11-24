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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockWriter;
import com.linkedin.cubert.block.CommonContext;
import com.linkedin.cubert.block.ContextBlock;
import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.TeeOperator;
import com.linkedin.cubert.utils.FileCache;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.MemoryStats;
import com.linkedin.cubert.utils.print;

/**
 * Executes the physical plan at the reducer.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CubertReducer extends Reducer<Tuple, Tuple, Object, Object>
{
    @Override
    public void run(Context context) throws IOException,
            InterruptedException
    {
        print.f("Reducer init --------------------------------");
        Configuration conf = context.getConfiguration();

        FileCache.initialize(conf);
        PhaseContext.create(context, conf);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode shuffleJson =
                mapper.readValue(conf.get(CubertStrings.JSON_SHUFFLE), JsonNode.class);
        JsonNode outputJson =
                mapper.readValue(conf.get(CubertStrings.JSON_OUTPUT), JsonNode.class);
        ArrayNode operatorsJson =
                mapper.readValue(conf.get(CubertStrings.JSON_REDUCE_OPERATOR_LIST),
                                 ArrayNode.class);

        // Create input block
        CommonContext commonContext = new ReduceContext(context);
        Block input = new ContextBlock(commonContext);
        input.configure(shuffleJson);

        // Create phase executor
        PhaseExecutor exec =
                new PhaseExecutor(shuffleJson.get("name").getTextValue(),
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
        }

        // HACK!! Asking the TeeOperator to close the files that were opened
        TeeOperator.closeFiles();

        print.f("Reducer operators completed in %d ms", System.currentTimeMillis()
                - start);
        print.f("----------------------------------");
        MemoryStats.printGCStats();
    }

    void printMemory(String msg)
    {

        Runtime rt = Runtime.getRuntime();
        long mem1 = rt.totalMemory() - rt.freeMemory();
        System.gc();

        long mem2 = rt.totalMemory() - rt.freeMemory();
        print.f("%s %.3fMB => %.3fMB",
                msg,
                1.0 * mem1 / 1024 / 1024,
                1.0 * mem2 / 1024 / 1024);
    }

    static final class ReduceContext implements CommonContext
    {
        private final Context context;

        ReduceContext(org.apache.hadoop.mapreduce.Reducer.Context context)
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
