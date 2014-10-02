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

package com.linkedin.cubert.operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.functions.FunctionTree;
import com.linkedin.cubert.io.StorageFactory;
import com.linkedin.cubert.io.TeeWriter;
import com.linkedin.cubert.utils.JsonUtils;

public class TeeOperator implements TupleOperator
{
    private static Map<String, TeeWriter> openedWriters =
            new HashMap<String, TeeWriter>();

    private Block block;
    private TeeWriter writer;

    private GenerateOperator generateOperator;
    private FunctionTree filterTree;

    public static void closeFiles() throws IOException
    {
        for (TeeWriter writer : openedWriters.values())
            writer.close();
    }

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        block = input.values().iterator().next();
        String prefix = JsonUtils.getText(json, "prefix");

        BlockSchema teeSchema = new BlockSchema(json.get("teeSchema"));

        if (json.has("generate") && !json.get("generate").isNull())
        {
            ObjectNode generateJson =
                    JsonUtils.createObjectNode("name",
                                               "GENERATE",
                                               "input",
                                               json.get("input"),
                                               "output",
                                               json.get("input"),
                                               "outputTuple",
                                               json.get("generate"));

            generateOperator = new GenerateOperator();

            BlockProperties generateProps =
                    new BlockProperties("teeGenerate", teeSchema, props);
            generateOperator.setInput(input, generateJson, generateProps);
        }

        Configuration conf = PhaseContext.getConf();

        Path root = null;
        String filename = null;

        if (PhaseContext.isMapper())
        {
            root = FileOutputFormat.getWorkOutputPath(PhaseContext.getMapContext());
            filename =
                    FileOutputFormat.getUniqueFile(PhaseContext.getMapContext(),
                                                   prefix,
                                                   "");
        }
        else
        {
            root = FileOutputFormat.getWorkOutputPath(PhaseContext.getRedContext());
            filename =
                    FileOutputFormat.getUniqueFile(PhaseContext.getRedContext(),
                                                   prefix,
                                                   "");
        }

        writer = openedWriters.get(prefix);

        if (writer == null)
        {
            writer = StorageFactory.get(JsonUtils.getText(json, "type")).getTeeWriter();
            writer.open(conf, json, teeSchema, root, filename);
            openedWriters.put(prefix, writer);
        }

        if (json.has("filter") && json.get("filter") != null
                && !json.get("filter").isNull())
        {
            JsonNode filterJson = json.get("filter");
            filterTree = new FunctionTree(block);
            try
            {
                filterTree.addFunctionTree(filterJson);
            }
            catch (PreconditionException e)
            {
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple tuple = block.next();
        tee(tuple);
        return tuple;
    }

    private void tee(Tuple tuple) throws IOException,
            InterruptedException
    {
        if (tuple == null)
        {
            writer.flush();
            return;
        }
        boolean isFiltered = true;

        if (filterTree != null)
        {
            filterTree.attachTuple(tuple);
            Boolean val = (Boolean) filterTree.evalTree(0);
            isFiltered = (val != null && val);
        }

        if (isFiltered)
        {
            if (generateOperator == null)
            {
                writer.write(tuple);
            }
            else
            {
                writer.write(generateOperator.next(tuple));
            }
        }
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition preCondition = preConditions.values().iterator().next();
        PostCondition teeCondition = preCondition;

        if (json.has("generate") && !json.get("generate").isNull())
        {
            ObjectNode generateJson =
                    JsonUtils.createObjectNode("name",
                                               "GENERATE",
                                               "input",
                                               json.get("input"),
                                               "output",
                                               json.get("input"),
                                               "outputTuple",
                                               json.get("generate"));

            generateOperator = new GenerateOperator();
            teeCondition = generateOperator.getPostCondition(preConditions, generateJson);
        }

        ((ObjectNode) json).put("teeSchema", teeCondition.getSchema().toJson());

        if (json.has("filter") && json.get("filter") != null
                && !json.get("filter").isNull())
        {
            JsonNode filterJson = json.get("filter");
            FunctionTree tree = new FunctionTree(preCondition.getSchema());
            tree.addFunctionTree(filterJson);
        }

        return preCondition;
    }
}
