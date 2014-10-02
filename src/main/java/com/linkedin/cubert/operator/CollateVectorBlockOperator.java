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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BlockUtils;
import com.linkedin.cubert.block.TupleOperatorBlock;
import com.linkedin.cubert.io.BlockSerializationType;
import com.linkedin.cubert.io.rubix.RubixMemoryBlock;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.Pair;
import com.linkedin.cubert.utils.RewriteUtils;

/**
 * Given an input block with a pivoted column and a side meta-data block, create vector
 * blocks for each input in metadata file. * this <code>BlockOperator</code> outputs
 * multiple blocks (one combined block for each vector from meta-data block)
 * 
 * @author Mani Parkhe
 */
public class CollateVectorBlockOperator implements BlockOperator
{
    private RubixMemoryBlock inputBlock;
    private Map<Object, Pair<Integer, Integer>> coord2offsets =
            new HashMap<Object, Pair<Integer, Integer>>();

    private String metaRelationName;
    private Block matchingMetaBlock;
    private int[] coordinateColumnIndexes;
    private String identifierColumnName = null;
    private int identifierColumnIndex;

    private JsonNode jsonForCombine = null;
    private CombineOperator combineOp = new CombineOperator();
    private TupleOperatorBlock combinedBlock = null;

    private Map<String, Block> inputGenerator = new HashMap<String, Block>();
    private JsonNode jsonForGenerate = null;
    private GenerateOperator genOp = null;
    private TupleOperatorBlock generatedBlock = null;

    @SuppressWarnings("unchecked")
    private Class<Tuple> valueClass = (Class<Tuple>) TupleFactory.getInstance()
                                                                 .newTuple()
                                                                 .getClass();

    @Override
    public void setInput(Configuration conf, Map<String, Block> input, JsonNode json) throws IOException,
            InterruptedException
    {
        // #1. input block
        inputBlock = (RubixMemoryBlock) input.get(JsonUtils.getText(json, "inputBlock"));

        // #2. lookup column
        String lookupColumn = json.get("lookupColumn").getTextValue();
        BlockSchema inputSchema = inputBlock.getProperties().getSchema();

        coord2offsets = BlockUtils.generateColumnIndex(inputBlock, lookupColumn);

        // #3. meta data relation name
        metaRelationName = new String(JsonUtils.getText(json, "metaRelationName"));
        matchingMetaBlock = (Block) input.get(metaRelationName);
        BlockSchema metaBlockSchema = matchingMetaBlock.getProperties().getSchema();

        // #4. find indexes for coordinate column names in meta relation's schema
        String[] coordinateColumns = JsonUtils.asArray(json.get("coordinateColumns"));
        coordinateColumnIndexes = new int[coordinateColumns.length];
        int idx = 0;
        for (String s : JsonUtils.asArray(json.get("coordinateColumns")))
            coordinateColumnIndexes[idx++] = metaBlockSchema.getIndex(s);

        // #5. find index of identifier column in meta relation's schema
        identifierColumnName = new String(JsonUtils.getText(json, "identifierColumn"));
        identifierColumnIndex = metaBlockSchema.getIndex(identifierColumnName);

        // #6. combine columns
        ArrayNode combineColumns = (ArrayNode) json.get("combineColumns");

        // setup info for sort operator
        /*
         * jsonForSort = JsonUtils.cloneNode(json); ((ObjectNode)
         * jsonForSort).put("sortBy", combineColumns); sortedBlock = new
         * TupleOperatorBlock(sortOp);
         */

        // setup info for combiner operator
        jsonForCombine = JsonUtils.createObjectNode();
        ((ObjectNode) jsonForCombine).put("pivotBy", combineColumns);
        ((ObjectNode) jsonForCombine).put("schema", inputSchema.toJson());
        combinedBlock = new TupleOperatorBlock(combineOp, null);

        // setup info for generate operator
        jsonForGenerate = JsonUtils.createObjectNode();
    }

    @Override
    public Block next() throws IOException,
            InterruptedException
    {
        Tuple metaDataTuple = matchingMetaBlock.next();
        if (metaDataTuple == null)
            return null; // Done

        System.out.println("Collate Vector: metadata tuple = " + metaDataTuple.toString());
        return generateVectorBlock(metaDataTuple);
    }

    private Block generateVectorBlock(Tuple metaDataTuple) throws ExecException,
            IOException,
            InterruptedException
    {
        Map<String, Block> inputBlocksToCombiner = new HashMap<String, Block>();
        for (int i : coordinateColumnIndexes)
        {
            Object coordinate = metaDataTuple.get(i);
            Block coordBlock = createCoordinateBlock(coordinate);
            if (coordBlock == null)
                continue;
            inputBlocksToCombiner.put(coordinate.toString(), coordBlock);
        }

        // No data for this vector -- proceed to next one.
        if (inputBlocksToCombiner.size() == 0)
            return this.next();

        if (inputBlocksToCombiner.size() != coordinateColumnIndexes.length)
        {
            System.out.println("CollateVectorBlock: Found fewer input blocks than number of co-ordinates ");
            return this.next();
        }

        // Combine individual blocks
        Object vectorIdentifier = metaDataTuple.get(identifierColumnIndex);
        if (!(vectorIdentifier instanceof Integer || vectorIdentifier instanceof String))
            throw new RuntimeException("Unexpected data-type for identifier column");
        Block combinedBlock = createCombinedBlock(inputBlocksToCombiner);

        /*
         * // Prepare input args for sort operator inputSorter.clear();
         * inputSorter.put("combined_block", combinedBlock);
         * 
         * // Setup sort operator object sortOp.setInput(inputSorter, jsonForSort);
         */

        // Prepare input arguments for generator operator
        ArrayNode outputTupleJson = createJsonForGenerate(vectorIdentifier);

        JsonNode thisGenJson = JsonUtils.cloneNode(jsonForGenerate);
        ((ObjectNode) thisGenJson).put("outputTuple", outputTupleJson);

        inputGenerator.clear();
        inputGenerator.put("combined_block", combinedBlock);

        // Setup generate operator object.
        genOp = new GenerateOperator();
        genOp.setInput(inputGenerator, thisGenJson, null);

        // Return tuple operator block that contains this generate op.
        generatedBlock = new TupleOperatorBlock(genOp, null);
        // TODO: generatedBlock.setProperty("identifierColumn", vectorIdentifier);

        // System.out.println("CollateVectorBlock: finished setInput");
        return generatedBlock;
    }

    private ArrayNode createJsonForGenerate(Object vectorIdentifier)
    {
        ArrayNode outputTupleJson = JsonUtils.createArrayNode();

        // + First duplicate existing schema
        for (String s : inputBlock.getProperties().getSchema().getColumnNames())
        {
            outputTupleJson.add(RewriteUtils.createProjectionExpressionNode(s, s));
        }

        // + Add the new generated column
        JsonNode constNode;
        if (vectorIdentifier instanceof String)
            constNode = RewriteUtils.createStringConstant((String) vectorIdentifier);
        else
            constNode = RewriteUtils.createIntegerConstant((Integer) vectorIdentifier);

        String outColName = metaRelationName + "___" + identifierColumnName;
        outputTupleJson.add(JsonUtils.createObjectNode("col_name",
                                                       outColName,
                                                       "expression",
                                                       constNode));
        return outputTupleJson;
    }

    private Block createCombinedBlock(Map<String, Block> inputCombiner) throws IOException,
            InterruptedException
    {
        // Special case -- only one block.
        if (inputCombiner.size() == 1)
        {
            return inputCombiner.values().iterator().next();
        }

        // Setup combine operator object
        combineOp.setInput(inputCombiner, jsonForCombine, null);

        // Return tuple operator block that contains the combine op.
        return combinedBlock;
    }

    private Block createCoordinateBlock(Object coordinate) throws IOException,
            InterruptedException
    {
        Pair<Integer, Integer> offsets = coord2offsets.get(coordinate);
        if (offsets == null)
            return null;

        int start = offsets.getFirst();
        int len = offsets.getSecond() - start;
        ByteBuffer inMemBuffer =
                ByteBuffer.wrap(inputBlock.getByteBuffer().array(), start, len);

        RubixMemoryBlock miniBlock =
                new RubixMemoryBlock(null,
                                     PhaseContext.getConf(),
                                     inMemBuffer,
                                     valueClass,
                                     (CompressionCodec) null,
                                     BlockSerializationType.DEFAULT);

        // This is because the schema and pivotBy attributes which are set in
        // jsonForCombine also apply to this.
        miniBlock.configure(jsonForCombine);

        return miniBlock;
    }
}
