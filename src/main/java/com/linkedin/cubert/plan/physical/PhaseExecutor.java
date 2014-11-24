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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.BufferedTupleOperatorBlock;
import com.linkedin.cubert.block.TupleOperatorBlock;
import com.linkedin.cubert.block.TupleStoreBlock;
import com.linkedin.cubert.functions.builtin.FunctionFactory;
import com.linkedin.cubert.io.rubix.RubixMemoryBlock;
import com.linkedin.cubert.operator.BlockOperator;
import com.linkedin.cubert.operator.OperatorFactory;
import com.linkedin.cubert.operator.OperatorType;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Parses and executes the physical plan operators within a Mapper or a Reducer.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PhaseExecutor
{
    private static final class BlockOperatorInfo
    {
        BlockOperator operator;
        String name;
        int operatorIndex;
        Block currentBlock;

        Iterator<ArrayNode> multipassChainIterator;

        public BlockOperatorInfo(BlockOperator operator,
                                 String name,
                                 int operatorIndex,
                                 Block currentBlock)
        {
            this.operator = operator;
            this.name = name;
            this.operatorIndex = operatorIndex;
            this.currentBlock = currentBlock;
        }
    }

    private ArrayNode operatorsJson;
    private final String outputBlockName;
    private final Configuration conf;

    private List<ArrayNode> multipassOperatorsJson;

    private final Map<String, Block> blocks = new HashMap<String, Block>();
    private final Stack<BlockOperatorInfo> blockOperatorStack =
            new Stack<BlockOperatorInfo>();

    private boolean firstBlock = true;

    private PerfProfiler profiler = null; // Only valid when profile mode is enabled.
    private BufferedTupleOperatorBlock previousProfileOutputBlock = null;

    public PhaseExecutor(String inputBlockName,
                         Block inputBlock,
                         String outputBlockName,
                         ArrayNode operatorsJson,
                         Configuration conf) throws IOException, InterruptedException
    {
        this.conf = conf;
        this.outputBlockName = outputBlockName;
        this.operatorsJson = operatorsJson;
        blocks.put(inputBlockName, inputBlock);

        if (isMultipass(operatorsJson))
        {
            multipassOperatorsJson = extractMultipassChains(operatorsJson);
            this.operatorsJson = multipassOperatorsJson.get(0);
        }

        boolean profileMode = conf.get(CubertStrings.PROFILE_MODE).equals("true");
        if (profileMode)
        {
            profiler =
                    multipassOperatorsJson == null ? new PerfProfiler(operatorsJson)
                            : new PerfProfiler(multipassOperatorsJson);
        }

        prepareOperatorChain(0);
    }

    public Block next() throws IOException,
            InterruptedException
    {
        if (firstBlock)
        {
            firstBlock = false;
            return getOutputBlock();
        }

        while (!blockOperatorStack.isEmpty())
        {
            BlockOperatorInfo info = blockOperatorStack.peek();

            if (info.multipassChainIterator != null)
            {
                // this is the multipass root Block operator
                if (info.multipassChainIterator.hasNext())
                {
                    this.operatorsJson = info.multipassChainIterator.next();
                    if (profiler != null)
                    {
                        if (previousProfileOutputBlock != null)
                            previousProfileOutputBlock.updatePerformanceCounter();
                        profiler.incPass();
                    }

                    prepareOperatorChain(info.operatorIndex + 1);
                    return getOutputBlock();
                }

                info.multipassChainIterator = this.multipassOperatorsJson.iterator();
                this.operatorsJson = info.multipassChainIterator.next();
                if (profiler != null)
                {
                    if (previousProfileOutputBlock != null)
                        previousProfileOutputBlock.updatePerformanceCounter();
                    profiler.resetPass();
                }
            }

            Block nextBlock = info.operator.next();

            if (nextBlock == null)
            {
                blockOperatorStack.pop();
            }
            else
            {
                blocks.put(info.name, nextBlock);
                info.currentBlock = nextBlock;
                prepareOperatorChain(info.operatorIndex + 1);
                return getOutputBlock();
            }
        }

        // Inputs are exhausted, flush the profiling results.
        if (previousProfileOutputBlock != null)
            previousProfileOutputBlock.updatePerformanceCounter();
        return null;
    }

    private Block getOutputBlock()
    {
        Block block = blocks.get(outputBlockName);

        if (profiler != null)
        {
            // Profile mode
            if (!(block instanceof BufferedTupleOperatorBlock))
            {
                System.err.println("WARN: The output block is not a TupleOperatorBlock,"
                        + " profiling result will not be reported.");
            }
            else
            {
                previousProfileOutputBlock = (BufferedTupleOperatorBlock) block;
                previousProfileOutputBlock.setAsOutputBlock(profiler);
            }
        }

        return block;
    }

    void prepareOperatorChain(int startIndex) throws IOException,
            InterruptedException
    {
        int numOperators = operatorsJson.size();

        for (int i = startIndex; i < numOperators; i++)

        {
            JsonNode operatorJson = operatorsJson.get(i);
            if (!operatorJson.has("operator"))
                continue;

            OperatorType type =
                    OperatorType.valueOf(operatorJson.get("operator").getTextValue());

            String name = operatorJson.get("output").getTextValue();

            if (type.isTupleOperator())
            {
                TupleOperator operator =
                        type == OperatorType.USER_DEFINED_TUPLE_OPERATOR
                                ? (TupleOperator) FunctionFactory.createFunctionObject(JsonUtils.getText(operatorJson,
                                                                                                         "class"),
                                                                                       operatorJson.get("constructorArgs"))

                                : OperatorFactory.getTupleOperator(type);

                Map<String, Block> inputBlocks = getInputBlocks(blocks, operatorJson);
                BlockProperties[] parentProps = new BlockProperties[inputBlocks.size()];
                int idx = 0;
                for (Block parent : inputBlocks.values())
                    parentProps[idx++] = parent.getProperties();

                BlockProperties props =
                        new BlockProperties(name,
                                            new BlockSchema(operatorJson.get("schema")),
                                            parentProps);

                operator.setInput(inputBlocks, operatorJson, props);

                Block block = getTupleOperatorBlock(i, operator, props);

                blocks.put(name, block);
            }
            else if (type == OperatorType.LOAD_BLOCK)
            {
                BlockOperator operator = OperatorFactory.getBlockOperator(type);
                Map<String, Block> inputBlocks = getInputBlocks(blocks, operatorJson);
                operator.setInput(conf, inputBlocks, operatorJson);

                Block block = operator.next();
                blocks.put(name, block);
            }
            else
            {
                BlockOperator operator =
                        type == OperatorType.USER_DEFINED_BLOCK_OPERATOR
                                ? (BlockOperator) FunctionFactory.createFunctionObject(JsonUtils.getText(operatorJson,
                                                                                                         "class"),
                                                                                       operatorJson.get("constructorArgs"))
                                : OperatorFactory.getBlockOperator(type);
                Map<String, Block> inputBlocks = getInputBlocks(blocks, operatorJson);
                operator.setInput(conf, inputBlocks, operatorJson);

                Block block = operator.next();
                blocks.put(name, block);

                BlockOperatorInfo info = new BlockOperatorInfo(operator, name, i, block);
                blockOperatorStack.push(info);

                if (operatorJson.has("isMultipassRoot"))
                {
                    info.multipassChainIterator = this.multipassOperatorsJson.iterator();
                    info.multipassChainIterator.next();
                }
            }

        }
    }

    public Block getTupleOperatorBlock(int operatorIndex,
                                       TupleOperator operator,
                                       BlockProperties props)
    {
        if (profiler == null)
            return new TupleOperatorBlock(operator, props);
        else
            return profiler.getProfileOperatorBlock(operatorIndex, operator, props);
    }

    private boolean isMultipass(ArrayNode operatorsJson)
    {
        for (JsonNode json : operatorsJson)
        {
            if (json.has("multipassIndex"))
                return true;
        }

        return false;
    }

    private List<ArrayNode> extractMultipassChains(ArrayNode operatorsJson)
    {
        List<ArrayNode> list = new ArrayList<ArrayNode>();

        // Find the number of passes
        int numPasses = 0;
        for (JsonNode json : operatorsJson)
        {
            if (json.has("multipassIndex"))
            {
                int multipassIndex = json.get("multipassIndex").getIntValue();
                if (multipassIndex + 1 > numPasses)
                    numPasses = multipassIndex + 1;
            }
        }

        // Locate the last BlockOperator that appears before multipass operators.
        // This is the BlockOperator that is the root of the multipass branches.
        JsonNode rootBlockOperator = null;
        for (JsonNode json : operatorsJson)
        {
            if (json.has("multipassIndex"))
            {
                break;
            }

            OperatorType type = OperatorType.valueOf(JsonUtils.getText(json, "operator"));
            if (!type.isTupleOperator())
            {
                rootBlockOperator = json;
            }
        }

        if (rootBlockOperator == null)
            throw new IllegalStateException("No BLOCK operator was found at root on multipass operators");

        ((ObjectNode) rootBlockOperator).put("isMultipassRoot", true);

        // Extract different passes from the chain
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < numPasses; i++)
        {
            ArrayNode anode = mapper.createArrayNode();

            for (JsonNode json : operatorsJson)
            {
                // if this is a multipass operator, then add it only if it belongs to the
                // current pass
                if (json.has("multipassIndex"))
                {
                    int multipassIndex = json.get("multipassIndex").getIntValue();
                    if (multipassIndex == i)
                        anode.add(json);
                }
                // replace the GATHER operator with NO_OP. The input to this operator will
                // correspond to the correct multipass index
                else if (JsonUtils.getText(json, "operator").equals("GATHER"))
                {
                    String[] inputs = JsonUtils.asArray(json, "input");
                    String output = JsonUtils.getText(json, "output");
                    anode.add(JsonUtils.createObjectNode("operator",
                                                         "NO_OP",
                                                         "input",
                                                         inputs[i],
                                                         "output",
                                                         output));
                }
                // all other operators are added as is.
                else
                {
                    anode.add(json);
                }
            }
            list.add(anode);
        }

        return list;
    }

    Map<String, Block> getInputBlocks(Map<String, Block> allBlocks, JsonNode json)
    {
        if (!json.has("input"))
            return null;

        Map<String, Block> inputBlocks = new HashMap<String, Block>();
        String[] inputs = JsonUtils.asArray(json.get("input"));
        for (String input : inputs)
        {
            inputBlocks.put(input, allBlocks.get(input));
        }

        return inputBlocks;
    }

    public void rewindMemoryBlocks(int startIndex) throws IOException
    {

        for (BlockOperatorInfo boi : this.blockOperatorStack)
        {
            if (boi.operatorIndex >= startIndex)
                return;

            if (boi.currentBlock instanceof TupleStoreBlock
                    || boi.currentBlock instanceof RubixMemoryBlock)
                boi.currentBlock.rewind();
        }
    }
}
