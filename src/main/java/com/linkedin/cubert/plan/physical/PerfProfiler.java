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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BufferedTupleOperatorBlock;
import com.linkedin.cubert.operator.OperatorType;
import com.linkedin.cubert.operator.PhaseContext;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.utils.JsonUtils;

/*
 * Utility functions for performance profiling (when -perf is enabled).
 */
public class PerfProfiler
{
    private static final String mapperProfileCounterGroupName =
            "MapperPerformanceCounter";
    private static final String reducerProfileCounterGroupName =
            "ReducerPerformanceCounter";

    /*
     * Auxiliary data for performance profile. First dimension indicates the pass index,
     * and the second dimension indicates the index of the operator in the pass.
     */
    private BufferedTupleOperatorBlock[][] profileOperatorBlock;
    private LongWritable[][] cumulativeOperatorTime;
    private List<Integer>[][] operatorDependency;
    List<ArrayNode> multipassOperatorsJson;
    private int currentPassIndex;

    public PerfProfiler(List<ArrayNode> multipassOperatorsJson)
    {
        this.multipassOperatorsJson = multipassOperatorsJson;
        Setup();
    }

    public PerfProfiler(ArrayNode operatorsJson)
    {
        multipassOperatorsJson = new ArrayList<ArrayNode>();
        multipassOperatorsJson.add(operatorsJson);
        Setup();
    }

    private void Setup()
    {
        int numPass = multipassOperatorsJson.size();
        profileOperatorBlock = new BufferedTupleOperatorBlock[numPass][];
        cumulativeOperatorTime = new LongWritable[numPass][];
        operatorDependency = new List[numPass][];
        currentPassIndex = 0;

        for (int i = 0; i < numPass; i++)
            SetupPass(i, multipassOperatorsJson.get(i));
    }

    private void SetupPass(int passIndex, ArrayNode operatorsJson)
    {
        profileOperatorBlock[passIndex] =
                new BufferedTupleOperatorBlock[operatorsJson.size()];
        cumulativeOperatorTime[passIndex] = new LongWritable[operatorsJson.size()];
        operatorDependency[passIndex] = getOperatorDependency(operatorsJson);
    }

    // Get the parent operators for each operator.
    private static List<Integer>[] getOperatorDependency(ArrayNode operatorsJson)
    {
        Map<String, Integer> relationName2OperatorID = new HashMap<String, Integer>();
        int numOperators = operatorsJson.size();
        List<Integer>[] inputOperatorIDs = new ArrayList[numOperators];

        for (int i = 0; i < numOperators; i++)
        {
            JsonNode operatorJson = operatorsJson.get(i);
            if (!operatorJson.has("operator"))
                continue;

            OperatorType type =
                    OperatorType.valueOf(operatorJson.get("operator").getTextValue());
            String outputName = operatorJson.get("output").getTextValue();

            if (type.isTupleOperator())
            {
                inputOperatorIDs[i] =
                        getInputOperatorIDs(relationName2OperatorID, operatorJson);
                relationName2OperatorID.put(outputName, i);
            }
        }

        return inputOperatorIDs;
    }

    private static List<Integer> getInputOperatorIDs(Map<String, Integer> relationName2OperatorID,
                                                     JsonNode json)
    {
        if (!json.has("input"))
            return null;

        List<Integer> inputOperatorIDs = new ArrayList<Integer>();
        String[] inputs = JsonUtils.asArray(json.get("input"));
        for (String input : inputs)
        {
            Integer inputOperatorID = relationName2OperatorID.get(input);
            if (inputOperatorID != null)
                inputOperatorIDs.add(inputOperatorID);
        }

        return inputOperatorIDs.size() == 0 ? null : inputOperatorIDs;
    }

    public void updatePerformanceCounter()
    {
        updateCounter();
        resetOperatorTime();
    }

    // Obtain the time spending on each operator by subtracting the time from its upstream
    // operator.
    public long[] getOperatorTime()
    {
        LongWritable[] curCumulativeOperatorTime =
                cumulativeOperatorTime[currentPassIndex];
        List<Integer>[] curOperatorDependency = operatorDependency[currentPassIndex];

        long[] operatorTime = new long[curCumulativeOperatorTime.length];
        for (int i = 0; i < operatorTime.length; i++)
        {
            if (curCumulativeOperatorTime[i] != null)
            {
                operatorTime[i] = curCumulativeOperatorTime[i].get();
                if (curOperatorDependency[i] != null)
                {
                    for (int parentID : curOperatorDependency[i])
                        operatorTime[i] -= curCumulativeOperatorTime[parentID].get();
                }
            }
            else
            {
                operatorTime[i] = -1;
            }
        }

        return operatorTime;
    }

    private void resetOperatorTime()
    {
        LongWritable[] curCumulativeOperatorTime =
                cumulativeOperatorTime[currentPassIndex];

        for (LongWritable singleOperatorTime : curCumulativeOperatorTime)
        {
            if (singleOperatorTime != null)
                singleOperatorTime.set(0);
        }
    }

    // Update the counter.
    private void updateCounter()
    {
        long[] operatorTime = getOperatorTime();

        String profileCounterGroupName =
                PhaseContext.isMapper() ? mapperProfileCounterGroupName
                        : reducerProfileCounterGroupName;

        ArrayNode operatorsJson = multipassOperatorsJson.get(currentPassIndex);
        for (int i = 0; i < operatorTime.length; i++)
        {
            if (operatorTime[i] > 0)
            {
                JsonNode operatorJson = operatorsJson.get(i);

                OperatorType type =
                        OperatorType.valueOf(operatorJson.get("operator").getTextValue());
                String outputName = operatorJson.get("output").getTextValue();

                String counterName =
                        String.format("P%d-O%d-%s-%s",
                                      currentPassIndex,
                                      i,
                                      type,
                                      outputName);
                Counter profileCounter =
                        PhaseContext.getCounter(profileCounterGroupName, counterName);
                profileCounter.increment(operatorTime[i]);
            }
        }
    }

    public void incPass()
    {
        currentPassIndex++;
    }

    public void resetPass()
    {
        currentPassIndex = 0;
    }

    public Block getProfileOperatorBlock(int operatorIndex,
                                         TupleOperator operator,
                                         BlockProperties props)
    {
        BufferedTupleOperatorBlock[] curProfileOperatorBlock =
                profileOperatorBlock[currentPassIndex];
        LongWritable[] curCumulativeOperatorTime =
                cumulativeOperatorTime[currentPassIndex];

        if (curProfileOperatorBlock[operatorIndex] == null)
        {
            curCumulativeOperatorTime[operatorIndex] = new LongWritable();
            curProfileOperatorBlock[operatorIndex] =
                    new BufferedTupleOperatorBlock(operator,
                                                   props,
                                                   curCumulativeOperatorTime[operatorIndex]);
        }
        else
        {
            curProfileOperatorBlock[operatorIndex].reset(operator);
        }

        return curProfileOperatorBlock[operatorIndex];
    }
}
