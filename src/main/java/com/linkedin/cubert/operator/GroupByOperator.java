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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.PivotedBlock;
import com.linkedin.cubert.operator.aggregate.AggregationFunction;
import com.linkedin.cubert.operator.aggregate.AggregationFunctions;
import com.linkedin.cubert.operator.aggregate.AggregationType;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Group by operator implementation for the framework. The main assumption is that the
 * group by keys are also the pivot keys of the input cube.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class GroupByOperator implements TupleOperator
{
    public static final String GROUP_BY_COLUMNNAMES = "groupBy";

    private Block pivotedBlock;

    private Tuple output = null;
    private int[] groupByColumnIndex;

    private final List<AggregationFunction> aggregators =
            new ArrayList<AggregationFunction>();

    private boolean moreData = true;
    private boolean isGroupedAll = false;

    @Override
    public void setInput(Map<String, Block> input, JsonNode root, BlockProperties props) throws IOException,
            InterruptedException
    {
        // get the input block
        Block dataBlock = input.values().iterator().next();

        BlockSchema inputSchema = dataBlock.getProperties().getSchema();
        BlockSchema outputSchema = props.getSchema();
        output = TupleFactory.getInstance().newTuple(outputSchema.getNumColumns());

        // create pivoted block
        if (((ArrayNode) root.get(GROUP_BY_COLUMNNAMES)).size() > 0)
        {
            isGroupedAll = false;
            String[] groupByColumns = JsonUtils.asArray(root, GROUP_BY_COLUMNNAMES);
            pivotedBlock = new PivotedBlock(dataBlock, groupByColumns);

            // store the index of groupby columns
            BlockSchema groupBySchema = inputSchema.getSubset(groupByColumns);
            groupByColumnIndex = new int[groupBySchema.getNumColumns()];
            for (int i = 0; i < groupByColumnIndex.length; i++)
            {
                groupByColumnIndex[i] = inputSchema.getIndex(groupBySchema.getName(i));
            }
        }
        else
        {
            isGroupedAll = true;
            pivotedBlock = dataBlock;
        }

        if (root.has("aggregates"))
        {
            for (JsonNode aggregateJson : root.path("aggregates"))
            {
                AggregationType aggType =
                        AggregationType.valueOf(JsonUtils.getText(aggregateJson, "type"));

                AggregationFunction aggregator =
                        AggregationFunctions.get(aggType, aggregateJson);

                aggregator.setup(pivotedBlock, outputSchema, aggregateJson);
                aggregators.add(aggregator);
            }
        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        // for all the tuples in the current pivot, apply the aggregation operator and
        // then return the tuple with the aggregation columns added;

        if (!moreData)
            return null;

        // reset the aggregator state and the output tuple (the output tuple is reused)
        for (AggregationFunction aggregator : aggregators)
        {
            aggregator.resetState();
        }

        // read the input rows and compute the aggregates
        Tuple tuple;
        boolean firstTuple = true;
        boolean noRecordSeen = true;
        while ((tuple = pivotedBlock.next()) != null)
        {
            if (firstTuple)
            {
                if (!isGroupedAll)
                {
                    for (int i = 0; i < groupByColumnIndex.length; i++)
                    {
                        output.set(i, tuple.get(groupByColumnIndex[i]));
                    }
                }

                firstTuple = false;
                noRecordSeen = false;
            }

            for (AggregationFunction aggregator : aggregators)
            {
                aggregator.aggregate(tuple);
            }
        }

        // copy the computed aggregates to the output tuple
        for (AggregationFunction aggregator : aggregators)
        {
            aggregator.output(output);
        }

        if (isGroupedAll)
            moreData = false;
        else
            moreData = ((PivotedBlock) pivotedBlock).advancePivot();

        if (noRecordSeen)
            return null;
        else
            return output;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();
        String[] partitionKeys = condition.getPartitionKeys();
        String[] sortKeys = condition.getSortKeys();
        if (condition.getPivotKeys() != null)
            sortKeys = CommonUtils.concat(condition.getPivotKeys(), sortKeys);

        BlockSchema outputSchema;
        String[] groupByColumns = JsonUtils.asArray(json, GROUP_BY_COLUMNNAMES);

        // test that group by columns are present
        for (String groupByColumn : groupByColumns)
        {
            if (!inputSchema.hasIndex(groupByColumn))
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                "Column [" + groupByColumn
                                                        + "] not present.");
        }

        // test that block is sorted on group by columns
        if (groupByColumns.length > 0)
        {
            if (!CommonUtils.isPrefix(sortKeys, groupByColumns))
            {
                System.out.println("Input SortKeys = " + Arrays.toString(sortKeys));
                throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS);
            }
        }
        // generate the output schema
        if (((ArrayNode) json.get(GROUP_BY_COLUMNNAMES)).size() > 0)
        {
            outputSchema = inputSchema.getSubset(groupByColumns);
        }
        else
        {
            outputSchema = new BlockSchema(new ColumnType[] {});
        }

        String[] fullExpectedSortKeys = groupByColumns;
        boolean countDistinctAggPresent = false;
        if (json.has("aggregates"))
        {
            for (JsonNode aggregateJson : json.path("aggregates"))
            {
                // BlockSchema aggOutputSchema;
                AggregationType aggType =
                        AggregationType.valueOf(JsonUtils.getText(aggregateJson, "type"));
                AggregationFunction aggregator = null;
                aggregator = AggregationFunctions.get(aggType, aggregateJson);
                if (aggregator == null)
                    throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                                    "Cannot instantiate aggregation operator for type "
                                                            + aggType);

                BlockSchema aggOutputSchema =
                        aggregator.outputSchema(inputSchema, aggregateJson);

                outputSchema = outputSchema.append(aggOutputSchema);

                // Check pre-condition for COUNT-DISTINCT

                String[] measureColumn = JsonUtils.asArray(aggregateJson.get("input"));

                if (aggType == AggregationType.COUNT_DISTINCT)
                {

                    if (countDistinctAggPresent)
                        throw new PreconditionException(PreconditionExceptionType.INVALID_GROUPBY);
                    countDistinctAggPresent = true;
                    fullExpectedSortKeys =
                            CommonUtils.concat(groupByColumns, measureColumn);
                    if (!CommonUtils.isPrefix(sortKeys, fullExpectedSortKeys))
                    {
                        String errorMesg =
                                "Expecting sortkeys = "
                                        + CommonUtils.join(fullExpectedSortKeys, ",")
                                        + " actual = " + CommonUtils.join(sortKeys, ",");
                        throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                                        errorMesg);
                    }
                }

            }
        }

        return new PostCondition(outputSchema, partitionKeys, groupByColumns);
    }
}
