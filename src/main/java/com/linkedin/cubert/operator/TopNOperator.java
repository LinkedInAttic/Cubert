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
import java.util.Arrays;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Tuple operator that works on groups and sorted data set to extract TOP-N tuples.
 * 
 * @author Mani Parkhe
 */
public class TopNOperator implements TupleOperator
{
    private Block inputBlock = null;

    private PivotBlockOperator pivotBlockOp = null;
    private Block currentBlock = null;

    private int current_n = 0;
    private int top_n = 1;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        inputBlock = input.get(JsonUtils.getText(json, "input"));

        if (json.has("topN"))
            top_n = json.get("topN").getIntValue();

        pivotBlockOp = new PivotBlockOperator();
        String[] groupByColumns = JsonUtils.asArray(JsonUtils.get(json, "groupBy"));
        pivotBlockOp.setInput(inputBlock, groupByColumns, false);

        resetState();
    }

    /**
     * reset state of the operator between pivot blocks
     */
    private void resetState()
    {
        current_n = 0;
        currentBlock = null;
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (currentBlock == null)
        {
            if (!nextBlock())
                return null;
        }

        // Advance pivot if this block is exhausted or top_n have been exhausted.
        Tuple tuple = currentBlock.next();

        if ((tuple != null) && (++current_n <= top_n))
        {
            return tuple;
        }

        resetState();
        return this.next();
    }

    /*
     * Get next pivot block.
     */
    private boolean nextBlock() throws IOException,
            InterruptedException
    {
        // Get next pivot block
        currentBlock = pivotBlockOp.next();

        if (null == currentBlock)
            return false;

        return true;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        String inputBlock = JsonUtils.getText(json, "input");
        PostCondition inputBlockPostCondition = preConditions.get(inputBlock);
        BlockSchema inputSchema = inputBlockPostCondition.getSchema();

        // Check: partition order
        // Logic -- partition keys should match group-by keys or shud prefix group-by
        // keys.
        // since the sorted order check (below) will ensure that all group+order columsn
        // will be
        // seen together for top-N query.
        String[] groupByKeys = JsonUtils.asArray(JsonUtils.get(json, "groupBy"));
        String[] partitionKeys = inputBlockPostCondition.getPartitionKeys();
        if (groupByKeys.length == 0 || partitionKeys.length == 0
                || !CommonUtils.isPrefix(groupByKeys, partitionKeys))
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS,
                                            String.format("Found=%s, Expected=%s",
                                                          partitionKeys == null
                                                                  ? "[null]"
                                                                  : Arrays.toString(partitionKeys),
                                                          groupByKeys == null
                                                                  ? "[null]"
                                                                  : Arrays.toString(groupByKeys)));
        }

        // Check: all groupBy columns exist in inputBlock's schema
        for (String colName : groupByKeys)
        {
            if (!inputSchema.hasIndex(colName))
            {
                String msg =
                        String.format("Input block '%s' is missing expected column '%s'",
                                      inputBlock,
                                      colName);
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                msg);
            }
        }

        // Check: sorting order
        String[] orderByKeys = JsonUtils.asArray(JsonUtils.get(json, "orderBy"));
        String[] expectedSortOrder = CommonUtils.concat(groupByKeys, orderByKeys);
        String[] sortKeys = inputBlockPostCondition.getSortKeys();
        if (!CommonUtils.isPrefix(sortKeys, expectedSortOrder))
        {
            throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                            String.format("Found=%s, Expected=%s",
                                                          sortKeys == null
                                                                  ? "[null]"
                                                                  : Arrays.toString(sortKeys),
                                                          expectedSortOrder == null
                                                                  ? "[null]"
                                                                  : Arrays.toString(expectedSortOrder)));
        }

        // Check: all orderBy columns exist in inputBlock's schema
        for (String colName : orderByKeys)
        {
            if (!inputSchema.hasIndex(colName))
            {
                String msg =
                        String.format("Input block '%s' is missing expected column '%s'",
                                      inputBlock,
                                      colName);
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                msg);
            }
        }

        return inputBlockPostCondition;
    }

}
