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

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.PivotedBlock;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.TupleUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import static com.linkedin.cubert.utils.JsonUtils.asArray;
import static com.linkedin.cubert.utils.JsonUtils.getText;

/**
 * @author Maneesh Varshney
 */
public class RSJoinOperator implements TupleOperator
{
    public static final String TAG_COLUMN = "___tag";
    private PivotedBlock pivotedBlock;

    private Joiner joiner;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props)
            throws IOException, InterruptedException
    {
        Block block = input.values().iterator().next();

        String[] joinKeys = asArray(json, "joinKeys");
        pivotedBlock = new PivotedBlock(block, joinKeys);

        joiner = new Joiner(pivotedBlock, json);
        joiner.newPivot();
    }


    @Override
    public Tuple next() throws IOException, InterruptedException
    {
        while (true)
        {
            Tuple tuple = joiner.next();
            if (tuple != null)
                return tuple;

            if (!pivotedBlock.advancePivot())
                return null;

            joiner.newPivot();
        }
    }

    static final class Joiner
    {
        private final Block block;
        private final int tagIndex;
        private final Tuple rightTuple;
        private final Tuple output;
        private final boolean isLeftOuter;
        private final String[] joinKeys;
        private Tuple leftTuple;

        public Joiner(PivotedBlock block, JsonNode json)
        {
            this.block = block;
            BlockSchema schema = block.getProperties().getSchema();
            tagIndex = schema.getNumColumns() - 1;

            rightTuple = TupleFactory.getInstance().newTuple(schema.getNumColumns());
            output = TupleFactory.getInstance().newTuple(schema.getNumColumns() - 1);

            isLeftOuter = (json.has("joinType") && getText(json, "joinType").equalsIgnoreCase("LEFT OUTER"));
            joinKeys = asArray(json, "joinKeys");
        }

        void newPivot() throws IOException, InterruptedException
        {
            // we have just advanced the pivot

            // fetch the first tuple from this pivot
            Tuple tuple = block.next();

            // if this tuple has tag 0 (that is, row from right table is found)
            if (tuple.get(tagIndex).equals(0))
            {
                // make a copy of this right tuple
                TupleUtils.copy(tuple, rightTuple);

                // fetch the next row (this will be the row from left table)
                tuple = block.next();

                if (tuple != null && tuple.get(tagIndex).equals(0))
                {
                    //  found duplicate value in dimension table. Error out!

                    BlockSchema schema = block.getProperties().getSchema();
                    Map<String, Integer> indexMap = schema.getIndexMap();

                    // exclude TAG_COLUMN
                    String[] values1 = new String[indexMap.size() - 1];
                    String[] values2 = new String[indexMap.size() - 1];
                    String[] columns = new String[indexMap.size() - 1];

                    int i = 0;
                    for (Map.Entry<String, Integer> entry : indexMap.entrySet())
                    {
                        String columName =  entry.getKey();
                        if (columName.equalsIgnoreCase(TAG_COLUMN))
                        {
                            continue;
                        }

                        columns[i] = columName;
                        values1[i] = String.valueOf(rightTuple.get(entry.getValue()));
                        values2[i] = String.valueOf(tuple.get(entry.getValue()));

                        i++;
                    }

                    String message = String.format("Duplicate keys found in dimension table.\n\t%s\n\t%s",
                        Arrays.toString(CommonUtils.zip(columns, values1, ":")),
                        Arrays.toString(CommonUtils.zip(columns, values2, ":")));

                    throw new RuntimeException(message);
                }
            }
            else
            {
                // if we haven't seen any row from right table

                if (isLeftOuter)
                {
                    // if this is outer join, assign nulls to the rightTuple
                    for (int i = 0; i < rightTuple.size(); i++)
                        rightTuple.set(i, null);
                }
                else
                {
                    // inner join: drain the current pivot
                    while ((tuple = block.next()) != null)
                        ;
                }
            }

            leftTuple = tuple;
        }

        Tuple next() throws IOException, InterruptedException
        {
            if (leftTuple == null)
                return null;

            int rightTupleStart = (Integer) leftTuple.get(tagIndex);
            int tupleEnd = output.size();

            // copy the columns from the left tuple
            for (int i = 0; i < rightTupleStart; i++)
            {
                output.set(i, leftTuple.get(i));
            }

            // copy the columns from the right tuple
            for (int i = rightTupleStart; i < tupleEnd; i++)
            {
                output.set(i, rightTuple.get(i));
            }

            // after copying, fetch the next tuple
            leftTuple = block.next();

            return output;
        }

    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions, JsonNode json)
            throws PreconditionException
    {
        PostCondition preCondition = preConditions.values().iterator().next();
        BlockSchema schema = preCondition.getSchema();
        // remove the ___tag columns
        BlockSchema outputSchema = schema.getComplementSubset(new String[]{TAG_COLUMN});

        return new PostCondition(outputSchema, preCondition.getPartitionKeys(), preCondition.getSortKeys());
    }
}
