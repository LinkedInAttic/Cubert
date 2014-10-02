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
import java.util.HashSet;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.TupleComparator;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

public class MergeJoinOperator implements TupleOperator
{
    private static final String JOIN_TYPE_STR = "joinType";
    private static final String LEFT_OUTER_JOIN = "LEFT OUTER";
    private static final String RIGHT_OUTER_JOIN = "RIGHT OUTER";
    private static final String FULL_OUTER_JOIN = "FULL OUTER";
    private Block leftBlock;
    private Block rightBlock;
    private Tuple leftTuple = null;
    private Tuple rightTuple = null;

    private String leftName;
    private String rightName;

    private TupleComparator comparator = null;
    private JoinSet currentJoinSet = null;
    private boolean initDone = false;

    int outputCounter = 0;

    String[] leftBlockColumns = null;
    String[] rightBlockColumns = null;
    private boolean isLeftJoin = false;
    private boolean isRightJoin = false;
    private boolean isFullOuterJoin = false;
    private Tuple nullRightTuple, nullLeftTuple;
    private Tuple joinedTuple;

    // . or - are not compatible with avro; hence this string
    private static String JOIN_SEP = "___";

    @Override
    public void setInput(Map<String, Block> input, JsonNode root, BlockProperties props) throws JsonParseException,
            JsonMappingException,
            IOException
    {
        String leftBlockName = JsonUtils.getText(root, "leftBlock");

        for (String name : input.keySet())
        {
            if (name.equalsIgnoreCase(leftBlockName))
            {
                leftName = name;
                leftBlock = input.get(name);
            }
            else
            {
                rightName = name;
                rightBlock = input.get(name);
            }
        }

        if (root.has(JOIN_TYPE_STR))
        {
            if (JsonUtils.getText(root, JOIN_TYPE_STR).equalsIgnoreCase(LEFT_OUTER_JOIN))
                isLeftJoin = true;

            if (JsonUtils.getText(root, JOIN_TYPE_STR).equalsIgnoreCase(RIGHT_OUTER_JOIN))
                isRightJoin = true;
            if (JsonUtils.getText(root, JOIN_TYPE_STR).equalsIgnoreCase(FULL_OUTER_JOIN))
                isFullOuterJoin = true;
        }

        if (rightBlock == null)
            throw new RuntimeException("RIGHT block is null for join");
        if (leftBlock == null)
            throw new RuntimeException("LEFT block is null for join");

        leftBlockColumns = JsonUtils.asArray(root, "leftCubeColumns");
        rightBlockColumns = JsonUtils.asArray(root, "rightCubeColumns");

        BlockSchema leftSchema = leftBlock.getProperties().getSchema();
        BlockSchema rightSchema = rightBlock.getProperties().getSchema();

        comparator =
                new TupleComparator(leftSchema,
                                    leftBlockColumns,
                                    rightSchema,
                                    rightBlockColumns);

        /*
         * init the SingleNullTupleList, which is a singleton in the context of operator
         * and contains one all-null right tuple.
         */
        nullRightTuple = TupleFactory.getInstance().newTuple(rightSchema.getNumColumns());
        nullLeftTuple = TupleFactory.getInstance().newTuple(leftSchema.getNumColumns());

        joinedTuple =
                TupleFactory.getInstance().newTuple(props.getSchema().getNumColumns());
        currentJoinSet = new JoinSet(joinedTuple);
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        outputCounter++;
        if (outputCounter % 1000 == 0)
        {
            PhaseContext.getCounter("mergejoinoperator", "outputCounter")
                        .increment(outputCounter);

            outputCounter = 0;
        }

        if (!initDone)
        {
            initDone = true;

            // get a tuple from each of the cube to start off
            leftTuple = leftBlock.next();
            rightTuple = rightBlock.next();
        }

        while (true)
        {
            if (currentJoinSet != null && currentJoinSet.hasNext())
            {
                return currentJoinSet.next();
            }
            else if (leftTuple == null && rightTuple == null)
            {
                return null;

            }

            // else if ((leftTuple == null && !isRightJoin && !isFullOuterJoin)
            // || (rightTuple == null && !isLeftJoin && !isFullOuterJoin))
            // {
            // return null;
            // }

            else if (currentJoinSet != null
                    && leftTuple != null
                    && currentJoinSet.rightTupleList.size() > 0
                    && comparator.compare(leftTuple, currentJoinSet.rightTupleList.get(0)) == 0)
            {
                Tuple clonedLeft =
                        TupleFactory.getInstance().newTuple(leftTuple.getAll());
                currentJoinSet.reset(clonedLeft);
                leftTuple = leftBlock.next();

            }
            else if (leftTuple != null
                    && (rightTuple == null || comparator.compare(leftTuple, rightTuple) < 0))
            {
                if (isLeftJoin || isFullOuterJoin)
                {
                    joinedTuple = buildJoinedTuple(leftTuple, nullRightTuple);
                    leftTuple = leftBlock.next();
                    return joinedTuple;
                }
                else
                {

                    leftTuple = leftBlock.next();
                }
            }
            else if (rightTuple != null
                    && (leftTuple == null || comparator.compare(leftTuple, rightTuple) > 0))
            {
                if (isRightJoin || isFullOuterJoin)
                {
                    joinedTuple = buildJoinedTuple(nullLeftTuple, rightTuple);
                    rightTuple = rightBlock.next();
                    return joinedTuple;
                }
                else
                {
                    rightTuple = rightBlock.next();
                }
            }
            else
            {

                /*
                 * this is the case that we will use the leftTuple and pull a new
                 * rightTuple list for the joinSet
                 */
                assert (comparator.compare(leftTuple, rightTuple) == 0);

                currentJoinSet = getValidJoinSet();
                leftTuple = leftBlock.next();
            }

        }
    }

    private JoinSet getValidJoinSet() throws IOException,
            InterruptedException
    {
        if (leftTuple == null)
            return null;

        Tuple clonedLeft = TupleFactory.getInstance().newTuple(leftTuple.getAll());
        currentJoinSet.reset(clonedLeft);
        currentJoinSet.rightTupleList.clear();

        while (rightTuple != null && comparator.compare(leftTuple, rightTuple) == 0)
        {
            Tuple cloned = TupleFactory.getInstance().newTuple(rightTuple.getAll());
            currentJoinSet.rightTupleList.add(cloned);

            rightTuple = rightBlock.next();
        }

        return currentJoinSet;
    }

    private Tuple buildJoinedTuple(Tuple left, Tuple right) throws ExecException
    {
        int idx = 0;
        for (Object field : left.getAll())
        {
            joinedTuple.set(idx++, field);
        }
        for (Object field : right.getAll())
        {
            joinedTuple.set(idx++, field);
        }

        return joinedTuple;
    }

    public static class JoinSet
    {
        public ArrayList<Tuple> rightTupleList;

        private final Tuple joinedTuple;

        private Tuple leftTuple;
        private int currentTuplePosition = 0;

        public JoinSet(Tuple joinedTuple)
        {
            this.joinedTuple = joinedTuple;
            rightTupleList = new ArrayList<Tuple>();
            currentTuplePosition = 0;
        }

        public void reset(Tuple ltuple)
        {
            this.leftTuple = ltuple;
            currentTuplePosition = 0;
        }

        public boolean hasNext()
        {
            return (currentTuplePosition < rightTupleList.size());
        }

        public Tuple next() throws ExecException
        {
            if (currentTuplePosition == rightTupleList.size())
                return null;

            int idx = 0;
            for (Object field : leftTuple.getAll())
            {
                joinedTuple.set(idx++, field);
            }
            for (Object field : rightTupleList.get(currentTuplePosition).getAll())
            {
                joinedTuple.set(idx++, field);
            }

            currentTuplePosition++;
            return joinedTuple;
        }
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        // check the left block is available
        String leftBlockName = JsonUtils.getText(json, "leftBlock");
        PostCondition leftCondition = preConditions.get(leftBlockName);

        // get the right block conditions
        HashSet<String> inputBlockNames = new HashSet<String>(preConditions.keySet());
        inputBlockNames.remove(leftBlockName);
        // preConditions.remove(leftBlockName);
        if (preConditions.isEmpty())
            throw new PreconditionException(PreconditionExceptionType.INPUT_BLOCK_NOT_FOUND,
                                            "Only one input block is specified");
        String rightBlockName = inputBlockNames.iterator().next();
        PostCondition rightCondition = preConditions.get(rightBlockName);

        String[] leftColumns = JsonUtils.asArray(json, "leftCubeColumns");
        String[] rightColumns = JsonUtils.asArray(json, "rightCubeColumns");

        // test that the blocks are partitioned on join columns
        if (!CommonUtils.isPrefix(leftColumns, leftCondition.getPartitionKeys()))
            throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS,
                                            String.format("Block %s. Expected: %s. Found: %s",
                                                          leftBlockName,
                                                          Arrays.toString(leftColumns),
                                                          Arrays.toString(leftCondition.getPartitionKeys())));

        if (!CommonUtils.isPrefix(rightColumns, rightCondition.getPartitionKeys()))
            throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS,
                                            String.format("Block %s. Expected: %s. Found: %s",
                                                          rightBlockName,
                                                          Arrays.toString(rightColumns),
                                                          Arrays.toString(rightCondition.getPartitionKeys())));

        // test that block is sorted on join columns
        if (!CommonUtils.isPrefix(leftCondition.getSortKeys(), leftColumns))
            throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                            String.format("Block %s. Expected: %s. Found: %s",
                                                          leftBlockName,
                                                          Arrays.toString(leftColumns),
                                                          Arrays.toString(leftCondition.getSortKeys())));

        if (!CommonUtils.isPrefix(rightCondition.getSortKeys(), rightColumns))
            throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                            "Block " + rightBlockName);

        // create block schema
        BlockSchema leftSchema = leftCondition.getSchema();
        BlockSchema rightSchema = rightCondition.getSchema();

        ColumnType[] joinedTypes =
                new ColumnType[leftSchema.getNumColumns() + rightSchema.getNumColumns()];
        int idx = 0;
        for (int i = 0; i < leftSchema.getNumColumns(); i++)
        {
            ColumnType leftColType = leftSchema.getColumnType(i);
            ColumnType type = new ColumnType();
            type.setName(leftBlockName + JOIN_SEP + leftColType.getName());
            type.setType(leftColType.getType());
            type.setColumnSchema(leftColType.getColumnSchema());

            joinedTypes[idx++] = type;
        }

        for (int i = 0; i < rightSchema.getNumColumns(); i++)
        {
            ColumnType rightColType = rightSchema.getColumnType(i);
            ColumnType type = new ColumnType();
            type.setName(rightBlockName + JOIN_SEP + rightColType.getName());
            type.setType(rightColType.getType());
            type.setColumnSchema(rightColType.getColumnSchema());

            joinedTypes[idx++] = type;
        }

        BlockSchema outputSchema = new BlockSchema(joinedTypes);

        // create sort keys
        String[] joinedSortKeys = new String[leftColumns.length + rightColumns.length];
        for (int i = 0; i < leftColumns.length; i += 2)
        {
            joinedSortKeys[2 * i] = leftBlockName + JOIN_SEP + leftColumns[i];
            joinedSortKeys[2 * i + 1] = rightBlockName + JOIN_SEP + rightColumns[i];
        }

        String[] partitionKeys = new String[leftCondition.getPartitionKeys().length];
        for (int i = 0; i < partitionKeys.length; i++)
            partitionKeys[i] =
                    leftBlockName + JOIN_SEP + leftCondition.getPartitionKeys()[i];

        // if there were pivot keys, we need to add those to sort keys as well (they will
        // be added in the "front" of the sort key list)
        ArrayList<String> pivotKeys = new ArrayList<String>();
        if (leftCondition.getPivotKeys() != null)
        {
            for (String key : leftCondition.getPivotKeys())
            {
                if (key.equals(leftColumns[0]))
                    break;
                pivotKeys.add(leftBlockName + JOIN_SEP + key);
            }
        }

        if (rightCondition.getPivotKeys() != null)
        {
            for (String key : rightCondition.getPivotKeys())
            {
                if (key.equals(rightColumns[0]))
                    break;
                pivotKeys.add(rightBlockName + JOIN_SEP + key);
            }
        }

        pivotKeys.addAll(Arrays.asList(joinedSortKeys));
        joinedSortKeys = pivotKeys.toArray(new String[] {});

        return new PostCondition(outputSchema, partitionKeys, joinedSortKeys);
    }
}
