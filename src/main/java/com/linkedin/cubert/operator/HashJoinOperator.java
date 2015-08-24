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
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.memory.ColumnarTupleStore;
import com.linkedin.cubert.memory.LookUpTable;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.MemoryStats;
import com.linkedin.cubert.utils.SerializedTupleStore;
import com.linkedin.cubert.utils.TupleStore;
import com.linkedin.cubert.utils.print;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;


public class HashJoinOperator implements TupleOperator
{
    private Map<Tuple, List<Tuple>> rightBlockHashTable;

    private Block leftBlock;
    private Block rightBlock;
    private Tuple leftTuple = null;
    private Tuple keyTuple;
    private RightTupleList matchedRightTupleList = new RightTupleList(0);

    /*
     * SingleNullTupleList, which is a singleton in the context of operator and contains
     * one all-null right tuple.
     */
    private RightTupleList singleNULLTupleList;
    private final RightTupleList emptyTupleList = new RightTupleList(0);

    private int[] leftJoinColumnIndex;
    private int[] rightJoinColumnIndex;

    private RightTupleList rightTupleList;

    int outputCounter = 0;

    Tuple output;

    String[] leftBlockColumns = null;
    String[] rightBlockColumns = null;

    private int nLeftColumns  = -1;
    private int nRightColumns = -1;

    private boolean isLeftJoin = false;
    private boolean isRightJoin = false;

    private Set<Object> matchedRightKeySet;

    private boolean initUnmatchedRightTupleOutput;

    private Iterator<Tuple> unmatchedRightKeyIterator;

    private boolean isLeftBlockExhausted = false;

    private RightTupleList unMatchedRightTupleList;

    private static final String JOIN_CHARACTER = "___";
    private static final String JOIN_TYPE_STR = "joinType";
    private static final String LEFT_OUTER_JOIN = "LEFT OUTER";
    private static final String RIGHT_OUTER_JOIN = "RIGHT OUTER";
    private Counter outputTupleCounter;

    @Override
    public void setInput(Map<String, Block> input, JsonNode root, BlockProperties props) throws
            IOException,
            InterruptedException
    {
        String leftBlockName = JsonUtils.getText(root, "leftBlock");

        for (String name : input.keySet())
        {
            if (name.equalsIgnoreCase(leftBlockName))
            {
                leftBlock = input.get(name);
            }
            else
            {
                rightBlock = input.get(name);
            }
        }

        if (rightBlock == null)
            throw new RuntimeException("RIGHT block is null for join");
        if (leftBlock == null)
            throw new RuntimeException("LEFT block is null for join");

        BlockSchema leftSchema = leftBlock.getProperties().getSchema();
        BlockSchema rightSchema = rightBlock.getProperties().getSchema();

        nLeftColumns = leftSchema.getNumColumns();
        nRightColumns = rightSchema.getNumColumns();

        if (root.has("joinKeys"))
        {
            leftBlockColumns = rightBlockColumns = JsonUtils.asArray(root, "joinKeys");
        }
        else
        {
            leftBlockColumns = JsonUtils.asArray(root, "leftJoinKeys");
            rightBlockColumns = JsonUtils.asArray(root, "rightJoinKeys");
        }

        leftJoinColumnIndex = new int[leftBlockColumns.length];
        rightJoinColumnIndex = new int[rightBlockColumns.length];

        for (int i = 0; i < leftBlockColumns.length; i++)
        {
            leftJoinColumnIndex[i] = leftSchema.getIndex(leftBlockColumns[i]);
            rightJoinColumnIndex[i] = rightSchema.getIndex(rightBlockColumns[i]);
        }

        // this is just keyTuple object that's reused for join key lookup.
        keyTuple = TupleFactory.getInstance().newTuple(leftBlockColumns.length);

        rightTupleList = new RightTupleList();

        if (root.has(JOIN_TYPE_STR))
        {
            if (JsonUtils.getText(root, JOIN_TYPE_STR).equalsIgnoreCase(LEFT_OUTER_JOIN))
            {
                isLeftJoin = true;
            }
            else if (JsonUtils.getText(root, JOIN_TYPE_STR)
                              .equalsIgnoreCase(RIGHT_OUTER_JOIN))
            {
                isRightJoin = true;
                matchedRightKeySet = new HashSet<Object>();
            }
        }

        /*
         * init the SingleNullTupleList, which is a singleton in the context of operator
         * and contains one all-null right tuple.
         */
        singleNULLTupleList = new RightTupleList();
        Tuple nullTuple =
                TupleFactory.getInstance().newTuple(rightSchema.getNumColumns());
        singleNULLTupleList.add(nullTuple);

        output = TupleFactory.getInstance().newTuple(props.getSchema().getNumColumns());

        long startTime = System.currentTimeMillis();
        MemoryStats.print("HASH JOIN OPERATOR before creating hashtable");

        /* Serialize the data and create the Hash Table */
        createHashTable();

        MemoryStats.print("HASH JOIN OPERATOR after creating hashtable");
        long duration = System.currentTimeMillis() - startTime;
        print.f("HashJoinOperator: createHashTable() for %d entries completed in %d ms", rightBlockHashTable.size(),
            duration);
        MemoryStats.printGCStats();

        outputTupleCounter = CubertCounter.HASH_JOIN_OUTPUT_COUNTER.getCounter();
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        final Tuple next = getNextRow();

        /* Increment counter for every row. This count includes the null row for which it is subtracted later on */
        outputCounter++;

        if ((next == null || outputCounter >= 100000) && PhaseContext.getContext() != null)
        {
            /* If there is no next tuple decrement the already incremented value */
            if (next == null) --outputCounter;

            outputTupleCounter.increment(outputCounter);
            outputCounter = 0;
        }

        return next;
    }

    private RightTupleList getMatchedRightTupleList(Tuple leftTuple) throws IOException,
            InterruptedException
    {
        assert (leftTuple != null);

        // the projected
        keyTuple = getProjectedKeyTuple(leftTuple, leftJoinColumnIndex, true);

        List<Tuple> arrayList = rightBlockHashTable.get(keyTuple);

        if (arrayList == null)
        {
            rightTupleList = isLeftJoin ? singleNULLTupleList : emptyTupleList;
        }
        else
        {
            rightTupleList = new RightTupleList();
            rightTupleList.addAll(arrayList);
        }
        rightTupleList.rewind();

        if (arrayList != null && isRightJoin)
        {
            matchedRightKeySet.add(keyTuple);
        }

        return rightTupleList;
    }

    Tuple constructJoinTuple(Tuple leftTuple, Tuple rightTuple) throws ExecException
    {
        int idx = 0;
        for (int i = 0; i < nLeftColumns; i++)
        {
            output.set(idx++, leftTuple.get(i));
        }
        for (int i = 0; i < nRightColumns; i++)
        {
            output.set(idx++, rightTuple.get(i));
        }

        return output;
    }

    private Tuple getNextRow() throws IOException,
            InterruptedException
    {

        while (!isLeftBlockExhausted && matchedRightTupleList.isExhausted())
        {
            leftTuple = leftBlock.next();

            if (leftTuple == null)
            {
                isLeftBlockExhausted = true;
            }
            else
            {
                matchedRightTupleList = getMatchedRightTupleList(leftTuple);
            }
        }

        if (!isLeftBlockExhausted)
        {
            return constructJoinTuple(leftTuple, matchedRightTupleList.getNextTuple());
        }
        else if (isRightJoin)
        {
            // left block exhausted
            // output those un-matched right tuples
            if (!initUnmatchedRightTupleOutput)
            {
                initUnmatchedRightTupleOutput = true;
                Set<Tuple> keySet = rightBlockHashTable.keySet();
                keySet.removeAll(matchedRightKeySet);
                unmatchedRightKeyIterator = keySet.iterator();
                if (!unmatchedRightKeyIterator.hasNext())
                    return null;

                Object key = unmatchedRightKeyIterator.next();

                RightTupleList l = new RightTupleList();
                l.addAll(rightBlockHashTable.get(key));

                unMatchedRightTupleList = l;
            }

            if (unMatchedRightTupleList.isExhausted())
            {

                if (!unmatchedRightKeyIterator.hasNext())
                    return null;

                Object key = unmatchedRightKeyIterator.next();
                RightTupleList l = new RightTupleList();
                l.addAll(rightBlockHashTable.get(key));
                unMatchedRightTupleList = l;
            }

            return constructJoinTuple(singleNULLTupleList.get(0),
                                      unMatchedRightTupleList.getNextTuple());

        }
        else
        {
            return null;
        }
    }

    // The projected keyTuple schema should be the same as that of the JoinKeys
    // New objects are created during hashTable creation, but during other times objects
    // must be reused
    Tuple getProjectedKeyTuple(Tuple inputTuple, int[] indices, boolean makeNewObject) throws ExecException
    {
        Tuple tempTuple;

        if (makeNewObject)
            tempTuple = TupleFactory.getInstance().newTuple(leftBlockColumns.length);
        else
            tempTuple = keyTuple;

        for (int i = 0; i < indices.length; i++)
            tempTuple.set(i, inputTuple.get(indices[i]));

        return tempTuple;
    }

    private void createHashTable() throws IOException,
            InterruptedException
    {
        final TupleStore store;
        final boolean isColumnar = PhaseContext.getConf().getBoolean("cubert.use.hashjoin.storage.columnar", false);

        final long start, end;
        start = System.currentTimeMillis();
        if (isColumnar)
        {
            boolean useDictEncodedStrings =
                    (PhaseContext.isIntialized()
                     && PhaseContext.getConf().getBoolean("cubert.columnar.storage.encode.strings", false));

            store = new ColumnarTupleStore(rightBlock.getProperties().getSchema(), useDictEncodedStrings);
        }
        else
        {
            store = new SerializedTupleStore(rightBlock.getProperties().getSchema(), rightBlockColumns);
        }

        int count = 0;
        Tuple t;
        while ((t = rightBlock.next()) != null)
        {
            store.addToStore(t);
            ++count;
        }
        end = System.currentTimeMillis();
        print.f("HashJoinOperator: Added %d entries to store in %d ms", count, (end - start));

        /* Create the Hash Table */
        if (isColumnar)
        {
            rightBlockHashTable = new LookUpTable(store, rightBlockColumns);
        }
        else
        {
            rightBlockHashTable = ((SerializedTupleStore) store).getHashTable();
            /* Drop the start offsets. This is to be done AFTER creating the Hash Table since, creation requires random
             * access to the store. */
            ((SerializedTupleStore) store).dropIndex();
        }
    }

    @SuppressWarnings("serial")
    private class RightTupleList extends ArrayList<Tuple>
    {

        private int currentTuplePosition = 0;

        public RightTupleList()
        {
        }

        public RightTupleList(int initCapacity)
        {
            super(initCapacity);
        }

        public void rewind()
        {
            currentTuplePosition = 0;
        }

        public Tuple getNextTuple()
        {
            return this.get(currentTuplePosition++);
        }

        public boolean isExhausted()
        {
            return currentTuplePosition == this.size();
        }
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        // get the conditions of input blocks
        String leftBlockName = JsonUtils.getText(json, "leftBlock");
        PostCondition leftCondition = preConditions.get(leftBlockName);
        preConditions.remove(leftBlockName);
        if (preConditions.isEmpty())
            throw new PreconditionException(PreconditionExceptionType.INPUT_BLOCK_NOT_FOUND,
                                            "Only one input block is specified");
        String rightBlockName = preConditions.keySet().iterator().next();
        PostCondition rightCondition = preConditions.get(rightBlockName);

        // validate that the number of join keys are same
        if (json.has("joinKeys"))
        {
            leftBlockColumns = rightBlockColumns = JsonUtils.asArray(json, "joinKeys");
        }
        else
        {
            leftBlockColumns = JsonUtils.asArray(json, "leftJoinKeys");
            rightBlockColumns = JsonUtils.asArray(json, "rightJoinKeys");
        }
        if (leftBlockColumns.length != rightBlockColumns.length)
            throw new RuntimeException("The number of join keys in the left and the right blocks do not match");

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
            type.setName(leftBlockName + JOIN_CHARACTER + leftColType.getName());
            type.setType(leftColType.getType());
            type.setColumnSchema(leftColType.getColumnSchema());

            joinedTypes[idx++] = type;
        }

        for (int i = 0; i < rightSchema.getNumColumns(); i++)
        {
            ColumnType rightColType = rightSchema.getColumnType(i);
            ColumnType type = new ColumnType();
            type.setName(rightBlockName + JOIN_CHARACTER + rightColType.getName());
            type.setType(rightColType.getType());
            type.setColumnSchema(rightColType.getColumnSchema());

            joinedTypes[idx++] = type;
        }

        BlockSchema outputSchema = new BlockSchema(joinedTypes);

        final String[] sortKeys = leftCondition.getSortKeys();
        String[] joinedSortKeys = new String[sortKeys != null ? sortKeys.length : 0];
        for (int i = 0; i < joinedSortKeys.length; i++)
        {
            joinedSortKeys[i] = leftBlockName + JOIN_CHARACTER + sortKeys[i];
        }

        String[] partitionKeys = null;
        if (leftCondition.getPartitionKeys() != null)
        {
            partitionKeys = new String[leftCondition.getPartitionKeys().length];
            for (int i = 0; i < partitionKeys.length; i++)
                partitionKeys[i] =
                        leftBlockName + JOIN_CHARACTER
                                + leftCondition.getPartitionKeys()[i];
        }

        return new PostCondition(outputSchema, partitionKeys, joinedSortKeys);
    }
}
