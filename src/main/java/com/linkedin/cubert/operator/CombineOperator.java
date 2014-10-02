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
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.TupleComparator;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * TupleOperator class that combines two or more cubes, while preserving the pivot key
 * ordering.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CombineOperator implements TupleOperator
{
    private static final class PQEntry
    {
        Tuple tuple;
        Block block;

        public void next() throws IOException,
                InterruptedException
        {
            this.tuple = this.block.next();
        }
    }

    private static final class PQEntryComparator implements Comparator<PQEntry>
    {
        private final TupleComparator comparator;

        PQEntryComparator(TupleComparator comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int compare(PQEntry o1, PQEntry o2)
        {
            return comparator.compare(o1.tuple, o2.tuple);
        }
    }

    private PriorityQueue<PQEntry> pqueue;
    private PQEntry lastEntry;

    private boolean fastPath = false;
    private PQEntry[] entryList;

    private PQEntryComparator pqcomparator;

    @Override
    public void setInput(Map<String, Block> input, JsonNode root, BlockProperties props) throws IOException,
            InterruptedException
    {
        String[] pivotColumns = JsonUtils.asArray(root, "pivotBy");

        TupleComparator comparator = new TupleComparator(props.getSchema(), pivotColumns);
        pqcomparator = new PQEntryComparator(comparator);
        lastEntry = null;

        if (input.size() <= 2)
        {
            fastPath = true;
            entryList = new PQEntry[input.size()];
            int i = 0;
            for (Block block : input.values())
            {
                PQEntry entry = new PQEntry();
                entry.block = block;
                entry.tuple = block.next();
                entryList[i++] = entry;
            }
            return;
        }

        pqueue = new PriorityQueue<PQEntry>(input.size(), pqcomparator);

        for (Block block : input.values())
        {
            Tuple tuple = block.next();
            if (tuple == null)
                continue;

            PQEntry entry = new PQEntry();
            entry.block = block;
            entry.tuple = tuple;
            pqueue.add(entry);
        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (fastPath)
            return fastPathNext();

        // if an entry was extracted previously, add it now
        if (lastEntry != null)
        {
            lastEntry.tuple = lastEntry.block.next();
            // add it back to queue, if there is data available
            if (lastEntry.tuple != null)
            {
                pqueue.add(lastEntry);
            }
            lastEntry = null;
        }

        if (pqueue.isEmpty())
            return null;

        PQEntry entry = pqueue.poll();
        if (entry == null)
            return null;

        lastEntry = entry;
        return entry.tuple;
    }

    /**
     * fast path implementation for <code> next() </code> method: iterate over a fixed
     * array and return next tuple.
     */
    private Tuple fastPathNext() throws RuntimeException,
            IOException,
            InterruptedException
    {
        if (lastEntry != null)
            lastEntry.next();

        if (entryList[0].tuple == null)
        {
            lastEntry = entryList[1];
        }
        else if (entryList[1].tuple == null)
        {
            lastEntry = entryList[0];
        }
        else
        {
            int cmp = pqcomparator.compare(entryList[0], entryList[1]);
            lastEntry = entryList[cmp <= 0 ? 0 : 1];
        }
        return lastEntry.tuple;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        String[] pivotKeys = JsonUtils.asArray(json, "pivotBy");

        for (String inputBlock : preConditions.keySet())
        {
            PostCondition inputCondition = preConditions.get(inputBlock);
            String[] sortKeys = inputCondition.getSortKeys();

            if (!CommonUtils.isPrefix(sortKeys, pivotKeys))
                throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                                "Block " + inputBlock);

        }

        // the post condition is same as the input condition
        return preConditions.values().iterator().next();
    }
}
