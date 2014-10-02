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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.cube.DimensionKey;
import com.linkedin.cubert.utils.JsonUtils;

public class ExtractMedianOperator implements TupleOperator
{

    private Block dataBlock;
    private BlockSchema schema;
    private long blockId;
    private Tuple partitionKey;

    private Map<DimensionKey, List<Long>> positionMap =
            new HashMap<DimensionKey, List<Long>>();
    private Tuple outputTuple;

    private Map<String, Integer> argsMap = new HashMap<String, Integer>();

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        String[] inputBlockNames = JsonUtils.asArray(json, "input");
        assert (inputBlockNames.length == 2);

        // get data block
        String dataName = inputBlockNames[0];
        dataBlock = input.get(dataName);
        blockId = dataBlock.getProperties().getBlockId();
        schema = getOutputSchema(dataBlock.getProperties().getSchema());

        String positionName = inputBlockNames[1];
        Block positionBlock = input.get(positionName);

        // jsonutils as map for args?
        setArgs(json.get("args"));

        // set up right relation into a hashmap (ancestor -> position of median)
        Tuple t;
        DimensionKey key;

        int pkIndex = argsMap.get("pkIndex");
        int positionIndex = argsMap.get("positionIndex");
        while ((t = positionBlock.next()) != null)
        {
            // two fields are pkIndex and positionIndex so make dimension key size - 2
            key = new DimensionKey(new int[t.size() - 2]);
            for (int i = 0; i < t.size(); i++)
            {
                if (i != pkIndex && i != positionIndex)
                {
                    Integer dimValue = (Integer) t.get(i);
                    key.set(i, dimValue);
                }
            }

            List<Long> positions;
            if (positionMap.containsKey(key))
            {
                positions = positionMap.get(key);
                positions.add((Long) t.get(t.size() - 2));
            }
            else
            {
                positions = new ArrayList<Long>();
                positions.add((Long) t.get(t.size() - 2));
            }
            positionMap.put(key, positions);

            /*
             * int location = (Integer) t.get(0); int month = (Integer) t.get(1); key =
             * new DimensionKey(new int[] {location, month});
             * 
             * List<Long> positions; if (positionMap.containsKey(key)) { positions =
             * positionMap.get(key); positions.add((Long)t.get(2)); } else { positions =
             * new ArrayList<Long>(); positions.add((Long) t.get(2)); }
             * positionMap.put(key, positions);
             */
        }

        // instantiate outputTuple
        outputTuple = TupleFactory.getInstance().newTuple(schema.getColumnNames().length);
    }

    private void setArgs(JsonNode args)
    {
        String argString =
                args.toString().replace("{", "").replace("}", "").replaceAll("\"", "");
        String[] elements = argString.split(",");

        for (String element : elements)
        {
            String[] keyValueComponents = element.split(":");

            String key = keyValueComponents[0];
            Integer val = Integer.parseInt(keyValueComponents[1]);

            argsMap.put(key, val);
        }
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        // while positionMap is not empty, we first check if any of the <K,V> pairs has a
        // V that is zero, in which
        // case we can emit that tuple and remove the <K,V> pair from the hashmap.
        // if none are zero, we want to keep getting more tuples from the data block.
        // each of those tuples will get enumerated into it's ancestors and update the
        // hashmap.
        // after updating the hashmap, once again check if any <K,V> pairs are zero, and
        // emit the tuple
        // and remove the <K,V> pairs from the hashmap if so.

        // can store an outputTuple, and if any hashmap values are zero, set it's
        // ancestors to the fields
        // of the outputTuple and then remove from the hashmap. if all hashmap values are
        // zero, maybe
        // set outputTuple to null. or keep updating outputTuple's value field no matter
        // what.

        while (positionMap.size() > 0)
        {
            // check if any <K,V> pairs has a V that is zero.
            Tuple previousRetVal = updateHashMapAndRemoveTuple();
            if (previousRetVal != null)
            {
                return previousRetVal;
            }

            // if none are ready to be outputted, must enumerate more tuples in the
            // datablock.
            Tuple t;
            int valueIndex = argsMap.get("valueIndex");
            int outputTupleSize = outputTuple.size();
            while ((t = dataBlock.next()) != null)
            {

                // subtract one because that is the valueIndex, the rest are dimensions
                DimensionKey dataKey = new DimensionKey(new int[t.size() - 1]);

                for (int i = 0; i < t.size(); i++)
                {
                    if (i != valueIndex)
                    {
                        Integer dimValue = (Integer) t.get(i);
                        dataKey.set(i, dimValue);
                    }
                }

                DimensionKey[] ancestors = ancestors(dataKey);

                outputTuple.set(outputTupleSize - 1, t.get(valueIndex));

                for (DimensionKey ancestor : ancestors)
                {
                    // for each enumeration, update hashmap
                    if (positionMap.containsKey(ancestor))
                    {
                        List<Long> positions = positionMap.get(ancestor);
                        for (int i = 0; i < positions.size(); i++)
                        {
                            Long position = positions.get(i);
                            position -= 1;
                            positions.set(i, position);
                        }
                        positionMap.put(ancestor, positions);
                    }
                }

                // check if hashmap has any -1 and output if so
                Tuple retval = updateHashMapAndRemoveTuple();
                if (retval != null)
                {
                    return retval;
                }
                else
                {
                    continue;
                }

                /*
                 * // enumerate t, update outputTuple, update hashmap, check if hashmap
                 * has any -1 and output if so DimensionKey dataKey = new DimensionKey(new
                 * int[] { (Integer) t.get(0) , (Integer) t.get(1)}); DimensionKey[]
                 * ancestors = ancestors(dataKey); outputTuple.set(2, (Long) t.get(2));
                 * for (DimensionKey ancestor : ancestors) { // for each enumeration,
                 * update hashmap if (positionMap.containsKey(ancestor)) { List<Long>
                 * positions = positionMap.get(ancestor); for (int i = 0; i <
                 * positions.size(); i++) { Long position = positions.get(i); position -=
                 * 1; positions.set(i, position); } positionMap.put(ancestor, positions);
                 * } }
                 * 
                 * // check if hashmap has any -1 and output if so Tuple retval =
                 * updateHashMapAndRemoveTuple(); if (retval != null) { return retval; }
                 * else { continue; }
                 */
            }
        }

        return null;
    }

    private Tuple updateHashMapAndRemoveTuple() throws ExecException
    {
        Iterator<Entry<DimensionKey, List<Long>>> iterator =
                positionMap.entrySet().iterator();
        while (iterator.hasNext())
        {
            Entry<DimensionKey, List<Long>> pair = iterator.next();

            List<Long> positions = pair.getValue();
            Iterator<Long> positionIterator = positions.iterator();

            while (positionIterator.hasNext())
            {
                Long position = positionIterator.next();
                if (position == -1)
                {
                    int[] dimensions = pair.getKey().getArray();
                    for (int i = 0; i < dimensions.length; i++)
                    {
                        outputTuple.set(i, dimensions[i]);
                    }
                    positionIterator.remove();

                    if (positions.size() == 0)
                    {
                        iterator.remove();
                    }
                    return outputTuple;
                }
            }
            /*
             * if (pair.getValue() == -1) // since we are zero indexed { int[] dimensions
             * = pair.getKey().getArray(); outputTuple.set(0, dimensions[0]);
             * outputTuple.set(1, dimensions[1]); iterator.remove(); return outputTuple;
             * // value should have already been set! }
             */
        }
        return null;
    }

    private DimensionKey[] ancestors(DimensionKey dataKey)
    {
        int[] dataKeyArray = dataKey.getArray();
        int dimensionKeySize = dataKeyArray.length;
        int numAncestors = (int) Math.pow(2, dimensionKeySize);
        DimensionKey[] retval = new DimensionKey[numAncestors];

        for (int i = 0; i < numAncestors; i++)
        {
            DimensionKey ancestor = new DimensionKey(new int[dimensionKeySize]);
            for (int j = 0; j < dimensionKeySize; j++)
            {
                // if the right most bit is 1, select that element
                if (((i >> j) & 1) == 1)
                {
                    ancestor.set(j, dataKeyArray[j]);
                }
                else
                {
                    ancestor.set(j, -1);
                }
            }
            retval[i] = ancestor;
        }

        return retval;

        /*
         * int location = dataKey.getArray()[0]; int month = dataKey.getArray()[1];
         * DimensionKey[] retval = new DimensionKey[4]; retval[0] = dataKey; retval[1] =
         * new DimensionKey(new int[] {location, -1}); retval[2] = new DimensionKey(new
         * int[] {-1, month}); retval[3] = new DimensionKey(new int[] {-1, -1});
         * 
         * return retval;
         */
    }

    // assumption: valueIndex is the last index of the inputSchema
    private BlockSchema getOutputSchema(BlockSchema inputSchema)
    {
        int sizeOfOutputSchema = inputSchema.getColumnNames().length;
        ColumnType[] columns = new ColumnType[sizeOfOutputSchema];

        for (int i = 0; i < inputSchema.getColumnNames().length - 1; i++)
        {
            ColumnType column = inputSchema.getColumnType(i);
            columns[i] = column;
        }
        ColumnType lastColumn;

        if (inputSchema.getColumnType(inputSchema.getNumColumns() - 1).getType() == DataType.LONG)
        {
            lastColumn = new ColumnType("value", DataType.LONG);
        }
        else
        {
            lastColumn = new ColumnType("value", DataType.DOUBLE);
        }
        columns[sizeOfOutputSchema - 1] = lastColumn;
        return new BlockSchema(columns);

        /*
         * int sizeOfOutputSchema = inputSchema.getColumnNames().length + 1; ColumnType[]
         * columns = new ColumnType[sizeOfOutputSchema];
         * 
         * for (int i = 0; i < inputSchema.getColumnNames().length; i++) { ColumnType
         * column = inputSchema.getColumnType(i); if (column.getType() != DataType.BAG) {
         * columns[i] = column; } else { ColumnType tuple =
         * column.getColumnSchema().getColumnType(0);
         * 
         * ColumnType[] innerColumns = tuple.getColumnSchema().getColumnTypes(); assert
         * (innerColumns.length == 2);
         * 
         * columns[i] = innerColumns[0]; columns[i+1] = innerColumns[1]; } } return new
         * BlockSchema(columns);
         */
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        Iterator<PostCondition> iter = preConditions.values().iterator();
        iter.next();
        PostCondition condition = iter.next();
        BlockSchema inputSchema = condition.getSchema();

        return new PostCondition(getOutputSchema(inputSchema),
                                 condition.getPartitionKeys(),
                                 condition.getSortKeys());

        /*
         * System.out.println("PRECONDITIONS: " + preConditions);
         * System.out.println("JSON: " + json);
         * 
         * PostCondition condition = preConditions.values().iterator().next();
         * 
         * ColumnType[] columns = new ColumnType[3];
         * 
         * ColumnType first = new ColumnType("location", DataType.INT); ColumnType second
         * = new ColumnType("month", DataType.INT); ColumnType third = new
         * ColumnType("value", DataType.LONG);
         * 
         * columns[0] = first; columns[1] = second; columns[2] = third;
         * 
         * BlockSchema s = new BlockSchema(columns);
         * 
         * return new PostCondition(s, condition.getPartitionKeys(),
         * condition.getSortKeys());
         */
    }

}
