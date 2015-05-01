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
import com.linkedin.cubert.block.DataType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;


public class FlattenOperator2 implements TupleOperator
{
    public static enum FlattenType
    {
        TUPLE(0), BAG(1), BAG_TUPLE(2);

        private final int value;

        private FlattenType(int value)
        {
            this.value = value;
        }

        public static FlattenType fromString(String s) throws IllegalArgumentException
        {
            for (FlattenType ftype : FlattenType.values())
            {
                if (s.equalsIgnoreCase(ftype.toString()))
                    return ftype;
            }
            throw new IllegalArgumentException("Unmatched FlattenType");
        }
    }

    // TODO: use this datastructure as value in TreeMap<Integer, FlattenInfo> inputColumnToFlattenInfo to
    //      replace columnIndexArray, flattenPositions, inputColumnIndexToOutputTypes
    class FlattenInfo {
        private FlattenType type;
        private List<ColumnType> outputColumnTypes;
    }

    // input column positions that are being flattened
    private final Set<Integer> columnIndexArray = new TreeSet<Integer>();

    private final Map<Integer, FlattenType> flattenPositions = new HashMap<Integer, FlattenType>();

    private Tuple outTuple = null;

    // TODO: refactor odometer into a class by itself
    private final ArrayList<Iterator<Tuple>> odometerIterators = new ArrayList<Iterator<Tuple>>();

    private Block inputBlock;

    private Tuple inTuple;

    private BlockSchema outSchema;
    private final Map<Integer, List<ColumnType>> inputColumnIndexToOutputTypes =
            new HashMap<Integer, List<ColumnType>>();

    private final Set<String> flattenColumnNameSet = new HashSet<String>();

//    private int resultFieldCount;

//    private BlockSchema inSchema;
    private DataBag nullBag;

    @Override
    public void setInput(Map<String, Block> input,
                         JsonNode operatorJson,
                         BlockProperties props) throws IOException,
            InterruptedException
    {
        inputBlock = input.values().iterator().next();

        init(operatorJson, inputBlock.getProperties().getSchema());

        nullBag = BagFactory.getInstance().newDefaultBag();

        nullBag.add(TupleFactory.getInstance().newTuple(0));
    }

    private void init(JsonNode operatorJson, BlockSchema inputSchema)
    {
        JsonNode exprListNode = operatorJson.get("genExpressions");

        Iterator<JsonNode> it = exprListNode.getElements();
        while (it.hasNext())
        {
            JsonNode expr = it.next();
            String colName = expr.get("col").getTextValue();

            int columnId = inputSchema.getIndex(colName);

            flattenColumnNameSet.add(colName);
            columnIndexArray.add(columnId);

            if (expr.has("flatten"))
            {
                final String flattenTypeStr = expr.get("flatten").getTextValue();

                flattenPositions.put(inputSchema.getIndex(colName),
                                     FlattenType.fromString(flattenTypeStr));

                odometerIterators.add(null);

                // Extract output column definition from 'expr'
                List<ColumnType> outputColumTypeList = new ArrayList<ColumnType>();
                inputColumnIndexToOutputTypes.put(columnId, outputColumTypeList);

                JsonNode outputNode = expr.get("output");
                for (JsonNode j : outputNode)
                {
                    String outColName = j.get("col").getTextValue();
                    DataType outType = DataType.valueOf(j.get("type").getTextValue());
                    outputColumTypeList.add(new ColumnType(outColName, outType));
                }

            }
        }

        this.outSchema = generateOutSchema(inputSchema);
    }

    private BlockSchema generateOutSchema(BlockSchema inputSchema)
    {
        List<ColumnType> outputColumnTypes = new ArrayList<ColumnType>();

        for (ColumnType ct : inputSchema.getColumnTypes())
        {
            String colName = ct.getName();
            int colIndex = inputSchema.getIndex(colName);

            if (! flattenColumnNameSet.contains(colName))
            {
                outputColumnTypes.add(ct);
            }
            else
            {
                BlockSchema inputNestedColumnSchema = ct.getColumnSchema();

                ColumnType[] ctypes = inputNestedColumnSchema.getColumnTypes();
                if (ctypes.length == 1 && ctypes[0].getType() == DataType.TUPLE)
                    inputNestedColumnSchema = ctypes[0].getColumnSchema();

                List<ColumnType> flattedOutputColumnTypes = inputColumnIndexToOutputTypes.get(colIndex);

                if (flattedOutputColumnTypes != null && !flattedOutputColumnTypes.isEmpty())
                {
                    // output schema published in json.
                    // TODO: assert output schema in json matches nested input schema for the column

                    if (inputNestedColumnSchema == null || inputNestedColumnSchema.getColumnTypes() == null)
                        throw new RuntimeException("Invalid schema for columnn:  "
                            + colName + " column schema: " + inputNestedColumnSchema);

                    if (flattedOutputColumnTypes.size() != inputNestedColumnSchema.getColumnTypes().length)
                        throw new RuntimeException("Output column specification does not match number of input fields for "
                                + colName);
                }
                else
                {
                    // output schema not published in json. Extract from nested input column schema

                    if (inputNestedColumnSchema == null)
                    {
                        throw new RuntimeException("Schema is unknown for column: " + colName);
                    }
                    else
                    {
                        List<ColumnType> subColTypes = Arrays.asList(inputNestedColumnSchema.getColumnTypes());

                        flattedOutputColumnTypes = new ArrayList<ColumnType>();
                        flattedOutputColumnTypes.addAll(subColTypes);
                    }

                    inputColumnIndexToOutputTypes.put(colIndex, flattedOutputColumnTypes);
                }

                outputColumnTypes.addAll(flattedOutputColumnTypes);
            }
        }

        return new BlockSchema(outputColumnTypes.toArray(new ColumnType[0]));
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        // First attempt to flatten bag, if type == BAG || BAG_TUPLE
        Tuple tuple1 = flattenBagNext();

        if (tuple1 == null) {
            // Past the last tuple.
            return null;
        }

        // Now attempt to flatten tuple, if type == TUPLE || BAG_TUPLE
        return tupleFlatten(tuple1);
    }

    public Tuple tupleFlatten(Tuple inTuple) throws IOException
    {
        final int count = outSchema.getNumColumns();

        // TODO: can we reuse tuple?
        Tuple tuple = TupleFactory.getInstance().newTuple(count);

        // for each position, retrieve either column value or flattened value
        int outidx = 0;
        for (int colId = 0; colId < inTuple.size(); colId++)
        {
            Object obj = inTuple.get(colId);

            if (!isFlattenTuple(flattenPositions.get(colId)))
            {
                // Not a "flatten" column. Preserve object.
                tuple.set(outidx++, obj);
                continue;
            }

            // Object is a tuple. Flatten it.
            Tuple preFlattening = (Tuple) obj;

            int nColumnFields = this.inputColumnIndexToOutputTypes.get(colId).size();

            if (obj == null || preFlattening.size() == 0)
            {
                for (int i = 0; i < nColumnFields; i++)
                    tuple.set(outidx++, null);
            }
            else
            {
                for (int i = 0; i < nColumnFields; i++)
                    tuple.set(outidx++, preFlattening.get(i));
            }
        }

        if (outidx < count)
            throw new RuntimeException(String.format("FlattenTuple: found fewer fields than expected=%d, found=%d",
                                                     count,
                                                     outidx));
        return tuple;
    }

    public Tuple flattenBagNext() throws IOException,
            InterruptedException
    {
        Tuple t; // Rui. to avoid currentTupleNext being called twice.

        if (outTuple == null || (t = currentTupleNext()) == null)
        {
            Tuple inTuple = inputBlock.next();

            if (inTuple == null)
                return null; // Rui.

            initCurrentTuple(inTuple);
            return this.outTuple;
        }

        return t;
    }

    private boolean isFlattenBag(FlattenType ftype)
    {
        return (ftype == FlattenType.BAG || ftype == FlattenType.BAG_TUPLE);
    }

    private boolean isFlattenTuple(FlattenType ftype)
    {
        return (ftype == FlattenType.TUPLE || ftype == FlattenType.BAG_TUPLE);
    }

    private void initCurrentTuple(Tuple inTuple) throws ExecException
    {
        this.inTuple = inTuple;
        this.odometerIterators.clear();

        for (int columnId : columnIndexArray)
        {
            FlattenType ftype = flattenPositions.get(columnId);
            if (ftype == null || !isFlattenBag(ftype))
            {
                continue;
            }

            DataBag dbag = (DataBag) (inTuple.get(columnId));// Rui. change outTuple to
                                                             // inTuple

            if (dbag == null || dbag.size() == 0)
            {
                // Null and empty bags are treated as if they contained a single null tuple.
                odometerIterators.add(nullBag.iterator());
            } else
            {
                odometerIterators.add(dbag.iterator());
            }
        }

        seedOutTuple();// Rui. move it here.
    }

    private void seedOutTuple() throws ExecException
    {
        // TODO: Can we re-use this tuple?
        this.outTuple = TupleFactory.getInstance().newTuple(inTuple.size());

        int oid = 0;
        for (int idx = 0; idx < inTuple.size(); idx++)
        {
            Object o = this.inTuple.get(idx);
            FlattenType ftype = flattenPositions.get(idx);

            if (ftype == null || !isFlattenBag(ftype))
            {
                // Non-flatten element. Add it as is.
                this.outTuple.set(idx, o);
                continue;
            }

            // Extract first element from oid-th iterator. Iterator guaranteed to have
            // at least one element: first element in bag or null.
            Iterator<Tuple> tupleIt = odometerIterators.get(oid);
            this.outTuple.set(idx, tupleIt.next());

            oid++;
        }
    }

    private Tuple currentTupleNext() throws ExecException
    {
        int nfields = outTuple.size();

        // TODO: Can we re-use this tuple?
        Tuple resultTuple = TupleFactory.getInstance().newTuple(nfields);

        // copy current outTuple to resultTuple;
        for (int idx = 0; idx < nfields; idx++)
        {
            Object o = outTuple.get(idx);
            resultTuple.set(idx, o);
        }

        // Figure out which of the iterators can be advanced and which needs reset.
        int oid = 0;
        for (int columnId : columnIndexArray)
        {

            FlattenType ftype = flattenPositions.get(columnId);
            if (ftype == null || !isFlattenBag(ftype))
                continue;

            Iterator<Tuple> tupleIt = odometerIterators.get(oid);

            if (tupleIt.hasNext())
            {
                Object o1 = tupleIt.next();
                resultTuple.set(columnId, o1);
                outTuple = resultTuple;
                return resultTuple;
            }
            else
            {
                // reset the iterator
                Object o = this.inTuple.get(columnId);
                DataBag dbag = (DataBag) o;
                tupleIt = dbag.iterator();
                odometerIterators.set(oid, tupleIt);
            }

            oid++;
        }

        return null;
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {
        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();

        this.init(json, inputSchema);

        return new PostCondition(outSchema,
                                 condition.getPartitionKeys(),
                                 condition.getSortKeys());
    }
}
