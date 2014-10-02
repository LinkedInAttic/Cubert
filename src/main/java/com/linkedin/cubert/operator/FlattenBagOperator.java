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

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;

public class FlattenBagOperator implements TupleOperator
{
    public static enum FlattenType
    {
        TUPLE(0), BAG(1), BAG_TUPLE(2);

        private final int value;

        private FlattenType(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
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

    // input column positions that are being flattened
    private final Set<Integer> columnIndexArray = new TreeSet<Integer>();
    // whether flatten is needed at a particular output

    private final HashMap<Integer, FlattenType> flattenPositions =
            new HashMap<Integer, FlattenType>();

    private Tuple outTuple = null;
    private final ArrayList<Iterator<Tuple>> odometerIterators =
            new ArrayList<Iterator<Tuple>>();

    private Block inputBlock;

    private Tuple inTuple;

    private BlockSchema outSchema;
    private final Map<String, List<ColumnType>> outputColumnTypeMap =
            new HashMap<String, List<ColumnType>>();

    private final Set<String> flattenColumnNameSet = new HashSet<String>();

    private int resultFieldCount;

    private BlockSchema inSchema;
    private DataBag nullBag;

    @Override
    public void setInput(Map<String, Block> input,
                         JsonNode operatorJson,
                         BlockProperties props) throws IOException,
            InterruptedException
    {

        for (Block ib : input.values())
        {
            inputBlock = ib;
            break;
        }

        BlockSchema inputSchema = inputBlock.getProperties().getSchema();

        init(operatorJson, inputSchema);
        createNullBag();
    }

    private void createNullBag()
    {
        nullBag = BagFactory.getInstance().newDefaultBag();
        Tuple emptyTuple = TupleFactory.getInstance().newTuple(0);
        nullBag.add((Tuple) emptyTuple);

    }

    private void init(JsonNode operatorJson, BlockSchema inputSchema)
    {
        JsonNode exprListNode = operatorJson.get("genExpressions");
        Iterator<JsonNode> it = exprListNode.getElements();
        while (it.hasNext())
        {
            JsonNode e = it.next();
            String colName = e.get("col").getTextValue();
            int columnId = inputSchema.getIndex(colName);
            flattenColumnNameSet.add(colName);
            columnIndexArray.add(columnId);

            JsonNode flattenNode = e.get("flatten");
            if (flattenNode != null)
            {
                String ftypestr = flattenNode.getTextValue();

                flattenPositions.put(inputSchema.getIndex(colName),
                                     FlattenType.fromString(ftypestr));
                // System.out.println("Flatten column  =" + colName + " position = " +
                // inputSchema.getIndex(colName) + " type= " + ftypestr);

                odometerIterators.add(null);

                // out put column definitions:
                List<ColumnType> outputColumTypeList = new ArrayList<ColumnType>();
                outputColumnTypeMap.put(colName, outputColumTypeList);
                JsonNode outputNode = e.get("output");
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
        inSchema = inputSchema;
        List<ColumnType> colTypeList = new ArrayList<ColumnType>();

        resultFieldCount = 0;
        for (ColumnType ct : inSchema.getColumnTypes())
        {
            String colName = ct.getName();

            if (flattenColumnNameSet.contains(colName))
            {
                BlockSchema columnSchema = ct.getColumnSchema();
                List<ColumnType> outputColTypeDef = outputColumnTypeMap.get(colName);
                if (outputColTypeDef != null && !outputColTypeDef.isEmpty())
                {
                    if (outputColTypeDef.size() != columnSchema.getColumnTypes().length)
                        throw new RuntimeException("Output column specification does not match number of input fields for "
                                + colName);
                    colTypeList.addAll(outputColTypeDef);
                }
                else
                {

                    outputColTypeDef = new ArrayList<ColumnType>();
                    if (columnSchema == null)
                    {
                        throw new RuntimeException("Schema is unkown for col: " + colName);
                    }
                    else
                    {

                        List<ColumnType> subColTypes =
                                Arrays.asList(columnSchema.getColumnTypes());
                        colTypeList.addAll(subColTypes);
                        outputColTypeDef.addAll(subColTypes);
                    }
                    outputColumnTypeMap.put(colName, outputColTypeDef);

                }
                resultFieldCount += outputColTypeDef.size();
            }
            else
            {

                colTypeList.add(ct);
                resultFieldCount++;

            }
        }

        return new BlockSchema(colTypeList.toArray(new ColumnType[0]));
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        Tuple tuple1 = flattenBagNext();
        if (tuple1 == null)
            return null; // Rui
        Tuple result = tupleFlatten(tuple1);
        // System.out.println("flatten result: " + result.toString());
        return result;
    }

    public Tuple tupleFlatten(Tuple inTuple) throws IOException,
            InterruptedException
    {

        // field count
        // System.out.println("tupleFlatten input: " + inTuple.toString());
        int nfields = resultFieldCount;

        Tuple outTuple = TupleFactory.getInstance().newTuple(nfields);
        // for each position, retrieve either column value or flattened value
        int outidx = 0;
        for (int colId = 0; colId < inTuple.size(); colId++)
        {
            Object o = inTuple.get(colId);
            if (!isFlattenTuple(flattenPositions.get(colId)))
            {
                outTuple.set(outidx++, o);
                continue;
            }

            String inputColumnName = inSchema.getColumnNames()[colId];
            int nColumnFields = this.outputColumnTypeMap.get(inputColumnName).size();
            Tuple t = (Tuple) o;
            if (o == null || t.size() == 0)
            {
                for (int i = 0; i < nColumnFields; i++)
                    outTuple.set(outidx++, null);
            }
            else
            {

                for (int i = 0; i < nColumnFields; i++)
                    outTuple.set(outidx++, t.get(i));
            }
        }

        if (outidx < resultFieldCount)
            throw new RuntimeException(String.format("FlattenTuple: found fewer fields than expected=%d, found=%d",
                                                     resultFieldCount,
                                                     outidx));
        return outTuple;
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
        return (ftype == FlattenType.BAG || ftype == FlattenType.BAG_TUPLE) ? true
                : false;
    }

    private boolean isFlattenTuple(FlattenType ftype)
    {
        return (ftype == FlattenType.TUPLE || ftype == FlattenType.BAG_TUPLE ? true
                : false);
    }

    private void initCurrentTuple(Tuple inTuple) throws ExecException
    {
        // TODO Auto-generated method stub

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
            Iterator<Tuple> tupleIt;

            // Deal with null and empty bags as if they contained a single null tuple.
            if (dbag == null || dbag.size() == 0)
                tupleIt = nullBag.iterator();
            else
                tupleIt = dbag.iterator();
            odometerIterators.add(tupleIt);
        }

        seedOutTuple();// Rui. move it here.

    }

    private void seedOutTuple() throws ExecException
    {
        // TODO Auto-generated method stub
        this.outTuple = TupleFactory.getInstance().newTuple(inTuple.size());
        int oid = 0;
        for (int idx = 0; idx < inTuple.size(); idx++)
        {
            Object o = this.inTuple.get(idx);
            FlattenType ftype = flattenPositions.get(idx);
            if (ftype == null || !isFlattenBag(ftype))
            {

                this.outTuple.set(idx, o);
                continue;
            }

            // DataBag dbag = (DataBag) o;
            Iterator<Tuple> tupleIt = odometerIterators.get(oid++);

            Object o1 = tupleIt.next();
            this.outTuple.set(idx, o1);

        }

    }

    private Tuple currentTupleNext() throws ExecException
    {
        int nfields = outTuple.size();
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
