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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockProperties;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.block.PivotedBlock;
import com.linkedin.cubert.memory.CompactHashTableBase;
import com.linkedin.cubert.memory.IntIterator;
import com.linkedin.cubert.memory.IntSet;
import com.linkedin.cubert.operator.cube.CountDistinctCubeAggregator;
import com.linkedin.cubert.operator.cube.CubeAggregator;
import com.linkedin.cubert.operator.cube.CubeDimensions;
import com.linkedin.cubert.operator.cube.DefaultCubeAggregator;
import com.linkedin.cubert.operator.cube.DefaultDupleCubeAggregator;
import com.linkedin.cubert.operator.cube.DimensionKey;
import com.linkedin.cubert.operator.cube.DupleCubeAggregator;
import com.linkedin.cubert.operator.cube.EasyCubeAggregator;
import com.linkedin.cubert.operator.cube.EasyCubeAggregatorBridge;
import com.linkedin.cubert.operator.cube.ValueAggregationType;
import com.linkedin.cubert.operator.cube.ValueAggregator;
import com.linkedin.cubert.operator.cube.ValueAggregatorFactory;
import com.linkedin.cubert.utils.ClassCache;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.Pair;

/**
 * 
 * @author Maneesh Varshney
 * 
 */
public class CubeOperator implements TupleOperator
{
    // Default operator configurations
    private static final int DEFAULT_HASH_TABLE_SIZE = 2000000;

    // inputs
    private boolean hasInnerDimensions = false;
    private Block inputBlock;

    // outputs
    private Tuple outputTuple;

    // aggregators
    private final List<CubeAggregator> aggregators = new ArrayList<CubeAggregator>();
    private final List<DupleCubeAggregator> dupleAggregators =
            new ArrayList<DupleCubeAggregator>();

    // hash table related
    private int hashTableSize = DEFAULT_HASH_TABLE_SIZE;
    private double flushThreshold = 0.95;
    private CompactHashTableBase hashTable;
    private Iterator<Pair<DimensionKey, Integer>> iterator;
    private final IntSet indexSet = new IntSet();

    // dimension key related
    private CubeDimensions dimensions;

    // runtime state management
    private boolean inputAvailable = true;

    @Override
    public void setInput(Map<String, Block> input, JsonNode json, BlockProperties props) throws IOException,
            InterruptedException
    {
        // get the input block and the input schema
        inputBlock = input.values().iterator().next();
        BlockSchema inputSchema = inputBlock.getProperties().getSchema();

        // get the output block schema
        BlockSchema outputSchema = props.getSchema();

        // read configurations from json
        String[] dimensionNames = JsonUtils.asArray(json, "dimensions");
        String[] innerDimensions = JsonUtils.asArray(json, "innerDimensions");
        if (json.has("hashTableSize") && !json.get("hashTableSize").isNull())
            hashTableSize = json.get("hashTableSize").getIntValue();

        hasInnerDimensions = (innerDimensions != null);

        if (hasInnerDimensions)
            inputBlock = new PivotedBlock(inputBlock, innerDimensions);

        // create aggregators
        List<CubeAggInfo> aggs = new ArrayList<CubeAggInfo>();
        List<DupleCubeAggInfo> dupleAggs = new ArrayList<DupleCubeAggInfo>();

        try
        {
            createAggregators(json, inputSchema, hasInnerDimensions, aggs, dupleAggs);
        }
        catch (PreconditionException e)
        {
            // this will not happen, since this method is also called in the
            // getPostCondition method, and any PreconditionException will be caught at
            // compile time
            throw new RuntimeException(e);
        }

        // initialize and allocate additive aggregates
        for (CubeAggInfo info : aggs)
        {
            info.getFirst().setup(inputBlock, outputSchema, info.getSecond());
            info.getFirst().allocate(hashTableSize);
            aggregators.add(info.getFirst());
        }

        // initialize and allocate partitioned additive aggregates
        for (DupleCubeAggInfo info : dupleAggs)
        {
            info.getFirst().setup(inputBlock, outputSchema, info.getSecond());
            info.getFirst().allocate(hashTableSize);
            dupleAggregators.add(info.getFirst());
        }
        // create the single copy of output tuple
        outputTuple = TupleFactory.getInstance().newTuple(outputSchema.getNumColumns());

        // initialize CubeDimensions
        dimensions =
                new CubeDimensions(inputSchema,
                                   outputSchema,
                                   dimensionNames,
                                   json.get("groupingSets"));

        // create compact hash table

        hashTable =
                new CompactHashTableBase(dimensions.getDimensionKeyLength(),
                                         hashTableSize);

        // set the flush threshold (if defined in conf)
        flushThreshold =
                PhaseContext.getConf().getFloat("cubert.cube.flush.threshold",
                                                (float) flushThreshold);
    }

    /**
     * Process input tuples for cubing without inner dimensions. Note that
     * DupleCubeAggregators cannot be used here (any attempt to use such aggregators would
     * have be caught at the compile time).
     * 
     * @return boolean flag to indicate if there is more input to be processed
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean processWithoutInnerDimensions() throws IOException,
            InterruptedException
    {
        if (!inputAvailable)
            return false;

        Tuple tuple;
        while ((tuple = inputBlock.next()) != null)
        {
            // only the additive aggregators can be handled
            for (CubeAggregator agg : aggregators)
                agg.processTuple(tuple);

            DimensionKey[] ancestors = dimensions.ancestors(tuple);

            for (DimensionKey ancestor : ancestors)
            {
                Pair<Integer, Boolean> idx = hashTable.lookupOrCreateIndex(ancestor);
                for (CubeAggregator agg : aggregators)
                    agg.aggregate(idx.getFirst());
            }

            if (hashTable.size() >= hashTableSize * flushThreshold)
                break;
        }

        if (tuple == null)
            inputAvailable = false;

        iterator = hashTable.getIterator();

        return true;
    }

    /**
     * Process input tuples for cubing WITH inner dimensions.
     * 
     * @return boolean flag to indicate if there is more input to be processed
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean processWithInnerDimensions() throws IOException,
            InterruptedException
    {
        if (!inputAvailable)
            return false;

        while (true)
        {
            Tuple tuple;

            indexSet.clear();

            while ((tuple = inputBlock.next()) != null)
            {
                for (CubeAggregator agg : aggregators)
                    agg.processTuple(tuple);
                for (CubeAggregator agg : dupleAggregators)
                    agg.processTuple(tuple);

                DimensionKey[] ancestors = dimensions.ancestors(tuple);

                for (DimensionKey ancestor : ancestors)
                {
                    Pair<Integer, Boolean> idx = hashTable.lookupOrCreateIndex(ancestor);

                    for (CubeAggregator agg : aggregators)
                        agg.aggregate(idx.getFirst());
                    for (DupleCubeAggregator agg : dupleAggregators)
                        agg.innerAggregate(idx.getFirst());

                    indexSet.add(idx.getFirst());
                }
            }

            IntIterator it = indexSet.iterator();
            while (it.hasNext())
            {
                int index = it.next();
                for (DupleCubeAggregator agg : dupleAggregators)
                    agg.aggregate(index);
            }

            if (!((PivotedBlock) inputBlock).advancePivot())
            {
                inputAvailable = false;
                break;
            }

            if (hashTable.size() >= hashTableSize * flushThreshold)
                break;
        }

        iterator = hashTable.getIterator();

        return true;
    }

    private boolean process() throws IOException,
            InterruptedException
    {
        hashTable.clear();

        for (CubeAggregator agg: this.aggregators)
            agg.clear();
        for (DupleCubeAggregator agg: this.dupleAggregators)
            agg.clear();

        if (hasInnerDimensions)
            return processWithInnerDimensions();
        else
            return processWithoutInnerDimensions();
    }

    @Override
    public Tuple next() throws IOException,
            InterruptedException
    {
        if (iterator == null)
        {
            if (!process())
                return null;
        }

        if (iterator.hasNext())
        {
            Pair<DimensionKey, Integer> pair = iterator.next();
            DimensionKey key = pair.getFirst();
            int index = pair.getSecond();

            dimensions.outputKey(key, outputTuple);
            for (CubeAggregator agg : aggregators)
                agg.outputTuple(outputTuple, index);
            for (CubeAggregator agg : dupleAggregators)
                agg.outputTuple(outputTuple, index);

            return outputTuple;
        }
        else
        {
            iterator = null;
            return next();
        }
    }

    @Override
    public PostCondition getPostCondition(Map<String, PostCondition> preConditions,
                                          JsonNode json) throws PreconditionException
    {

        PostCondition condition = preConditions.values().iterator().next();
        BlockSchema inputSchema = condition.getSchema();

        String[] dimensions = JsonUtils.asArray(JsonUtils.get(json, "dimensions"));
        String[] innerDimensions = JsonUtils.asArray(json, "innerDimensions");

        // validate that dimensions columns exist and are INT, LONG or STRING
        for (String dim : dimensions)
        {
            if (!inputSchema.hasIndex(dim))
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                dim);
            DataType type = inputSchema.getType(inputSchema.getIndex(dim));
            if (!type.isIntOrLong() && type != DataType.BOOLEAN && !type.equals(DataType.STRING))
                throw new PreconditionException(PreconditionExceptionType.INVALID_DIMENSION_TYPE,
                                                "Expecting type: BOOLEAN, INT, LONG or STRING. Found: "
                                                        + type);
        }

        // validate inner dimensions (if specified)
        if (innerDimensions != null)
        {
            // validate that innerDimensions exist
            for (String dim : innerDimensions)
            {
                if (!inputSchema.hasIndex(dim))
                    throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                    dim);
            }

            // validate that block is partitioned on inner dimensions
            String[] partitionKeys = condition.getPartitionKeys();
            if (partitionKeys == null || partitionKeys.length == 0
                    || !CommonUtils.isPrefix(innerDimensions, partitionKeys))
            {
                String msg =
                        String.format("Expected: %s. Found: %s",
                                      Arrays.toString(innerDimensions),
                                      Arrays.toString(partitionKeys));
                throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS,
                                                msg);
            }

            // validate that block is sorted on inner dimensions
            String[] sortKeys = condition.getSortKeys();
            if (sortKeys == null || sortKeys.length == 0
                    || !CommonUtils.isPrefix(sortKeys, innerDimensions))
            {
                String msg =
                        String.format("Expected: %s. Found: %s",
                                      Arrays.toString(innerDimensions),
                                      Arrays.toString(sortKeys));
                throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS,
                                                msg);
            }
        }

        // validate that dimensions in groupingSets are valid dimensions
        JsonNode gsJson = json.get("groupingSets");
        if (gsJson != null && !gsJson.isNull() && gsJson.size() > 0)
        {
            String[] gsInput = JsonUtils.asArray(gsJson);
            Set<String> dimensionSet = new HashSet<String>();
            for (int i = 0; i < dimensions.length; i++)
                dimensionSet.add(dimensions[i]);

            for (int i = 0; i < gsInput.length; i++)
            {
                String[] fields = gsInput[i].split(",");
                for (String field : fields)
                {
                    if (field.equals(""))
                        continue; // roll up everything TODO: check ROLLUP clause (?)

                    if (!dimensionSet.contains(field))
                    {
                        String msg =
                                String.format("Dimension \"%s\" in grouping set (%s) is not a valid dimension",
                                              field,
                                              gsInput[i]);
                        throw new PreconditionException(PreconditionExceptionType.INVALID_DIMENSION_TYPE,
                                                        msg);
                    }
                }
            }

        }

        // generate output schema
        BlockSchema outputSchema = createOutputSchema(inputSchema, json);

        // create post condition
        return new PostCondition(outputSchema, condition.getPartitionKeys(), null);

    }

    private static final class CubeAggInfo extends Pair<CubeAggregator, JsonNode>
    {
        private static final long serialVersionUID = 3313689844388231187L;

        public CubeAggInfo(CubeAggregator x, JsonNode y)
        {
            super(x, y);
        }
    }

    private static final class DupleCubeAggInfo extends
            Pair<DupleCubeAggregator, JsonNode>
    {
        private static final long serialVersionUID = -550007348499616264L;

        public DupleCubeAggInfo(DupleCubeAggregator x, JsonNode y)
        {
            super(x, y);
        }

    }

    private static BlockSchema createOutputSchema(BlockSchema inputSchema, JsonNode json) throws PreconditionException
    {
        List<CubeAggInfo> additiveAggs = new ArrayList<CubeAggInfo>();
        List<DupleCubeAggInfo> partitionedAdditiveAggs =
                new ArrayList<DupleCubeAggInfo>();
        final String[] innerDimensions = JsonUtils.asArray(json, "innerDimensions");

        createAggregators(json,
                          inputSchema,
                          innerDimensions != null,
                          additiveAggs,
                          partitionedAdditiveAggs);

        Map<JsonNode, BlockSchema> aggMap = new HashMap<JsonNode, BlockSchema>();
        for (CubeAggInfo info : additiveAggs)
        {
            JsonNode aggNode = info.getSecond();
            aggMap.put(aggNode, info.getFirst().outputSchema(inputSchema, aggNode));
        }

        for (DupleCubeAggInfo info : partitionedAdditiveAggs)
        {
            JsonNode aggNode = info.getSecond();
            aggMap.put(aggNode, info.getFirst().outputSchema(inputSchema, aggNode));
        }

        BlockSchema outputSchema =
                inputSchema.getSubset(JsonUtils.asArray(JsonUtils.get(json, "dimensions")));
        for (JsonNode aggregateJson : json.get("aggregates"))
            outputSchema = outputSchema.append(aggMap.get(aggregateJson));

        return outputSchema;
    }

    private static void createAggregators(JsonNode json,
                                          BlockSchema inputSchema,
                                          boolean hasInnerDimensions,
                                          List<CubeAggInfo> aggs,
                                          List<DupleCubeAggInfo> dupleAggs) throws PreconditionException
    {
        for (JsonNode aggregateJson : json.get("aggregates"))
        {
            JsonNode typeJson = aggregateJson.get("type");
            // validate that type is defined in json
            if (typeJson == null || typeJson.isNull())
                throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                                "<type> property not defined in Json: "
                                                        + typeJson.toString());

            // validate that type is a string or array
            if (!typeJson.isTextual() && !typeJson.isArray())
                throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                                "<type> property not text or array: "
                                                        + typeJson.toString());

            // if array, validate that type has one or two items
            if (typeJson.isArray() && !(typeJson.size() == 1 || typeJson.size() == 2))
                throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                                "<type> property as array can have either one or two items: "
                                                        + typeJson.toString());

            // validate that the input columns are present in input schema
            String[] inputColNames = null;
            DataType[] inputColTypes = null;

            if (aggregateJson.has("input") && !aggregateJson.get("input").isNull())
            {
                inputColNames = JsonUtils.asArray(aggregateJson, "input");
                inputColTypes = new DataType[inputColNames.length];
                int idx = 0;
                for (String colName : inputColNames)
                {
                    if (!inputSchema.hasIndex(colName))
                        throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                        colName);

                    inputColTypes[idx++] =
                            inputSchema.getType(inputSchema.getIndex(colName));
                }
            }

            // handle first the special case of array with two items
            if (typeJson.isArray() && typeJson.size() == 2)
            {
                String[] aggregators = JsonUtils.asArray(typeJson);

                ValueAggregationType outerType =
                        getCubeAggregationType(aggregators[0], true);
                ValueAggregationType innerType =
                        getCubeAggregationType(aggregators[1], true);

                // the "type" of inner aggregate is the type of input column
                ValueAggregator innerAggregator =
                        ValueAggregatorFactory.get(innerType,
                                                   inputColTypes[0],
                                                   inputColNames[0]);
                // the "type" of outer aggregate is the output type of inner aggregate
                ValueAggregator outerAggregator =
                        ValueAggregatorFactory.get(outerType,
                                                   innerAggregator.outputType(),
                                                   inputColNames[0]);

                DupleCubeAggregator cubeAggregator =
                        new DefaultDupleCubeAggregator(outerAggregator, innerAggregator);

                if (!hasInnerDimensions)
                    errorInnerDimensionsNotSpecified(java.util.Arrays.toString(aggregators));

                dupleAggs.add(new DupleCubeAggInfo(cubeAggregator, aggregateJson));
            }
            else
            {
                String type =
                        typeJson.isArray() ? typeJson.get(0).getTextValue()
                                : typeJson.getTextValue();

                ValueAggregationType aggType = getCubeAggregationType(type, false);

                // if this is builtin aggregator
                if (aggType != null)
                {

                    ValueAggregator aggregator =
                            ValueAggregatorFactory.get(aggType, inputColTypes == null
                                    ? null : inputColTypes[0], inputColNames == null
                                    ? null : inputColNames[0]);

                    CubeAggregator cubeggregator = new DefaultCubeAggregator(aggregator);

                    aggs.add(new CubeAggInfo(cubeggregator, aggregateJson));

                }
                else if (type.equals("COUNT_DISTINCT"))
                {
                    if (!hasInnerDimensions)
                        errorInnerDimensionsNotSpecified(type);

                    DupleCubeAggregator cubeAggregator =
                            new CountDistinctCubeAggregator(inputColNames[0]);

                    dupleAggs.add(new DupleCubeAggInfo(cubeAggregator, aggregateJson));
                }
                // this is udaf
                else
                {
                    Object object = null;

                    try
                    {
                        Class<?> cls = ClassCache.forName(type);
                        object =
                                instantiateObject(cls,
                                                  aggregateJson.get("constructorArgs"));

                    }
                    catch (ClassNotFoundException e)
                    {
                        throw new PreconditionException(PreconditionExceptionType.CLASS_NOT_FOUND,
                                                        type);
                    }
                    catch (Exception e)
                    {
                        throw new PreconditionException(PreconditionExceptionType.MISC_ERROR,
                                                        e.getClass().getSimpleName()
                                                                + " " + e.getMessage()
                                                                + " for class: " + type);
                    }

                    if (object instanceof DupleCubeAggregator)
                    {
                        DupleCubeAggregator cubeAggregator = (DupleCubeAggregator) object;

                        if (!hasInnerDimensions)
                            errorInnerDimensionsNotSpecified(type);
                        dupleAggs.add(new DupleCubeAggInfo(cubeAggregator, aggregateJson));

                    }
                    else if (object instanceof CubeAggregator)
                    {
                        CubeAggregator cubeAggregator = (CubeAggregator) object;

                        aggs.add(new CubeAggInfo(cubeAggregator, aggregateJson));
                    }

                    else if (object instanceof EasyCubeAggregator)
                    {
                        EasyCubeAggregatorBridge cubeAggregator =
                                new EasyCubeAggregatorBridge((EasyCubeAggregator) object);

                        if (!hasInnerDimensions)
                            errorInnerDimensionsNotSpecified(type);

                        dupleAggs.add(new DupleCubeAggInfo(cubeAggregator, aggregateJson));
                    }
                    else
                    {
                        String msg =
                                String.format("%s should implement one of these interfaces: AdditiveCubeAggregate, PartitionedAdditiveAggregate, EasyCubeAggregate",
                                              type);
                        throw new PreconditionException(PreconditionExceptionType.MISC_ERROR,
                                                        msg);
                    }
                }
            }
        }
    }

    private static void errorInnerDimensionsNotSpecified(String aggName) throws PreconditionException
    {
        String msg =
                String.format("INNER dimensions must be specified for the %s PartitionedAdditive aggregator",
                              aggName);
        throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG, msg);
    }

    private static Object instantiateObject(Class<?> cls, JsonNode constructorArgs) throws InstantiationException,
            IllegalAccessException,
            IllegalArgumentException,
            SecurityException,
            InvocationTargetException,
            NoSuchMethodException
    {
        if (constructorArgs == null || constructorArgs.isNull())
            return cls.newInstance();

        Object[] args = new Object[constructorArgs.size()];
        Class<?>[] argClasses = new Class[args.length];
        for (int i = 0; i < args.length; i++)
        {
            args[i] = JsonUtils.asObject(constructorArgs.get(i));
            argClasses[i] = args[i].getClass();
        }

        return cls.getConstructor(argClasses).newInstance(args);
    }

    private static ValueAggregationType getCubeAggregationType(String name,
                                                               boolean errorOnMissing) throws PreconditionException
    {
        try
        {
            return ValueAggregationType.valueOf(name.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            if (errorOnMissing)
            {
                String msg =
                        String.format("Aggregator [%s] not found. Valid aggregators: %s",
                                      name,
                                      java.util.Arrays.toString(ValueAggregationType.values()));
                throw new PreconditionException(PreconditionExceptionType.INVALID_CONFIG,
                                                msg);
            }
        }

        return null;
    }

}
