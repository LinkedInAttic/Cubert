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

import com.linkedin.cubert.block.CreateBlockOperator;

public class OperatorFactory
{

    public static BlockOperator getUserDefinedBlockOperator(String classname)
    {
        try
        {
            Class<?> cls = Class.forName(classname);
            Object object = cls.newInstance();

            return (BlockOperator) object;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static TupleOperator getUserDefinedTupleOperator(String classname)
    {
        try
        {
            Class<?> cls = Class.forName(classname);
            Object object = cls.newInstance();

            return (TupleOperator) object;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

    }

    public static BlockOperator getBlockOperator(OperatorType type)
    {
        switch (type)
        {
        case CREATE_BLOCK:
            return new CreateBlockOperator();
        case LOAD_BLOCK:
            return new LoadBlockOperator();
        case LOAD_CACHED_FILE:
            return new LoadBlockFromCacheOperator();
        case PIVOT_BLOCK:
            return new PivotBlockOperator();
        case COLLATE_VECTOR_BLOCK:
            return new CollateVectorBlockOperator();
        default:
            throw new IllegalArgumentException("Operator [" + type
                    + "] is not a BlockOperator");
        }
    }

    public static TupleOperator getTupleOperator(OperatorType type)
    {
        switch (type)
        {
        case NO_OP:
            return new NullOperator();
        case DUPLICATE:
            return new DuplicateOperator();
        case DICT_DECODE:
            return new DictionaryDecodeOperator();
        case DICT_ENCODE:
            return new DictionaryEncodeOperator();
        case FILTER:
            return new FilterOperator();
        case BLOCK_INDEX_JOIN:
            return new BlockIndexJoinOperator();
        case GROUPING_SET:
            break;
        case GROUP_BY:
            return new GroupByOperator();
        case JOIN:
            return new MergeJoinOperator();
        case GENERATE:
            return new GenerateOperator();
        case HASHJOIN:
            return new HashJoinOperator();
        case SORT:
            return new SortOperator();
        case LIMIT:
            return new LimitOperator();
        case LOAD_BLOCK:
            break;
        case LOAD_CACHED_FILE:
            break;
        case CUBE:
            return new CubeOperator();
        case COMBINE:
            return new CombineOperator();
        case DISTINCT:
            return new DistinctOperator();
        case TEE:
            return new TeeOperator();
        case GATHER:
            return new GatherOperator();
        case TOP_N:
            return new TopNOperator();
        case FLATTEN:
            return new FlattenBagOperator();
        case VALIDATE:
            return new ValidateOperator();
        default:
            break;
        }

        throw new IllegalArgumentException("Operator [" + type
                + "] is not a TupleOperator");

    }
}
