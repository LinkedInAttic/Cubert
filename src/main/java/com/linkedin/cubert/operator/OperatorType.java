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

/**
 * The supported operators by Cubert framework.
 *
 * The boolean field specified if the operator is a TupleOperator or a BlockOperator.
 *
 * @author Maneesh Varshney
 *
 */
public enum OperatorType
{
    NO_OP(true),
    DICT_ENCODE(true),
    DICT_DECODE(true),
    DUPLICATE(true),
    LOAD_BLOCK(false),
    LOAD_CACHED_FILE(false),
    CREATE_BLOCK(false),
    PIVOT_BLOCK(false),

    GROUP_BY(true),

    JOIN(true),
    HASHJOIN(true),
    BLOCK_INDEX_JOIN(true),

    CUBE(true),
    GROUPING_SET(true),

    COMBINE(true),
    SORT(true),
    FILTER(true),
    GENERATE(true),
    LIMIT(true),
    DISTINCT(true),
    TEE(true),
    GATHER(true),
    TOP_N(true),
    RANK(true),

    VALIDATE(true),
    FLATTEN(true),
    COLLATE_VECTOR_BLOCK(false),
    USER_DEFINED_TUPLE_OPERATOR(true),
    USER_DEFINED_BLOCK_OPERATOR(false);

    private boolean isTupleOperator;

    private OperatorType(boolean isTupleOperator)
    {
        this.isTupleOperator = isTupleOperator;
    }

    public boolean isTupleOperator()
    {
        return isTupleOperator;
    }

}
