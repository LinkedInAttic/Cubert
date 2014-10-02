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

package com.linkedin.cubert.functions;

import java.io.IOException;

import org.apache.pig.data.Tuple;

import com.linkedin.cubert.block.ColumnType;

/**
 * Data structure for the nodes in the {@code FunctionTree}.
 * <p>
 * This node represents a function in the function tree. The function can be evaluated
 * using the {@code eval} method.
 * 
 * @author Maneesh Varshney
 * 
 */
public class FunctionTreeNode
{
    private final Function function;
    private final Tuple tuple;
    private final ColumnType type;

    public FunctionTreeNode(Function function, Tuple tuple, ColumnType type)
    {
        this.function = function;
        this.tuple = tuple;
        this.type = type;
    }

    /**
     * Get the function object for this node.
     * 
     * @return
     */
    public Function getFunction()
    {
        return function;
    }

    /**
     * Get the input tuple to this function node.
     * 
     * @return
     */
    public Tuple getTuple()
    {
        return tuple;
    }

    /**
     * Get the output type of this function node.
     * 
     * @return
     */
    public ColumnType getType()
    {
        return type;
    }

    /**
     * Evaluate this function node.
     * 
     * @return
     * @throws IOException
     */
    public Object eval() throws IOException
    {
        return function.eval(tuple);
    }

}
