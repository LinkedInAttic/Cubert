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

import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.linkedin.cubert.block.Block;
import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.builtin.FunctionFactory;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Parses, constructs and executes the function tree.
 * <p>
 * This class can maintain multiple function trees (added via the {@code addFunctionTree}
 * method). This method parses the JSON specification and builds a runtime representation
 * of the function tree. This method may throw a {@code PreconditionException} if any
 * inconsistency is found in the JSON.
 * <p>
 * The output type for each function tree can be retrieved using the {@code getType}
 * method.
 * <p>
 * The function tree can be evaluation using the {@code evalTree} method. Before calling
 * this method, the input tuple must be assigned to the tree using the {@code attachTuple}
 * method.
 * 
 * @author Maneesh Varshney
 * 
 */
public class FunctionTree
{
    private final Block block;
    private final BlockSchema inputSchema;
    private final InputProjection[] inputProjections;
    private final List<FunctionTreeNode> functionTrees =
            new ArrayList<FunctionTreeNode>();
    private DataType[] outputTypes;

    /**
     * Create a new object using the input Block. This constructor is called when creating
     * the FunctionTree at runtime.
     * 
     * @param block
     */
    public FunctionTree(Block block)
    {
        this.block = block;
        this.inputSchema = block.getProperties().getSchema();
        inputProjections = new InputProjection[inputSchema.getNumColumns()];
    }

    /**
     * Create a new object using the schema of the input block. This constructor is called
     * when creating the FunctionTree at compile time (since the 'Block' object is not
     * available at compile time).
     * 
     * @param schema
     */
    public FunctionTree(BlockSchema schema)
    {
        this.block = null;
        this.inputSchema = schema;
        inputProjections = new InputProjection[inputSchema.getNumColumns()];
    }

    /**
     * Adds a new function tree.
     * 
     * @param json
     *            JSON representation of the function tree.
     * @throws PreconditionException
     */
    public void addFunctionTree(JsonNode json) throws PreconditionException
    {
        FunctionTreeNode root = createTreeNode(json);
        functionTrees.add(root);

        outputTypes = new DataType[functionTrees.size()];
        for (int i = 0; i < outputTypes.length; i++)
            outputTypes[i] = functionTrees.get(i).getType().getType();
    }

    /**
     * Get the type of the root function for the specified function tree.
     * 
     * @param treeIndex
     * @return
     */
    public ColumnType getType(int treeIndex)
    {
        return functionTrees.get(treeIndex).getType();
    }

    /**
     * Attaches an input tuple to the tree before calling the {@code evalTree} method.
     * 
     * @param tuple
     * @throws ExecException
     */
    public void attachTuple(Tuple tuple) throws ExecException
    {
        for (int i = 0; i < inputProjections.length; i++)
        {
            if (inputProjections[i] != null)
                inputProjections[i].setValue(tuple.get(i));
        }
    }

    /**
     * Evaluates the specified function tree.
     * 
     * @param treeIndex
     *            the index of the tree to evaluate.
     * @return
     * @throws IOException
     */
    public Object evalTree(int treeIndex) throws IOException
    {
        FunctionTreeNode root = functionTrees.get(treeIndex);
        Object val = root.eval();

        if (val == null)
            return null;

        // if val is numeric, the actual value may be of narrower type.
        // upcast it to the proper wider type
        switch (outputTypes[treeIndex])
        {
        case INT:
            return ((Number) val).intValue();
        case LONG:
            return ((Number) val).longValue();
        case FLOAT:
            return ((Number) val).floatValue();
        case DOUBLE:
            return ((Number) val).doubleValue();
        default:
            return val;
        }
    }

    /**
     * Recursive method to parse and construct the function tree.
     * 
     * @param json
     * @return
     * @throws PreconditionException
     */
    private FunctionTreeNode createTreeNode(JsonNode json) throws PreconditionException
    {
        String function = getText(json, "function");
        ArrayNode args = (ArrayNode) json.get("arguments");

        if (function.equals("INPUT_PROJECTION"))
        {
            Object selector = JsonUtils.decodeConstant(args.get(0), null);
            int index = getSelectorIndex(inputSchema, selector);

            if (inputProjections[index] == null)
                inputProjections[index] = new InputProjection(index);

            Function func = inputProjections[index];

            return new FunctionTreeNode(func, null, func.outputSchema(inputSchema));
        }
        else if (function.equals("PROJECTION"))
        {
            Object selector = JsonUtils.decodeConstant(args.get(1), null);
            FunctionTreeNode parent = createTreeNode(args.get(0));

            int index = getSelectorIndex(parent.getType().getColumnSchema(), selector);
            Function func = new Projection(index);

            return new FunctionTreeNode(func,
                                        new LazyTuple(parent),
                                        func.outputSchema(parent.getType()
                                                                .getColumnSchema()));

        }
        else if (function.equals("MAP_PROJECTION"))
        {
            String key = args.get(1).getTextValue();
            FunctionTreeNode mapNode = createTreeNode(args.get(0));
            Function func = new MapProjection(key);

            return new FunctionTreeNode(func,
                                        new LazyTuple(mapNode),
                                        func.outputSchema(mapNode.getType()
                                                                 .getColumnSchema()));

        }
        else if (function.equals("CONSTANT"))
        {
            String type = (args.size() > 1) ? args.get(1).getTextValue() : null;
            Object constant = JsonUtils.decodeConstant(args.get(0), type);
            Function func = new Constant(constant);

            return new FunctionTreeNode(func, null, func.outputSchema(null));
        }
        else
        {
            Function func = FunctionFactory.get(function, json.get("constructorArgs"));
            if (block != null)
                func.setBlock(block);

            FunctionTreeNode[] children = new FunctionTreeNode[args.size()];
            ColumnType[] columnTypes = new ColumnType[args.size()];

            for (int i = 0; i < args.size(); i++)
            {
                children[i] = createTreeNode(args.get(i));
                columnTypes[i] = children[i].getType();
            }

            return new FunctionTreeNode(func,
                                        new LazyTuple(children),
                                        func.outputSchema(new BlockSchema(columnTypes)));
        }
    }

    public static int getSelectorIndex(BlockSchema schema, Object selector) throws PreconditionException
    {
        int index = -1;
        if (selector instanceof Integer)
        {
            index = (Integer) selector;
            if (index < 0 || index >= schema.getNumColumns())
            {
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                "Column at index " + index
                                                        + " is not present in input: "
                                                        + schema);
            }
        }
        else
        {
            String colName = (String) selector;
            if (!schema.hasIndex(colName))
                throw new PreconditionException(PreconditionExceptionType.COLUMN_NOT_PRESENT,
                                                "Column " + colName
                                                        + " is not present in input: "
                                                        + schema);

            index = schema.getIndex(colName);
        }

        return index;
    }
}
