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

package com.linkedin.cubert.analyzer.physical;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.functions.builtin.FunctionFactory;
import com.linkedin.cubert.io.rubix.RubixConstants;
import com.linkedin.cubert.operator.BlockOperator;
import com.linkedin.cubert.operator.OperatorFactory;
import com.linkedin.cubert.operator.OperatorType;
import com.linkedin.cubert.operator.PivotBlockOperator;
import com.linkedin.cubert.operator.PostCondition;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;
import com.linkedin.cubert.operator.TupleOperator;
import com.linkedin.cubert.plan.physical.CubertCombiner;
import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.SchemaUtils;
import com.linkedin.cubert.utils.print;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import static com.linkedin.cubert.utils.JsonUtils.asArray;
import static com.linkedin.cubert.utils.JsonUtils.getText;

/**
 * Sematically analyzes the cubert physical plan.
 *
 * @author Maneesh Varshney
 *
 */
public class SemanticAnalyzer extends PhysicalPlanVisitor implements PlanRewriter
{
    public static final class Node
    {
        JsonNode json;
        PostCondition condition;
        Node child;
        ArrayList<Node> parents = new ArrayList<Node>();
        Node next;

        public Node(JsonNode json, PostCondition condition)
        {
            this.json = json;
            this.condition = condition;
        }

        public void setChild(Node child)
        {
            this.child = child;
        }

        public void addParent(Node parent)
        {
            this.parents.add(parent);
        }

        @Override
        public String toString()
        {
            String str = json.toString();
            if (child != null)
            {
                str += "\n  " + child.toString();
            }
            return str;
        }

        public JsonNode getOperatorJson()
        {
            return this.json;
        }

        public PostCondition getPostCondition()
        {
            return this.condition;
        }

        public Node getNext()
        {
            // TODO Auto-generated method stub
            return next;
        }
    }

    private Node linkedListHead;
    private final Map<String, PostCondition> datasetConditions =
            new HashMap<String, PostCondition>();

    private Node lastShuffleNode;
    private boolean hasErrors = false;
    private String[] blockIndexJoinKeys;
    private boolean revisit = false;

    private final Map<String, Node> operatorMap = new HashMap<String, Node>();
    private boolean debugMode = false;

    // List of final operator node from each mapper
    // The number of entries in this list is equal to # multimappers
    List<Node> mapOutputNodes = new ArrayList<Node>();

    public Node getNodeInformation()
    {
        return linkedListHead;
    }

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        this.revisit = revisit;
        this.debugMode = debugMode;
        new PhysicalPlanWalker(plan, this).walk();
        return hasErrors ? null : plan;
    }

    @Override
    public void enterProgram(JsonNode json)
    {
        if (json.has("input") && !json.get("input").isNull())
        {
            JsonNode programInput = json.get("input");
            Iterator<String> it = programInput.getFieldNames();
            while (it.hasNext())
            {
                String input = it.next();
                JsonNode inputJson = programInput.get(input);

                BlockSchema schema = new BlockSchema(inputJson.get("schema"));

                PostCondition condition = null;

                if (inputJson.has("partitionKeys") && inputJson.has("sortKeys"))
                    condition =
                            new PostCondition(schema,
                                              JsonUtils.asArray(inputJson.get("partitionKeys")),
                                              JsonUtils.asArray(inputJson.get("sortKeys")));
                else
                    condition = new PostCondition(schema, null, null);
                datasetConditions.put(input, condition);
            }
        }
    }

    @Override
    public void enterJob(JsonNode json)
    {
        print.f("Analyzing job [%s]...", getText(json, "name"));

        if (!json.has("jobType")
                || !getText(json, "jobType").equals("GENERATE_DICTIONARY"))
        {
            int numReducers = json.get("reducers").getIntValue();
            if (numReducers < 0)
            {
                error(null,
                      "Invalid negative number of reducers in job "
                              + getText(json, "name"));
            }
            else if (numReducers == 0)
            {
                if (json.has("shuffle") && !json.get("shuffle").isNull())
                    error(null, "Job [" + getText(json, "name")
                            + "] is configured with 0 reducers, but has shuffle operator");

                if (json.has("reduce") && !json.get("reduce").isNull())
                    error(null, "Job [" + getText(json, "name")
                            + "] is configured with 0 reducers, but has reduce operators");
            }
            else
            {
                if (!json.has("shuffle") || json.get("shuffle").isNull())
                    error(null,
                          "Job ["
                                  + getText(json, "name")
                                  + "] is configured with reducers, but does not define shuffle operator");
                if (!json.has("reduce") || json.get("reduce").isNull())
                    error(null,
                          "Job ["
                                  + getText(json, "name")
                                  + "] is configured with reducers, but does not define reduce operators");
            }
        }

        lastShuffleNode = null;
        linkedListHead = null;
        blockIndexJoinKeys = null;
        operatorMap.clear();

        mapOutputNodes.clear();
    }

    @Override
    public void visitInput(JsonNode json)
    {
        JsonNode pathJson = json.get("path");
        String path;
        if (pathJson.isArray())
            path = JsonUtils.encodePath(pathJson.get(0));
        else
            path = JsonUtils.encodePath(pathJson);

        // determine the postcondition of input
        PostCondition inputCondition = datasetConditions.get(path);
        if (inputCondition == null)
        {
            error(null, pathJson, "Cannot determine schema at path " + path);
        }

        // put the schema in json
        ((ObjectNode) json).put("schema", inputCondition.getSchema().toJson());

        Node node = new Node(json, inputCondition);

        // If the linkedListHead is not null, it implies there was a previous multimapper
        // In which case, store the Node in a list for now.
        // We will validate the postconditions in visitShuffle()
        if (linkedListHead != null)
        {
            mapOutputNodes.add(linkedListHead);
        }

        linkedListHead = node;
        operatorMap.clear();
        operatorMap.put(getText(json, "name"), node);
    }

    private void printConditions(String line,
                                 Map<String, PostCondition> preConditions,
                                 PostCondition postCondition)
    {
        if (!debugMode)
            return;
        print.f("---------------------------------------------");
        print.f("%s\n", line);
        for (String block : preConditions.keySet())
        {
            PostCondition preCondition = preConditions.get(block);
            print.f("Precondition for %s", block);
            print.f("\tSchema: %s", preCondition.getSchema());
            print.f("\tPartition Keys: %s",
                    Arrays.toString(preCondition.getPartitionKeys()));
            print.f("\tSort Keys:      %s", Arrays.toString(preCondition.getSortKeys()));
            if (preCondition.getPivotKeys() != null)
                print.f("\tPivot Keys:     %s",
                        Arrays.toString(preCondition.getPivotKeys()));
        }

        print.f("\nPost Condition");
        if (postCondition == null)
        {
            print.f("\tERROR");
        }
        else
        {
            print.f("\tSchema: %s", postCondition.getSchema());
            print.f("\tPartition Keys: %s",
                    Arrays.toString(postCondition.getPartitionKeys()));
            print.f("\tSort Keys:      %s", Arrays.toString(postCondition.getSortKeys()));
            if (postCondition.getPivotKeys() != null)
                print.f("\tPivot Keys:     %s",
                        Arrays.toString(postCondition.getPivotKeys()));
        }
    }

    @Override
    public void visitOperator(JsonNode json, boolean isMapper)
    {
        Map<String, PostCondition> preConditions = null;
        PostCondition postCondition = null;

        try
        {
            String[] inputNames = new String[] {};
            if (json.has("input"))
                inputNames = JsonUtils.asArray(json.get("input"));

            preConditions = getPreconditions(inputNames, json);
            if (preConditions == null)
                return;

            postCondition = getPostCondition(json, preConditions);

            if (json.get("line") != null)
                printConditions(json.has("line") ? getText(json, "line") : null,
                                preConditions,
                                postCondition);

            if (postCondition == null)
                return;

            ((ObjectNode) json).put("schema", postCondition.getSchema().toJson());

            // create a node for this operator
            Node node = new Node(json, postCondition);

            for (String inputName : inputNames)
            {
                Node parent = operatorMap.get(inputName);
                parent.setChild(node);
                node.addParent(parent);
            }

            operatorMap.put(getText(json, "output"), node);

            node.next = linkedListHead;
            linkedListHead = node;

            // special cases for individual operators
            OperatorType type = OperatorType.valueOf(getText(json, "operator"));
            switch (type)
            {
            case BLOCK_INDEX_JOIN:
                this.blockIndexJoinKeys = asArray(json, "partitionKeys");
                ((ObjectNode) json).put("schema", postCondition.getSchema().toJson());
                break;
            case TEE:
                this.datasetConditions.put(getText(json, "path"), postCondition);
                ((ObjectNode) json).put("schema", postCondition.getSchema().toJson());
                break;
            case LOAD_BLOCK:
                ((ObjectNode) json).put("schema", postCondition.getSchema().toJson());
                if (postCondition.getPartitionKeys() != null)
                    ((ObjectNode) json).put("partitionKeys",
                                            JsonUtils.createArrayNode(postCondition.getPartitionKeys()));
                if (postCondition.getSortKeys() != null)
                    ((ObjectNode) json).put("sortKeys",
                                            JsonUtils.createArrayNode(postCondition.getSortKeys()));

                break;
            default:

                break;
            }
        }
        catch (IllegalArgumentException e)
        {
            // error(json, "operator %s is not supported.", getText(json, "operator"));
            printConditions(json.get("line").getTextValue(), preConditions, postCondition);
            error(e, json, e.getMessage());
        }
        catch (PreconditionException e)
        {
            printConditions(json.get("line").getTextValue(), preConditions, postCondition);
            error(e, json, e.toString());
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            error(e, json, e.toString());
        }
    }

    private BlockSchema createReduceSideJoinSchema(Node leftNode, Node rightNode, JsonNode shuffleJson)
    {
        // Build a "union" schema for the join
        BlockSchema leftSchema = leftNode.condition.getSchema();
        BlockSchema rightSchema = rightNode.condition.getSchema();
        String[] joinKeys = asArray(leftNode.json, "joinKeys");

        // validate the the join keys have same schema in both mappers
        for (String joinKey: joinKeys)
        {
            ColumnType leftType = leftSchema.getColumnType(leftSchema.getIndex(joinKey));
            ColumnType rightType = rightSchema.getColumnType(rightSchema.getIndex(joinKey));

            if (!leftType.equals(rightType))
            {
                error(shuffleJson, "Datatype of join key " + joinKey + " is not same. Left: "
                            + leftType + " Right: " + rightType);
            }
        }

        // validate that column names are different in left and right tables
        Set<String> joinKeySet = new HashSet<String>();
        Set<String> leftColumnSet = new HashSet<String>();
        for (String str: joinKeys)
                joinKeySet.add(str);

        for (String str: leftSchema.getColumnNames())
            leftColumnSet.add(str);

        for (String str: rightSchema.getColumnNames())
                if (leftColumnSet.contains(str) && !joinKeySet.contains(str))
                    error(shuffleJson, "The names of columns (other than join keys) must be different. Found: " + str);

        // the new schema will have #leftColumns + #rightColumn - #joinKeys + 1 (tag)
        ColumnType[] joinTypes = new ColumnType[leftSchema.getNumColumns() + rightSchema.getNumColumns() - joinKeys.length + 1];
        Set<String> joinKeysSet = new HashSet<String>();

        // fill the schema with the join keys
        int idx = 0;
        for (String joinKey: joinKeys)
        {
            joinTypes[idx++] = leftSchema.getColumnType(leftSchema.getIndex(joinKey));
            joinKeysSet.add(joinKey);
        }


        // fill the remaining columns from the left schema
        for (int i = 0; i < leftSchema.getNumColumns(); i++)
        {
            String colName = leftSchema.getName(i);
            if (joinKeysSet.contains(colName))
                continue;
            joinTypes[idx++] = leftSchema.getColumnType(i);
        }

        // fill the remaining columns from the right schema
        for (int i = 0; i < rightSchema.getNumColumns(); i++)
        {
            String colName = rightSchema.getName(i);
            if (joinKeysSet.contains(colName))
                continue;
            joinTypes[idx++] = rightSchema.getColumnType(i);
        }


        // finally, fill the schema for tag (this is LAST columns)
        joinTypes[idx++] = new ColumnType("___tag", DataType.INT);

        BlockSchema joinSchema = new BlockSchema(joinTypes);

        return joinSchema;
    }

    @Override
    public void visitShuffle(JsonNode json)
    {
        // Add the current Mapper node in the list
        mapOutputNodes.add(linkedListHead);

        // if there are more than one mappers (multi-mapper situation)
        if (mapOutputNodes.size() > 1)
        {
            // special case: REDUCE-SIDE join
            if (json.has("join") && json.get("join").getBooleanValue())
            {
                // Build a "union" schema for the join
                Node leftNode = mapOutputNodes.get(0);
                Node rightNode = mapOutputNodes.get(1);

                BlockSchema joinSchema = createReduceSideJoinSchema(leftNode, rightNode, json);

                // Assign this schema to both mappers
                leftNode.condition = new PostCondition(joinSchema,
                                                       leftNode.condition.getPartitionKeys(),
                                                       leftNode.condition.getSortKeys());

                rightNode.condition = new PostCondition(joinSchema,
                                                        rightNode.condition.getPartitionKeys(),
                                                        rightNode.condition.getSortKeys());

                // set this joined schema in json for both operators
                ((ObjectNode) leftNode.json).put("schema", joinSchema.toJson());
                ((ObjectNode) rightNode.json).put("schema", joinSchema.toJson());
            }

            // validate that we are shuffling on the same name from each multi mapper
            // Also validate that the schema is identical (with one exception: ReduceSideJoin)
            String outputName = getText(mapOutputNodes.get(0).json, "output");
            BlockSchema firstSchema = mapOutputNodes.get(0).getPostCondition().getSchema();
            BlockSchema widerSchema = firstSchema;

            for (int i = 1; i < mapOutputNodes.size(); i++)
            {
                String name = getText(mapOutputNodes.get(i).json, "output");
                BlockSchema schema = mapOutputNodes.get(i).getPostCondition().getSchema();

                if (!name.equals(outputName))
                    error(json, "Multimappers must shuffle on same block names. Found: " + name + " and " + outputName);

                if (!firstSchema.equalsIgnoreNumeric(schema))
                    error(json, "Multimappers must have same output schema. Found:\n\t"
                                + firstSchema + " and\n\t" + schema);

                widerSchema = SchemaUtils.getWiderSchema(widerSchema, schema);
            }

            // update the condition for each multimapper with wider type
            for (int i = 0; i < mapOutputNodes.size(); i++)
            {

                PostCondition condition = mapOutputNodes.get(i).condition;
                mapOutputNodes.get(i).condition = new PostCondition(widerSchema,
                                                                    condition.getPartitionKeys(),
                                                                    condition.getSortKeys());
            }

        }


        String[] inputNames = asArray(json.get("name"));
        Map<String, PostCondition> preConditions = getPreconditions(inputNames, json);
        if (preConditions == null)
            return;

        PostCondition preCondition = preConditions.values().iterator().next();

        if (json.has("distinctShuffle") && json.get("distinctShuffle").getBooleanValue())
        {
            String[] columns = preCondition.getSchema().getColumnNames();
            JsonNode columnsJson = JsonUtils.createArrayNode(columns);
            ((ObjectNode) json).put("partitionKeys", columnsJson);
            ((ObjectNode) json).put("pivotKeys", columnsJson);
        }

        String[] partitionKeys = asArray(json, "partitionKeys");
        String[] pivotKeys = asArray(json, "pivotKeys");

        // make sure partition and pivot keys are in the schema
        if (partitionKeys != null)
        {
            checkShuffleKeyErrors(json, preCondition.getSchema(), partitionKeys);
        }

        if (pivotKeys != null)
        {
            checkShuffleKeyErrors(json, preCondition.getSchema(), pivotKeys);
        }

        // if there is a distinct operator in reduce, add remaining columns in pivot keys
        if (json.has("distinct") && json.get("distinct").getBooleanValue())
        {
            BlockSchema remaining =
                    preCondition.getSchema().getComplementSubset(pivotKeys);
            String[] completePivotKeys =
                    CommonUtils.concat(pivotKeys, remaining.getColumnNames());
            ((ObjectNode) json).put("pivotKeys",
                                    JsonUtils.createArrayNode(completePivotKeys));
            pivotKeys = asArray(json, "pivotKeys");
        }

        if (json.has("aggregates"))
        {
            try
            {
                CubertCombiner.checkPostCondition(preConditions, json);
            }
            catch (PreconditionException e)
            {
                error(json, e.getMessage());
            }
        }

        PostCondition postCondition =
                new PostCondition(preCondition.getSchema(), partitionKeys, pivotKeys);

        // put the schema in the json
        ((ObjectNode) json).put("schema", postCondition.getSchema().toJson());

        // create a node for this operator
        Node shuffleNode = new Node(json, postCondition);

        for (String inputName : inputNames)
        {
            Node parent = operatorMap.get(inputName);
            parent.setChild(shuffleNode);
            shuffleNode.addParent(parent);
        }

        // validate that all operators are used once

        for (Node node : mapOutputNodes)
        {
            node.setChild(shuffleNode);

            while (node != null)
            {
                if (node.child == null)
                    error(node.json, "The output of this command is not used");
                node = node.next;
            }
        }

        // start a new chain with shuffle as the input block
        operatorMap.clear();
        linkedListHead = shuffleNode;
        operatorMap.put(getText(json, "name"), shuffleNode);
    }

    private void checkShuffleKeyErrors(JsonNode json, BlockSchema schema, String[] keys)
    {
        final Map<String, Integer> schemaIndexMap = schema.getIndexMap();

        HashSet<String> cols = new HashSet<String>();
        for (String key : keys)
        {
            if (!schemaIndexMap.containsKey(key))
            {
                error(json, "Column " + key + " not found in schema:\n\t" + schema);
            }

            if (cols.contains(key))
            {
                error(json, "Duplicate key specified: " + key);
            }

            cols.add(key);
        }
    }

    @Override
    public void exitJob(JsonNode json)
    {
        JsonNode outputJson = json.get("output");
        boolean isRubixStorage =
                outputJson.has("type")
                        && getText(outputJson, "type").equalsIgnoreCase("RUBIX");

        // if the output type is cubert, we expect a metadata node in the json
        if (isRubixStorage)
        {
            // the metadata has the following fields
            JsonNode metadataNode =
                    JsonUtils.createObjectNode("schema",
                                               outputJson.get("schema"),
                                               "partitionKeys",
                                               outputJson.get("partitionKeys"),
                                               "sortKeys",
                                               outputJson.get("sortKeys"),
                                               "version",
                                               RubixConstants.RUBIX_FILE_VERSION_NUMBER,
                                               "BlockFormat",
                                               "DefaultRubixFormat",
                                               "BlockgenId",
                                               getText(outputJson, "blockgenId"));

            if (!json.has("metadata") || revisit)
                ((ObjectNode) json).put("metadata", metadataNode);
            else
                throw new RuntimeException(String.format("The metadata node is already added for the output of job %s, which is not expected.",
                                                         json.get("name").toString()));
        }
    }

    @Override
    public void visitOutput(JsonNode json)
    {
        String[] inputNames = asArray(json.get("name"));
        Map<String, PostCondition> preConditions = getPreconditions(inputNames, json);
        if (preConditions == null)
            return;

        PostCondition preCondition = preConditions.values().iterator().next();
        PostCondition postCondition = preCondition;

        boolean isRubixStorage =
                json.has("type") && getText(json, "type").equalsIgnoreCase("RUBIX");
        // Only rubix storage allows partition and sort keys
        if (!isRubixStorage)
        {
            postCondition = new PostCondition(postCondition.getSchema(), null, null);
        }

        // put the schema in the json
        ((ObjectNode) json).put("schema", postCondition.getSchema().toJson());

        if (postCondition.getPartitionKeys() != null)
            ((ObjectNode) json).put("partitionKeys",
                                    JsonUtils.createArrayNode(postCondition.getPartitionKeys()));
        if (postCondition.getSortKeys() != null)
            ((ObjectNode) json).put("sortKeys",
                                    JsonUtils.createArrayNode(postCondition.getSortKeys()));

        Node node = new Node(json, postCondition);

        for (String inputName : inputNames)
        {
            Node parent = operatorMap.get(inputName);
            parent.setChild(node);
            node.addParent(parent);
        }

        // validate that all operators are used once
        node = linkedListHead;
        while (node != null)
        {
            if (node.child == null)
                error(node.json, "The output of this command is not used");
            node = node.next;
        }

        datasetConditions.put(getText(json, "path"), postCondition);
    }

    private void error(JsonNode json, String format, Object... args)
    {
        error(null, json, format, args);
    }

    private void error(Exception e, JsonNode json, String format, Object... args)
    {
        if (debugMode && e != null)
            e.printStackTrace();

        hasErrors = true;
        System.err.println(String.format("ERROR: " + format, args));
        if (json != null)
        {
            System.err.print("At:\t");
            if (json.has("line"))
                System.err.println(json.get("line").getTextValue());
            else
                System.err.println(json.toString());

        }
        if (e != null)
            throw new PlanRewriteException(e);
        else
            throw new PlanRewriteException();
    }

    private Map<String, PostCondition> getPreconditions(String[] inputNames, JsonNode json)
    {
        Map<String, PostCondition> preConditions = new HashMap<String, PostCondition>();

        // verify that all inputs are present, and put them in the preConditions map
        for (String inputName : inputNames)
        {
            Node parent = operatorMap.get(inputName);
            if (parent == null)
            {
                error(json, "input block %s not found", inputName);
                continue;
            }

            if (parent.child != null)
            {
                // Exception 1: it is okay for the parent to have multiple child,
                // IF all other children are LOAD_BLOCK (which were using "MATCHING"
                // keyword to reference the parents)
                boolean isException = false;
                if (json.has("operator")
                        && getText(json, "operator").equals("LOAD_BLOCK"))
                    isException = true;

                if (parent.child.json.has("operator")
                        && getText(parent.child.json, "operator").equals("LOAD_BLOCK"))
                    isException = true;

                // Exception 2: it is okay for the parent to have multiple child,
                // IF the parent is an IN MEMORY PIVOT operator
                if (parent.json.has("operator")
                        && getText(parent.json, "operator").equals("PIVOT_BLOCK")
                        && parent.json.has("inMemory")
                        && parent.json.get("inMemory").getBooleanValue())
                    isException = true;

                // Exception 3 (for now): it is okay for parent to have multiple child,
                // if the other children are USER_DEFINED_BLOCK_OPERATOR
                if ((json.has("operator") && getText(json, "operator").equals("USER_DEFINED_BLOCK_OPERATOR"))
                        || (parent.child.json.has("operator") && getText(parent.child.json,
                                                                         "operator").equals("USER_DEFINED_BLOCK_OPERATOR")))
                    isException = true;

                if (!isException)
                {
                    error(json,
                          "parent operator [%s] has a child assigned already.\n\tChild: %s",
                          parent.json.get("line"),
                          parent.child.json.get("line"));
                    continue;
                }
            }

            preConditions.put(inputName, parent.condition);
        }

        // bail out, if we haven't accumulated all preconditions
        if (preConditions.size() != inputNames.length)
            return null;

        return preConditions;
    }

    private PostCondition getPostCondition(JsonNode json,
                                           Map<String, PostCondition> preConditions) throws PreconditionException
    {
        OperatorType type = OperatorType.valueOf(getText(json, "operator"));

        if (type.isTupleOperator())
        {
            // TupleOperator operatorObject = OperatorFactory.getTupleOperator(type);
            TupleOperator operatorObject =
                    (type == OperatorType.USER_DEFINED_TUPLE_OPERATOR)
                            ? (TupleOperator) FunctionFactory.createFunctionObject(getText(json,
                                                                                           "class"),
                                                                                   json.get("constructorArgs"))
                            : OperatorFactory.getTupleOperator(type);

            return operatorObject.getPostCondition(preConditions, json);
        }
        else
        {
            switch (type)
            {
            case CREATE_BLOCK:
            {
                String[] partitionKeys = JsonUtils.asArray(json, "partitionKeys");
                PostCondition condition = preConditions.values().iterator().next();
                BlockSchema schema = condition.getSchema();
                String[] sortKeys = condition.getSortKeys();
                if (json.has("pivotKeys"))
                    sortKeys = JsonUtils.asArray(json, "pivotKeys");

                if (!CommonUtils.isPrefix(partitionKeys, condition.getPartitionKeys())
                        && !CommonUtils.isPrefix(condition.getPartitionKeys(),
                                                 partitionKeys))
                    throw new PreconditionException(PreconditionExceptionType.INVALID_PARTITION_KEYS);

                boolean isIndexed = getText(json, "blockgenType").equals("BY_INDEX");

                if (isIndexed)
                {
                    partitionKeys = blockIndexJoinKeys;
                    // remove BLOCL_ID from schema
                    ColumnType[] ctypes = new ColumnType[schema.getNumColumns() - 1];
                    int idx = 0;
                    for (int i = 0; i < schema.getNumColumns(); i++)
                        if (!schema.getColumnType(i).getName().equals("BLOCK_ID"))
                            ctypes[idx++] = schema.getColumnType(i);

                    schema = new BlockSchema(ctypes);

                    // remove BLOCK_ID from sort keys
                    if (sortKeys.length == 0)
                        System.out.println("Empty sortKeys for opNode " + json.toString());
                    String[] tmp = new String[sortKeys.length - 1];

                    idx = 0;
                    for (String key : sortKeys)
                        if (!key.equals("BLOCK_ID"))
                            tmp[idx++] = key;
                    sortKeys = tmp;
                }

                return new PostCondition(schema, partitionKeys, sortKeys);
            }
            case LOAD_BLOCK:
            {
                if (json.get("path") == null)
                    System.out.println("null path name for " + json);
                String path = JsonUtils.encodePath(json.get("path"));
                PostCondition postCondition = this.datasetConditions.get(path);
                if (postCondition == null)
                    error(json, "Cannot determine schema of " + path);
                return postCondition;
            }
            case LOAD_CACHED_FILE:
            {
                String path = JsonUtils.encodePath(json.get("path"));
                try
                {
                    path = new URI(path).getPath();
                }
                catch (URISyntaxException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                PostCondition postCondition = this.datasetConditions.get(path);
                if (postCondition == null)
                    error(json, "Cannot determine schema of " + path);

                return new PostCondition(postCondition.getSchema(),
                                         new String[] {},
                                         new String[] {});
            }
            case PIVOT_BLOCK:
            {
                BlockOperator operator = new PivotBlockOperator();
                return operator.getPostCondition(preConditions, json);
            }
            case COLLATE_VECTOR_BLOCK:
            {
                PostCondition preCondition = preConditions.values().iterator().next();
                String metaRelationName =
                        new String(JsonUtils.getText(json, "metaRelationName"));

                BlockSchema outSchema = new BlockSchema(preCondition.getSchema());

                PostCondition preConditionMetaRelation =
                        preConditions.get(metaRelationName);
                String identifierColumns[] = JsonUtils.asArray(json, "identifierColumn");

                // This will allow a subsequent GROUP BY on (identifierColumns,
                // lookupColumns)
                if (!CommonUtils.isPrefix(preConditionMetaRelation.getSortKeys(),
                                          identifierColumns))
                {
                    System.out.println("PreconditionMetaRelation sortKeys "
                            + Arrays.toString(preConditionMetaRelation.getSortKeys()));
                    throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS);
                }

                String[] combineColumns = JsonUtils.asArray(json, "combineColumns");
                String[] lookupColumns = JsonUtils.asArray(json, "lookupColumn");
                if (!CommonUtils.isPrefix(preCondition.getSortKeys(),
                                          CommonUtils.concat(lookupColumns,
                                                             combineColumns)))
                {
                    System.out.println("PreconditionInputBlock sortKeys "
                            + Arrays.toString(preCondition.getSortKeys()));
                    throw new PreconditionException(PreconditionExceptionType.INVALID_SORT_KEYS);
                }

                int identifierColumnIndex =
                        preConditionMetaRelation.getSchema()
                                                .getIndex(getText(json,
                                                                  "identifierColumn"));
                ColumnType ct =
                        preConditionMetaRelation.getSchema()
                                                .getColumnType(identifierColumnIndex);

                // rename identifierColumn for output schema.
                identifierColumns[0] =
                        String.format("%s___%s",
                                      getText(json, "metaRelationName"),
                                      getText(json, "identifierColumn"));
                ct.setName(identifierColumns[0]);
                BlockSchema identifierColumnSchema =
                        new BlockSchema(new ColumnType[] { ct });
                /*
                 * String newColumn = String.format("STRING %s___%s",
                 * CommonUtils.stripQuotes(getText(json, "metaRelationName")),
                 * CommonUtils.stripQuotes(getText(json, "identifierColumn")));
                 * BlockSchema newColumnSchema = new BlockSchema(newColumn);
                 */

                // Only the metaRelation identifier column is renamed. Columns from the
                // "data/metrics" block are transmitted with their old names.
                outSchema = outSchema.append(identifierColumnSchema);

                return new PostCondition(outSchema,
                                         preCondition.getPartitionKeys(),
                                         (combineColumns),
                                         identifierColumns);
            }
            case USER_DEFINED_BLOCK_OPERATOR:
            {
                BlockOperator operator =
                        (BlockOperator) FunctionFactory.createFunctionObject(getText(json,
                                                                                     "class"),
                                                                             json.get("constructorArgs"));
                // OperatorFactory.getUserDefinedBlockOperator(getText(json, "class"),
                // json.get("constructorArgs"));
                return operator.getPostCondition(preConditions, json);

            }
            default:
                throw new IllegalArgumentException("Operator " + type
                        + " is not supported.");
            }
        }
    }
}
