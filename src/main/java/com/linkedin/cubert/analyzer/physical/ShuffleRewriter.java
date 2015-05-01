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

import static com.linkedin.cubert.utils.CommonUtils.generateVariableName;
import static com.linkedin.cubert.utils.JsonUtils.asArray;
import static com.linkedin.cubert.utils.JsonUtils.cloneNode;
import static com.linkedin.cubert.utils.JsonUtils.createArrayNode;
import static com.linkedin.cubert.utils.JsonUtils.createObjectNode;
import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.io.IOException;
import java.util.Set;

import com.linkedin.cubert.utils.ClassCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.utils.CommonUtils;
import com.linkedin.cubert.utils.JsonUtils;

/**
 * Rewrites BLOCKGEN or CUBE-COUNT-DISTINCT as shuffle operators.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ShuffleRewriter implements PlanRewriter
{
    private final ObjectMapper mapper = new ObjectMapper();
    private Set<String> namesUsed;

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit)
    {
        this.namesUsed = namesUsed;

        ObjectNode newPlan = (ObjectNode) cloneNode(plan);

        ArrayNode jobs = mapper.createArrayNode();

        for (JsonNode job : plan.path("jobs"))
        {
            jobs.add(rewriteJob(job));
        }

        newPlan.remove("jobs");
        newPlan.put("jobs", jobs);

        return newPlan;
    }

    private JsonNode rewriteJob(JsonNode job)
    {
        // no rewriting, if the job does not have shuffle
        if (!job.has("shuffle") || job.get("shuffle").isNull())
            return job;

        JsonNode shuffle = job.get("shuffle");
        // no rewriting if the shuffle is not a macro command
        if (!shuffle.has("type"))
            return job;

        String type = getText(shuffle, "type");

        if (type.equals("SHUFFLE"))
            return job;
        else if (type.equals("BLOCKGEN"))
            return rewriteBlockgen(job);
        else if (type.equals("CUBE"))
            return rewriteCube(job);
        else if (type.equals("CREATE-DICTIONARY"))
            return rewriteDictionary(job);
        else if (type.equals("DISTINCT"))
            return rewriteDistinct(job);
        else if (type.equals("JOIN"))
            return rewriteJoin(job);
        else
        {
            try
            {
                // get the class
                Class<?> cls = ClassCache.forName(type);

                // make sure this class implements PlanRewriter interface
                if (! PlanRewriter.class.isAssignableFrom(cls))
                    throw new PlanRewriteException("Shuffle rewriter class " + type
                                                   + " does not implement the PlanRewrite interface");

                // as the user defined shuffle rewriter to rewrite this job
                return cls.asSubclass(PlanRewriter.class)
                          .newInstance()
                          .rewrite(job, namesUsed, false, false);
            }
            catch (ClassNotFoundException e)
            {
                throw new PlanRewriteException("Shuffle rewriter class " + type + " not found");
            }
            catch (InstantiationException e)
            {
                throw new PlanRewriteException("Shuffle rewriter class " + type + " cannot be instantiated");
            }
            catch (IllegalAccessException e)
            {
                throw new PlanRewriteException("Shuffle rewriter class " + type + " has illegal access");
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

//        throw new PlanRewriteException("Cannot rewrite shuffle type " + type);
    }

    private JsonNode rewriteDictionary(JsonNode job)
    {
        ObjectNode newJob = (ObjectNode) cloneNode(job);

        ObjectNode shuffle = (ObjectNode) newJob.get("shuffle");
        if (shuffle == null)
            throw new RuntimeException("Shuffle description missing. Cannot rewrite.");

        newJob.put("reducers", 1);

        // Determine if this is a refresh job or a fresh dictionary creation by looking at
        // STORE location
        String storePath = job.get("output").get("path").getTextValue();
        String dictionaryPath = storePath + "/part-r-00000.avro";
        boolean refresh = false;

        try
        {
            FileSystem fs = FileSystem.get(new JobConf());
            refresh = fs.exists(new Path(dictionaryPath));
        }
        catch (IOException e)
        {
            // we will not refresh
        }

        // Rewrite map
        JsonNode relationName = shuffle.get("name");

        ObjectNode mapSideOperator =
                JsonUtils.createObjectNode("operator",
                                           "USER_DEFINED_TUPLE_OPERATOR",
                                           "class",
                                           "com.linkedin.cubert.operator.DictionaryRefreshMapSideOperator",
                                           "input",
                                           JsonUtils.createArrayNode(relationName),
                                           "output",
                                           relationName,
                                           "columns",
                                           shuffle.get("columns"));

        copyLine(shuffle, mapSideOperator, "[MAP] ");

        for (JsonNode map : newJob.path("map"))
        {
            if (!map.has("operators") || map.get("operators").isNull())
                ((ObjectNode) map).put("operators", JsonUtils.createArrayNode());
            ArrayNode operators = (ArrayNode) map.get("operators");
            operators.add(mapSideOperator);
        }

        // Rewrite shuffle
        shuffle.put("name", relationName);
        shuffle.put("type", "SHUFFLE");
        shuffle.put("partitionKeys",
                    JsonUtils.createArrayNode(CommonUtils.array("colname", "colvalue")));
        shuffle.put("pivotKeys",
                    JsonUtils.createArrayNode(CommonUtils.array("colname", "colvalue")));
        if (shuffle.has("columns"))
            shuffle.remove("columns");
        if (shuffle.has("dictionaryPath"))
            shuffle.remove("dictionaryPath");
        shuffle.remove("input");

        // Rewrite reduce
        if (!newJob.has("reduce") || newJob.get("reduce").isNull())
            newJob.put("reduce", JsonUtils.createArrayNode());
        ArrayNode reduceJob = (ArrayNode) newJob.get("reduce");

        ObjectNode reduceSideOperator = (ObjectNode) cloneNode(mapSideOperator);
        reduceSideOperator.put("class",
                               "com.linkedin.cubert.operator.DictionaryRefreshReduceSideOperator");
        copyLine(shuffle, reduceSideOperator, "[REDUCE] ");
        reduceJob.insert(0, reduceSideOperator);

        // Rewrite cached files
        if (refresh)
        {
            String newStorePath = storePath + "/tmp";
            String newDictionaryPath = newStorePath + "/part-r-00000.avro";

            // put the existing dictionary file in dist cache
            if (!newJob.has("cachedFiles") || newJob.get("cachedFiles").isNull())
                newJob.put("cachedFiles", JsonUtils.createArrayNode());
            ArrayNode cachedFiles = (ArrayNode) newJob.get("cachedFiles");
            cachedFiles.add(dictionaryPath + "#dictionary");

            // tell the operators to use cached dictionary
            mapSideOperator.put("dictionary", dictionaryPath + "#dictionary");
            reduceSideOperator.put("dictionary", dictionaryPath + "#dictionary");

            // the output path is changed to <original path>/tmp
            ((ObjectNode) newJob.get("output")).put("path", newStorePath);

            // put onCompletion for this job to move the new dictionary to the parent
            // folder
            ArrayNode onCompletion = mapper.createArrayNode();
            // onCompletion.add(JsonUtils.createObjectNode("type",
            // "rm",
            // "paths",
            // JsonUtils.createArrayNode(storePath
            // + "/dictionary.avro")));
            onCompletion.add(JsonUtils.createObjectNode("type",
                                                        "mv",
                                                        "paths",
                                                        JsonUtils.createArrayNode(new String[] {
                                                                newDictionaryPath,
                                                                dictionaryPath })));
            onCompletion.add(JsonUtils.createObjectNode("type",
                                                        "rm",
                                                        "paths",
                                                        JsonUtils.createArrayNode(newStorePath)));
            newJob.put("onCompletion", onCompletion);
        }

        return newJob;
    }

    private JsonNode rewriteBlockgen(JsonNode job)
    {

        String blockgenType = job.get("shuffle").get("blockgenType").getTextValue();
        if (blockgenType.equals("BY_INDEX"))
            return rewriteBlockgenByIndex(job);
        // else: following is the rewrite of BLOCKGEN

        ObjectNode newJob = (ObjectNode) cloneNode(job);
        ObjectNode shuffle = (ObjectNode) newJob.get("shuffle");
        JsonNode blockgenTypeNode = shuffle.get("blockgenType");
        JsonNode blockgenValueNode = shuffle.get("blockgenValue");

        if (!shuffle.has("pivotKeys"))
            throw new PlanRewriteException("PivotKeys are not defined in SHUFFLE");

        // add CREATE_BLOCK operator in the reducer
        if (!newJob.has("reduce") || newJob.get("reduce").isNull())
            newJob.put("reduce", mapper.createArrayNode());
        ArrayNode reduce = (ArrayNode) newJob.get("reduce");
        ObjectNode createBlockOperator =
                createObjectNode("operator",
                                 "CREATE_BLOCK",
                                 "input",
                                 shuffle.get("name"),
                                 "output",
                                 shuffle.get("name"),
                                 "blockgenType",
                                 blockgenTypeNode,
                                 "blockgenValue",
                                 blockgenValueNode,
                                 "partitionKeys",
                                 shuffle.get("partitionKeys"));
        copyLine(shuffle, createBlockOperator, "[REDUCE] ");
        reduce.insert(0, createBlockOperator);

        // add DISTINCT operator, if requested
        boolean isDistinct =
                shuffle.has("distinct") && shuffle.get("distinct").getBooleanValue();

        if (isDistinct)
        {
            ObjectNode distinct =
                    createObjectNode("operator",
                                     "DISTINCT",
                                     "input",
                                     shuffle.get("name"),
                                     "output",
                                     shuffle.get("name"));
            copyLine(shuffle, distinct, "[REDUCE DISTINCT]");
            reduce.insert(0, distinct);
        }

        // the sort keys for the SHUFFLE are set to the actual
        // blockgen PARTITION KEYS. These sort keys are configured into the JsonNode for
        // the CREATE_BLOCK operator

        // clean up shuffle
        shuffle.remove("blockgenType");
        shuffle.remove("blockgenValue");
        shuffle.put("type", "SHUFFLE");
        shuffle.put("distinct", isDistinct);

        if (!CommonUtils.isPrefix(asArray(shuffle, "pivotKeys"),
                                  asArray(shuffle, "partitionKeys")))
        {
            createBlockOperator.put("pivotKeys", shuffle.get("pivotKeys"));
            shuffle.put("pivotKeys", shuffle.get("partitionKeys"));
        }

        return newJob;
    }

    private JsonNode rewriteBlockgenByIndex(JsonNode job)
    {
        ObjectNode newJob = (ObjectNode) cloneNode(job);
        ObjectNode shuffle = (ObjectNode) newJob.get("shuffle");

        String path = getText(shuffle, "relation");

        // add a cache index
        String indexName = generateVariableName(namesUsed);
        if (!newJob.has("cacheIndex") || newJob.get("cacheIndex").isNull())
            newJob.put("cacheIndex", mapper.createArrayNode());
        ArrayNode cacheIndex = (ArrayNode) newJob.get("cacheIndex");
        cacheIndex.add(createObjectNode("name", indexName, "path", path));

        // create BLOCK-INDEX-JOIN operator
        ObjectNode blockIndexJoin =
                createObjectNode("operator",
                                 "BLOCK_INDEX_JOIN",
                                 "input",
                                 shuffle.get("name"),
                                 "output",
                                 shuffle.get("name"),
                                 "partitionKeys",
                                 shuffle.get("partitionKeys"),
                                 "index",
                                 indexName);
        copyLine(shuffle, blockIndexJoin, "[MAP] ");
        // add it as the last operator for all mapper
        for (JsonNode map : newJob.path("map"))
        {
            if (!map.has("operators") || map.get("operators").isNull())
                ((ObjectNode) map).put("operators", mapper.createArrayNode());
            ArrayNode operators = (ArrayNode) map.get("operators");
            // we need unique references for all blockIndexJoin
            operators.add(JsonUtils.cloneNode(blockIndexJoin));
        }

        // create CREATE-BLOCK operator
        ObjectNode createBlock =
                createObjectNode("operator",
                                 "CREATE_BLOCK",
                                 "input",
                                 shuffle.get("name"),
                                 "output",
                                 shuffle.get("name"),
                                 "blockgenType",
                                 "BY_INDEX",
                                 "index",
                                 indexName,
                                 "partitionKeys",
                                 createArrayNode("BLOCK_ID"),
                                 "indexPath",
                                 path);

        copyLine(shuffle, createBlock, "[REDUCE] ");
        // add it as first operator in reduce
        if (!newJob.has("reduce") || newJob.get("reduce").isNull())
            newJob.put("reduce", mapper.createArrayNode());
        ArrayNode reduce = (ArrayNode) newJob.get("reduce");
        reduce.insert(0, createBlock);

        // add DISTINCT operator, if requested
        boolean isDistinct =
                shuffle.has("distinct") && shuffle.get("distinct").getBooleanValue();

        if (isDistinct)
        {
            ObjectNode distinct =
                    createObjectNode("operator",
                                     "DISTINCT",
                                     "input",
                                     shuffle.get("name"),
                                     "output",
                                     shuffle.get("name"));
            copyLine(shuffle, distinct, "[REDUCE DISTINCT] ");
            reduce.insert(0, distinct);
        }

        // blockgen by index uses a different partitioner
        shuffle.put("partitionerClass",
                    "com.linkedin.cubert.plan.physical.ByIndexPartitioner");

        // clean up shuffle
        shuffle.put("type", "SHUFFLE");
        shuffle.put("partitionKeys", createArrayNode("BLOCK_ID"));
        shuffle.put("distinct", isDistinct);
        shuffle.put("index", indexName);
        shuffle.remove("blockgenType");
        shuffle.remove("relation");

        ArrayNode pivotKeys = mapper.createArrayNode();
        pivotKeys.add("BLOCK_ID");
        if (shuffle.has("pivotKeys"))
        {
            for (JsonNode key : shuffle.path("pivotKeys"))
                pivotKeys.add(key);
        }
        shuffle.put("pivotKeys", pivotKeys);

        return newJob;
    }

    private JsonNode rewriteCube(JsonNode job)
    {
        ObjectNode newJob = (ObjectNode) cloneNode(job);
        ObjectNode shuffle = (ObjectNode) newJob.get("shuffle");
        String name = getText(shuffle, "name");
        JsonNode aggregates = shuffle.get("aggregates");

        // create the OLAP_CUBE_COUNT_DISTINCT operator
        ObjectNode cube =
                createObjectNode("operator",
                                 "CUBE",
                                 "input",
                                 name,
                                 "output",
                                 name,
                                 "dimensions",
                                 shuffle.get("dimensions"),
                                 "aggregates",
                                 cloneNode(aggregates));

        if (shuffle.has("groupingSets"))
            cube.put("groupingSets", shuffle.get("groupingSets"));
        if (shuffle.has("innerDimensions"))
            cube.put("innerDimensions", shuffle.get("innerDimensions"));
        if (shuffle.has("hashTableSize"))
            cube.put("hashTableSize", shuffle.get("hashTableSize"));
        copyLine(shuffle, cube, "[MAP] ");

        // add it as the last operator for all mapper
        for (JsonNode map : newJob.path("map"))
        {
            if (!map.has("operators") || map.get("operators").isNull())
                ((ObjectNode) map).put("operators", mapper.createArrayNode());
            ArrayNode operators = (ArrayNode) map.get("operators");
            operators.add(cube);
        }

        rewriteGroupByAggregateForCube(aggregates);

        // create the GROUP BY operator at the reducer
        ObjectNode groupBy =
                createObjectNode("operator",
                                 "GROUP_BY",
                                 "input",
                                 name,
                                 "output",
                                 name,
                                 "groupBy",
                                 shuffle.get("dimensions"),
                                 "aggregates",
                                 aggregates);
        copyLine(shuffle, groupBy, "[REDUCE] ");
        // add it as first operator in reduce
        if (!newJob.has("reduce") || newJob.get("reduce").isNull())
            newJob.put("reduce", mapper.createArrayNode());
        ArrayNode reduce = (ArrayNode) newJob.get("reduce");
        reduce.insert(0, groupBy);

        // clean up shuffle
        shuffle.put("type", "SHUFFLE");
        shuffle.put("aggregates", aggregates);
        shuffle.put("partitionKeys", shuffle.get("dimensions"));
        shuffle.put("pivotKeys", shuffle.get("dimensions"));
        shuffle.remove("dimensions");
        shuffle.remove("groupingSets");
        shuffle.remove("innerDimensions");

        return newJob;
    }

    private void rewriteGroupByAggregateForCube(JsonNode aggregates)
    {
        // modify the aggregates JsonNode object (to be used in SHUFFLE, and GBY in
        // reducer):
        // a) if it is dual agg, use the outer aggregator
        // b) if it is COUNT or COUNT_DISTINCT agg, switch it to SUM
        // c) use the original output column name as input column name
        for (JsonNode aggNode : aggregates)
        {
            String type;
            JsonNode typeJson = aggNode.get("type");
            if (typeJson.isArray())
                type = typeJson.get(0).getTextValue();
            else
                type = typeJson.getTextValue();

            String outputColName = getText(aggNode, "output");

            ObjectNode onode = (ObjectNode) aggNode;
            // see if the aggregation type has to be changed
            if (type.equals("COUNT") || type.equals("COUNT_DISTINCT"))
                onode.put("type", "SUM");
            else
                onode.put("type", type);

            // change the input column name
            onode.put("input", outputColName);
        }
    }

    private JsonNode rewriteDistinct(JsonNode job)
    {
        ObjectNode newJob = (ObjectNode) cloneNode(job);
        ObjectNode shuffle = (ObjectNode) newJob.get("shuffle");
        String name = getText(shuffle, "name");

        ObjectNode distinctOp =
                JsonUtils.createObjectNode("operator",
                                           "DISTINCT",
                                           "input",
                                           name,
                                           "output",
                                           name);

        if (!newJob.has("reduce") || newJob.get("reduce").isNull())
            newJob.put("reduce", mapper.createArrayNode());
        ArrayNode reduce = (ArrayNode) newJob.get("reduce");
        reduce.insert(0, distinctOp);

        shuffle.put("type", "SHUFFLE");
        shuffle.put("distinctShuffle", true);

        return newJob;
    }

    private JsonNode rewriteJoin(JsonNode job)
    {
        ObjectNode newJob = (ObjectNode) cloneNode(job);
        ObjectNode shuffle = (ObjectNode) newJob.get("shuffle");
        JsonNode joinKeys = shuffle.get("joinKeys");
        String blockName = getText(shuffle, "name");

        // make sure there are two mappers in the job
        JsonNode mapJsons = newJob.get("map");
        if (mapJsons.size() != 2)
        {
            throw new RuntimeException("There must be exactly two multimappers for JOIN shuffle command.");
        }

        // Add the Map side operator in each of the mappers
        // tag = 1, for the first mapper (non dimensional)
        // tag = 0, for the second dimensional mapper
        int tag = 1;
        for (JsonNode mapJson: mapJsons)
        {
            if (!mapJson.has("operators") || mapJson.get("operators").isNull())
                ((ObjectNode) mapJson).put("operators", mapper.createArrayNode());
            ArrayNode operators = (ArrayNode) mapJson.get("operators");

            // we need unique references for all blockIndexJoin
            operators.add(createObjectNode("operator", "REDUCE_JOIN_MAPPER",
                                           "input", createArrayNode(blockName),
                                           "output", blockName,
                                           "joinKeys", joinKeys,
                                           "tag", tag));
            tag --;
        }

        // create the reduce side operator
        ObjectNode reducerOperator = createObjectNode("operator", "REDUCE_JOIN",
                                                      "input", createArrayNode(blockName),
                                                      "output", blockName,
                                                      "joinKeys", joinKeys);
        if (shuffle.has("joinType"))
            reducerOperator.put("joinType", shuffle.get("joinType"));

        // add the reduce side operator
        if (!newJob.has("reduce") || newJob.get("reduce").isNull())
        {
            newJob.put("reduce", mapper.createArrayNode());
        }
        ArrayNode reduce = (ArrayNode) newJob.get("reduce");
        reduce.insert(0, reducerOperator);

        // Fix the shuffle json
        if (shuffle.has("partitionKeys"))
        {
            String[] partitionKeys = JsonUtils.asArray(shuffle, "partitionKeys");
            String[] joinKeyNames = JsonUtils.asArray(shuffle, "joinKeys");
            // make sure that partitionKeys is prefix of joinKeys
            if (!CommonUtils.isPrefix(joinKeyNames, partitionKeys))
            {
                throw new RuntimeException("Partition key must be a prefix of join keys");
            }
        } else {
            shuffle.put("partitionKeys", shuffle.get("joinKeys"));
        }
        // We will sort on (joinKeys + ___tag)
        JsonNode pivotKeys = cloneNode(shuffle.get("joinKeys"));
        ((ArrayNode) pivotKeys).add("___tag");

        shuffle.put("type", "SHUFFLE");
        shuffle.put("join", true);
        shuffle.put("pivotKeys", pivotKeys);
        shuffle.remove("joinKeys");

        return newJob;
    }

    private void copyLine(ObjectNode from, ObjectNode to, String prefix)
    {
        if (from.has("line"))
            to.put("line", prefix + getText(from, "line"));
    }
}
