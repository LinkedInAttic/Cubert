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

import com.linkedin.cubert.io.rubix.RubixFile;
import com.linkedin.cubert.operator.OperatorType;
import com.linkedin.cubert.utils.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.*;

import static com.linkedin.cubert.utils.JsonUtils.getText;

/**
 * Analyzes the lineage of blockgens across the different CubertStore datasets.
 * 
 * @author Maneesh Varshney
 * 
 */
public class BlockgenLineageAnalyzer extends PhysicalPlanVisitor implements PlanRewriter
{
    private Configuration conf;
    private final Map<String, String> blockgenIdMap = new HashMap<String, String>();
    private String currentBlockgenId = null;

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        conf = new JobConf();

        // first get the blockgen id from global input cubert files
        JsonNode inputs = plan.get("input");
        if (inputs != null && !inputs.isNull())
        {
            Iterator<String> inputsIt = inputs.getFieldNames();
            while (inputsIt.hasNext())
            {
                String input = inputsIt.next();
                JsonNode json = inputs.get(input);
                String type = getText(json, "type");
                if (type.equalsIgnoreCase("RUBIX"))
                {
                    try
                    {
                        blockgenIdMap.put(input, getBlockgenId(input));
                    }
                    catch (ClassNotFoundException e)
                    {
                        throw new PlanRewriteException(e);
                    }
                }
            }
        }

        new PhysicalPlanWalker(plan, this).walk();
        return plan;
    }

    private String getBlockgenId(String input) throws IOException,
            ClassNotFoundException
    {
        Path path = new Path(input);
        Path afile = RubixFile.getARubixFile(conf, path);
        RubixFile<Tuple, Object> rubixFile = new RubixFile<Tuple, Object>(conf, afile);

        return rubixFile.getBlockgenId();
    }

    @Override
    public void enterJob(JsonNode json)
    {
        currentBlockgenId = null;
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

        // blockgenId related
        if (getText(json, "type").equalsIgnoreCase("RUBIX"))
        {
            currentBlockgenId = this.blockgenIdMap.get(path);
            if (currentBlockgenId == null)
                error(json,
                      "Attempting to load a rubix file that was not created by BLOCKGEN or BLOCKGEN BY INDEX");
        }
    }

    @Override
    public void visitOperator(JsonNode json, boolean isMapper)
    {

        // special cases for individual operators
        OperatorType type = OperatorType.valueOf(getText(json, "operator"));
        switch (type)
        {
        case LOAD_BLOCK:
        {
            String path = getText(json, "path");
            String blockgenId = this.blockgenIdMap.get(path);
            if (blockgenId == null)
                error(json,
                      "Attempting to load a rubix block that was not BLOCKGEN or BLOCKGEN BY INDEX");
            if (currentBlockgenId == null)
                error(json,
                      "Attempting to load a rubix block without reference to a valid MATCHING rubix block.");
            if (!currentBlockgenId.equals(blockgenId))
                error(json,
                      "Attempting to load rubix block that is inconsistently partitioned as the MATCHING rubix block.");
            break;
        }
        case CREATE_BLOCK:
        {
            boolean isIndexed = getText(json, "blockgenType").equalsIgnoreCase("BY_INDEX");

            if (isIndexed)
            {
                String parentPath = getText(json, "indexPath");
                currentBlockgenId = this.blockgenIdMap.get(parentPath);
                if (currentBlockgenId == null)
                    error(json,
                          "Attempting to create rubix block BY INDEX from an invalid rubix file.");
            }
            else
            {
                currentBlockgenId = UUID.randomUUID().toString();
            }
            break;
        }
        }
    }

    @Override
    public void visitShuffle(JsonNode json)
    {
        currentBlockgenId = null;
    }

    @Override
    public void visitOutput(JsonNode json)
    {
        // blockgen Id related
        if (getText(json, "type").equalsIgnoreCase("RUBIX"))
        {
            if (currentBlockgenId == null)
                error(json,
                      "Attempting to write to rubix file data that is not BLOCKGEN or BLOCKGEN BY INDEX");
            this.blockgenIdMap.put(getText(json, "path"), currentBlockgenId);
        }
    }

    @Override
    public void exitJob(JsonNode json)
    {
        JsonNode outputJson = json.get("output");
        boolean isRubixStorage =
                outputJson.has("type")
                        && getText(outputJson, "type").equalsIgnoreCase("rubix");
        if (isRubixStorage)
        {
            ((ObjectNode) outputJson).put("blockgenId", currentBlockgenId);
        }

        currentBlockgenId = null;
    }

    private void error(JsonNode json, String format, Object... args)
    {
        error(null, json, format, args);
    }

    private void error(Exception e, JsonNode json, String format, Object... args)
    {
        // if (debugMode && e != null)
        // e.printStackTrace();
        //
        // hasErrors = true;
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
}
