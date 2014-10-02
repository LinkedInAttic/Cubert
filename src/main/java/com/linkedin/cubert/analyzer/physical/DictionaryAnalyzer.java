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

import static com.linkedin.cubert.utils.JsonUtils.createObjectNode;
import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.block.BlockSchema;

/**
 * Analyses the dictionary jobs in the script.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DictionaryAnalyzer extends PhysicalPlanVisitor implements PlanRewriter
{
    private final Map<String, String[]> dictionaryColumns =
            new HashMap<String, String[]>();

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        new PhysicalPlanWalker(plan, this).walk();
        return plan;
    }

    @Override
    public void enterJob(JsonNode json)
    {

        // if this is dictionary job, retrieve the columns to be encoded
        if (json.has("jobType") && getText(json, "jobType").equals("GENERATE_DICTIONARY"))
        {
            String path = getText(json.get("output"), "path");
            BlockSchema schema = new BlockSchema(getText(json.get("output"), "columns"));
            String[] columns = schema.getColumnNames();
            dictionaryColumns.put(path, columns);
        }
    }

    @Override
    public void exitProgram(JsonNode json)
    {
        ObjectMapper mapper = new ObjectMapper();

        // put the dictionary columns in the json
        ObjectNode dictionaryNode = mapper.createObjectNode();
        ((ObjectNode) json).put("dictionary", dictionaryNode);
        for (String path : dictionaryColumns.keySet())
        {
            String[] columns = dictionaryColumns.get(path);
            ArrayNode node = mapper.createArrayNode();
            for (String column : columns)
            {
                node.add(createObjectNode("name", column, "type", "INT"));
            }
            dictionaryNode.put(path, node);
        }
    }
}
