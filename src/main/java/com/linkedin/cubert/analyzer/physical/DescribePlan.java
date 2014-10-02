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

import static com.linkedin.cubert.utils.JsonUtils.getText;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.utils.JsonUtils;
import com.linkedin.cubert.utils.print;

/**
 * Describes the schema of the output files.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DescribePlan extends PhysicalPlanVisitor
{

    private final Set<String> pathsSeen = new HashSet<String>();

    public void describe(JsonNode plan)
    {
        print.f("----------------------------------");
        print.f("Schemas of the output datasets:\n");

        new PhysicalPlanWalker(plan, this).walk();
    }

    @Override
    public void visitOutput(JsonNode json)
    {
        JsonNode pathJson = json.get("path");
        String path;
        if (pathJson.isArray())
            pathJson = pathJson.get(0);

        if (pathJson.isTextual())
        {
            path = pathJson.getTextValue();
        }
        else
        {
            path =
                    String.format("(%s, %s, %s)",
                                  getText(pathJson, "root"),
                                  getText(pathJson, "startDate"),
                                  getText(pathJson, "endDate"));
        }

        if (pathsSeen.contains(path))
            return;

        pathsSeen.add(path);

        String type = getText(json, "type");
        JsonNode schemaJson = json.get("schema");

        BlockSchema schema = new BlockSchema(schemaJson);
        print.f("%3d. (%s) %s %s", pathsSeen.size(), type, path, schema.toString());

        if (type.equalsIgnoreCase("CubertStore"))
        {
            print.f("\tPARTITIONED ON %s",
                    Arrays.toString(JsonUtils.asArray(json, "partitionKeys")));
            print.f("\tSORTED ON %s",
                    Arrays.toString(JsonUtils.asArray(json, "sortKeys")));
            print.f("\tBLOCKGEN ID: %s", JsonUtils.getText(json, "blockgenId"));
        }
    }
}
