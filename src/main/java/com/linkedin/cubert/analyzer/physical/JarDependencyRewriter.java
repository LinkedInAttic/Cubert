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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Add the standard dependency jars (found in $CUBERT_HOME/lib) into the json plan.
 * 
 * @author Maneesh Varshney
 * 
 */
@Deprecated
public class JarDependencyRewriter implements PlanRewriter
{

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        ArrayNode libjars = (ArrayNode) plan.get("libjars");
        String cubertHome = null;

        if (plan.has("cubert.home"))
            cubertHome = plan.get("cubert.home").getTextValue();

        if (libjars == null)
        {
            libjars = new ObjectMapper().createArrayNode();
            ((ObjectNode) plan).put("libjars", libjars);
        }

        // add the dependency libs to libjars
        if (cubertHome == null)
            cubertHome = System.getenv("CUBERT_HOME");

        if (cubertHome == null)
        {
            System.out.println("CUBERT_HOME is not defined.");
            System.exit(1);
        }

        File libDir = new File(cubertHome, "lib");
        if (!libDir.exists())
        {
            System.out.println("Cannot find $CUBERT_HOME/lib folder.");
            System.exit(1);
        }

        if (!libDir.isDirectory())
        {
            System.out.println("$CUBERT_HOME/lib is not a directory.");
            System.exit(1);
        }

        File[] cubertJars = libDir.listFiles();
        for (File cubertJar : cubertJars)
            libjars.add(cubertJar.getAbsolutePath());

        return plan;
    }
}
