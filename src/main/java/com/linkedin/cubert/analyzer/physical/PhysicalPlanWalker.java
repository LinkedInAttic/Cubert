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

import org.codehaus.jackson.JsonNode;

/**
 * Walks through the Cubert physical MapReduce plan and invokes the visitor.
 * 
 * @author Maneesh Varshney
 * 
 */
public class PhysicalPlanWalker
{
    private final PhysicalPlanVisitor visitor;
    private final JsonNode json;

    public PhysicalPlanWalker(JsonNode json, PhysicalPlanVisitor visitor)
    {
        this.json = json;
        this.visitor = visitor;
    }

    public void walk()
    {
        visitor.enterProgram(json);

        for (JsonNode job : json.path("jobs"))
        {
            visitor.enterJob(job);

            for (JsonNode map : job.path("map"))
            {
                visitor.visitMap(map);
                visitor.visitInput(map.get("input"));
                for (JsonNode operator : map.path("operators"))
                    visitor.visitOperator(operator, true);
            }

            if (job.has("shuffle") && !job.get("shuffle").isNull())
            {
                visitor.visitShuffle(job.get("shuffle"));

                for (JsonNode operator : job.path("reduce"))
                    visitor.visitOperator(operator, false);

            }

            visitor.visitOutput(job.get("output"));

            if (job.has("cachedFiles") && !job.get("cachedFiles").isNull())
            {
                for (JsonNode cachedFile : job.get("cachedFiles"))
                    visitor.visitCachedFile(cachedFile.getTextValue());
            }

            if (job.has("cacheIndex") && !job.get("cacheIndex").isNull())
            {
                for (JsonNode cacheIndex : job.get("cacheIndex"))
                    visitor.visitCachedIndex(cacheIndex);
            }

            visitor.exitJob(job);
        }

        visitor.exitProgram(json);
    }
}
