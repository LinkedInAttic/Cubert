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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;

/**
 * Analyzes if the output folders are present and if they should be overwritten.
 * 
 * @author Maneesh Varshney
 * 
 */
public class OverwriteAnalyzer extends PhysicalPlanVisitor implements PlanRewriter
{
    private FileSystem fs;
    private boolean defaultOverwriteFlag = false;

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        if (plan.has("hadoopConf") && !plan.get("hadoopConf").isNull())
        {
            JsonNode conf = plan.get("hadoopConf");
            if (conf.has("overwrite") && !conf.get("overwrite").isNull())
            {
                defaultOverwriteFlag =
                        Boolean.parseBoolean(conf.get("overwrite").getTextValue());
            }
        }

        fs = FileSystem.get(new JobConf());
        new PhysicalPlanWalker(plan, this).walk();
        return plan;
    }

    @Override
    public void visitOutput(JsonNode json)
    {
        boolean overwrite = defaultOverwriteFlag;

        // if the flag is overridden for this particular output
        if (json.has("params") && json.get("params").has("overwrite"))
        {
            overwrite =
                    Boolean.parseBoolean(json.get("params")
                                             .get("overwrite")
                                             .getTextValue());
        }

        if (!overwrite)
        {
            Path outputPath = new Path(getText(json, "path"));
            try
            {
                if (fs.exists(outputPath))
                {
                    throw new PlanRewriteException(String.format("The output path '%s' already exists and overwrite is 'false'.",
                                                                 outputPath));
                }
            }
            catch (IOException e)
            {
                throw new PlanRewriteException(e);
            }
        }

    }
}
