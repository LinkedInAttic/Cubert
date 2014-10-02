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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.linkedin.cubert.utils.FileSystemUtils;

/**
 * Analyzes the plan for cached files.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CachedFileAnalyzer implements PlanRewriter
{
    private final Configuration conf = new JobConf();

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Map<String, String> latestPathMap = new HashMap<String, String>();
        ObjectMapper mapper = new ObjectMapper();

        for (JsonNode job : plan.path("jobs"))
        {
            if (job.has("cachedFiles") && !job.get("cachedFiles").isNull())
            {
                ArrayNode cachedFiles = mapper.createArrayNode();
                for (JsonNode file : job.path("cachedFiles"))
                {
                    String path = file.getTextValue();

                    if (path.contains("#LATEST"))
                    {
                        final int beginIndex = path.lastIndexOf('#');
                        final String latestPath;
                        if (path.substring(beginIndex).startsWith("#LATEST"))
                        {
                            latestPath =
                                    FileSystemUtils.getLatestPath(fs, new Path(path))
                                                   .toUri()
                                                   .getPath();
                        }
                        else
                        {
                            latestPath =
                                    FileSystemUtils.getLatestPath(fs,
                                                                  new Path(path.substring(0,
                                                                                          beginIndex)))
                                                   .toUri()
                                                   .getPath()
                                                   .concat(path.substring(beginIndex));
                        }
                        cachedFiles.add(latestPath);
                        latestPathMap.put(path, latestPath);
                    }
                    else
                    {
                        cachedFiles.add(file);
                    }
                }
                ((ObjectNode) job).put("cachedFiles", cachedFiles);
            }
        }

        new PhysicalPlanWalker(plan, new ReplaceCachedFilePath(latestPathMap)).walk();

        return plan;
    }

    static final class ReplaceCachedFilePath extends PhysicalPlanVisitor
    {
        private final Map<String, String> map;

        ReplaceCachedFilePath(Map<String, String> map)
        {
            this.map = map;
        }

        @Override
        public void visitOperator(JsonNode json, boolean isMapper)
        {
            if (getText(json, "operator").equals("LOAD_CACHED_FILE"))
            {
                String path = getText(json, "path");
                if (map.containsKey(path))
                    ((ObjectNode) json).put("path", map.get(path));
            }
        }

    }
}
