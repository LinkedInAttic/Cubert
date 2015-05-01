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
import java.net.URI;
import java.net.URISyntaxException;
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

    private static String cleanLatestTag(String fname){
      if (fname.contains("#LATEST"))
        return fname.replace("#LATEST", "!LATEST");
      return fname;
    }

    private static String restoreLatestTag(String fname){
      if (fname.contains("!LATEST"))
        return fname.replace("!LATEST", "#LATEST");
      return fname;
    }

    @Override
    public JsonNode rewrite(JsonNode plan,
                            Set<String> namesUsed,
                            boolean debugMode,
                            boolean revisit) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Map<String, String> symlinkMap = new HashMap<String, String>();
        ObjectMapper mapper = new ObjectMapper();
        int symlinkCounter = 0;

        for (JsonNode job : plan.path("jobs"))
        {
            if (job.has("cachedFiles") && !job.get("cachedFiles").isNull())
            {
                ArrayNode cachedFiles = mapper.createArrayNode();
                for (JsonNode file : job.path("cachedFiles"))
                {
                    String filename = file.getTextValue();

                    filename = cleanLatestTag(filename);
                    URI uri = null;
                    String path, fragment;
                    try
                    {
                        uri = new URI(filename);
                        path = uri.getPath();
                        path = restoreLatestTag(path);

                        fragment = uri.getFragment();
                        if (path.contains("#LATEST"))
                        {
                          path =
                            FileSystemUtils.getLatestPath(fs, new Path(path))
                            .toString();
                          path = new URI(path).getPath();
                        }
                    }
                    catch (URISyntaxException e)
                    {
                        throw new PlanRewriteException(e);
                    }

                    // check if the fragment was already created earlier
                    if (fragment == null)
                        fragment = symlinkMap.get(path);

                    // create a new one
                    if (fragment == null)
                        fragment = "cached_" + (symlinkCounter++);

                    symlinkMap.put(path, fragment);
                    

                    // if (fs.isDirectory(new Path(path)))
                    // {
                    // Path childPath = null;
                    //
                    // FileStatus[] children = fs.globStatus(new Path(path + "/*"));
                    // for (FileStatus child : children)
                    // {
                    // childPath = child.getPath();
                    //
                    // if (fs.isDirectory(childPath))
                    // continue;
                    //
                    // String name = childPath.getName();
                    // if (name.startsWith("_") || name.startsWith("."))
                    // continue;
                    //
                    // break;
                    // }
                    //
                    // if (childPath == null)
                    // throw new IOException("No files found in directory: " + path);
                    //
                    // path = childPath.toString();
                    // }

                    cachedFiles.add(path + "#" + fragment);
                }
                ((ObjectNode) job).put("cachedFiles", cachedFiles);
                
            }
        }

        new PhysicalPlanWalker(plan, new AddSymlinksToCachedPath(symlinkMap)).walk();

        return plan;
    }

    static final class AddSymlinksToCachedPath extends PhysicalPlanVisitor
    {
        private final Map<String, String> map;
        private final Configuration conf = new JobConf();

        AddSymlinksToCachedPath(Map<String, String> map)
        {
            this.map = map;
        }

        @Override
        public void visitOperator(JsonNode json, boolean isMapper)
        {
            String type = getText(json, "operator");
            if (type.equals("LOAD_CACHED_FILE") || type.equals("DICT_ENCODE")
                    || type.equals("DICT_DECODE"))
            {

                if (!json.has("path"))
                    return;

                try
                {
                    String originalPath = getText(json, "path");
                    originalPath = cleanLatestTag(originalPath);
                    URI uri = new URI(originalPath);
                    String path = uri.getPath();
                    path = restoreLatestTag(path);

                     if (path.contains("#LATEST"))
                     {
                       path = 
                         FileSystemUtils.getLatestPath(FileSystem.get(conf), new Path(path))
                           .toString();
                       path = new URI(path).getPath();
                     }
                     
                     if (map.containsKey(path))
                     {
                         String fragment = map.get(path);
                         ((ObjectNode) json).put("path", path + "#" + fragment);
                     }

                }
                catch (URISyntaxException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (IOException e){
                  throw new RuntimeException(e);
                }
            }
        }
    }
}
