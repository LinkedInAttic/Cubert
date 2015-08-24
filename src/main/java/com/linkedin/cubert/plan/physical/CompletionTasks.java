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

package com.linkedin.cubert.plan.physical;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.utils.JsonUtils;

/**
 * Executes file systems tasks (rm, cp, mv etc).
 * 
 * @author Maneesh Varshney
 */
public class CompletionTasks
{
    public static void doCompletionTasks(JsonNode tasks) throws IOException
    {
        FileSystem fs = FileSystem.get(new JobConf());

        for (int i = 0; i < tasks.size(); i++)
        {
            try
            {
                final JsonNode task = tasks.get(i);
                final String taskType = JsonUtils.getText(task, "type");
                final String[] paths = JsonUtils.asArray(task, "paths");

                if (taskType.equals("rm"))
                {
                    for (String path : paths)
                    {
                        System.out.println("Deleting path " + path + "...");
                        fs.delete(new Path(path), true);
                    }
                }
                else if (taskType.equals("mv"))
                {
                    System.out.println("Moving " + paths[0] + " to " + paths[1] + "...");

                    final Path from = new Path(paths[0]);
                    final Path to = new Path(paths[1]);
                    fs.delete(to, true);
                    fs.rename(from, to);
                }
                else if (taskType.equals("mkdir"))
                {
                    for (String path : paths)
                    {
                        System.out.println("Creating directory " + path);
                        fs.mkdirs(new Path(path));
                    }
                }
            }
            catch (IOException e)
            {
                System.err.println("ERROR: " + e.getMessage());
            }
        }
    }
}
