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

package com.linkedin.cubert.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

/**
 * Creates diffs within the {@code Configuration} object.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ConfigurationDiff
{
    private final Configuration conf;
    private Configuration checkpoint;
    private int diffIndex = 0;
    private int totalDiffs = 0;

    public ConfigurationDiff(Configuration conf)
    {
        this.conf = conf;
    }

    public int getNumDiffs()
    {
        return conf.getInt("cubert.conf.num.diffs", 0);
    }

    public void startDiff()
    {
        if ((diffIndex + 1) > totalDiffs)
        {
            totalDiffs = diffIndex + 1;
            conf.setInt("cubert.conf.num.diffs", totalDiffs);
        }

        checkpoint = new Configuration(conf);
    }

    public void endDiff()
    {
        if (checkpoint == null)
            throw new IllegalStateException("Diffing process was not started");

        Iterator<Entry<String, String>> it = conf.iterator();
        List<String> diffKeys = new ArrayList<String>();

        while (it.hasNext())
        {
            Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String newValue = conf.get(key);
            String oldValue = checkpoint.get(key);

            if (!newValue.equals(oldValue))
            {
                // print.f("DIFF %d %s: %s => %s", diffIndex, key, oldValue, newValue);
                diffKeys.add(key);

                if (oldValue == null)
                {
                    conf.unset(key);
                }
                else
                {
                    conf.set(key, oldValue);
                    conf.set(String.format("cubert.conf.diff.%d.%s.old", diffIndex, key),
                             oldValue);
                }
                conf.set(String.format("cubert.conf.diff.%d.%s.new", diffIndex, key),
                         newValue);
            }
        }

        if (diffKeys.size() > 0)
        {
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            for (String diffKey : diffKeys)
                sb.append((idx++ == 0 ? "" : ",") + diffKey);

            conf.set(String.format("cubert.conf.diff.%d.keys", diffIndex), sb.toString());
        }

        checkpoint = null;
        diffIndex++;
    }

    public void applyDiff(int index)
    {
        int currentDiffIndex = conf.getInt("cubert.diff.current", -1);

        // undo diff if one is already present
        if (currentDiffIndex != -1 && currentDiffIndex != index)
        {
            undoDiff(currentDiffIndex);
        }

        conf.setInt("cubert.diff.current", index);

        String diffKeysStr = conf.get(String.format("cubert.conf.diff.%d.keys", index));
        if (diffKeysStr == null)
            return;

        String[] diffKeys = diffKeysStr.split(",");
        for (String key : diffKeys)
        {
            String newValue =
                    conf.get(String.format("cubert.conf.diff.%d.%s.new", index, key));

            if (newValue == null)
                conf.unset(key);
            else
                conf.set(key, newValue);
        }
    }

    public void undoDiff(int index)
    {
        String diffKeysStr = conf.get(String.format("cubert.conf.diff.%d.keys", index));
        if (diffKeysStr == null)
            return;

        String[] diffKeys = diffKeysStr.split(",");
        for (String key : diffKeys)
        {
            String oldValue =
                    conf.get(String.format("cubert.conf.diff.%d.%s.old", index, key));

            if (oldValue == null)
                conf.unset(key);
            else
                conf.set(key, oldValue);
        }

        conf.unset("cubert.diff.current");
    }

}
