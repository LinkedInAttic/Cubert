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

package com.linkedin.cubert.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Dictionary for translating column values to integer codes, and vice versa.
 * 
 * The class also has methods to read and write data from files.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CodeDictionary
{
    private final Map<String, Integer> keyToCodeMap = new HashMap<String, Integer>();
    private final Map<Integer, String> codeToKeyMap = new HashMap<Integer, String>();
    private int nextCode = 1;

    public int getCodeForKey(String key)
    {
        Integer code = keyToCodeMap.get(key);
        return code == null ? -1 : code;
    }

    public String getValueForCode(int code)
    {
        return codeToKeyMap.get(code);
    }

    public int addKey(String key)
    {
        int code = getCodeForKey(key);

        // If key already exists, return the existing code
        if (code > 0)
            return code;

        if (nextCode < 0)
            throw new RuntimeException("CodeDictionary cannot store more data");

        // assign a new code to this key
        code = nextCode;
        nextCode++;

        keyToCodeMap.put(key, code);
        codeToKeyMap.put(code, key);

        return code;
    }

    public Set<String> keySet()
    {
        return keyToCodeMap.keySet();
    }

    public void addKeyCode(String key, int code)
    {
        keyToCodeMap.put(key, code);
        codeToKeyMap.put(code, key);
        if (code >= nextCode)
        {
            nextCode = (code + 1);
        }
    }

    /**
     * Reads dictionary from local filesystem.
     * 
     * @param filename
     * @throws IOException
     */
    public void read(String filename) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        read(reader);
        reader.close();
    }

    /**
     * Reads dictionary from the HDFS filesystem.
     * 
     * @param fs
     * @param path
     * @throws IOException
     */
    public void read(FileSystem fs, Path path) throws IOException
    {
        FSDataInputStream istream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(istream));
        read(reader);
        reader.close();
    }

    private void read(BufferedReader reader) throws IOException
    {
        String line;

        while ((line = reader.readLine()) != null)
        {
            String[] keyval = line.split("\\s+");
            String key = keyval[0];
            int code = Integer.parseInt(keyval[1]);

            if (nextCode < (code + 1))
                nextCode = (code + 1);

            keyToCodeMap.put(key, code);
            codeToKeyMap.put(code, key);
        }

    }

    public void write(FileSystem fs, Path path) throws IOException
    {
        // if the path exists, rename the existing file with ".old" suffix
        if (fs.exists(path))
        {
            Path renamePath = new Path(path.toString() + ".old");
            fs.delete(renamePath, false);

            fs.rename(path, renamePath);
        }

        // Write data to file
        FSDataOutputStream ostream = fs.create(path);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(ostream));

        for (Map.Entry<String, Integer> entry : keyToCodeMap.entrySet())
        {
            String line = String.format("%s %d\n", entry.getKey(), entry.getValue());
            writer.write(line);
        }

        writer.flush();
        writer.close();
        ostream.close();
    }

    @Override
    public String toString()
    {
        return keyToCodeMap == null ? "<null>" : keyToCodeMap.toString();
    }

}
