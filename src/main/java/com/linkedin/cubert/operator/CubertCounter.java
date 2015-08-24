/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.operator;

import org.apache.hadoop.mapreduce.Counter;


/**
 * @author Mani Parkhe
 */
public enum CubertCounter
{
    CUBE_FLUSH_COUNTER("Cube Operator flush counter"),
    HASH_JOIN_OUTPUT_COUNTER("Hash Join Operator output tuples"),
    MERGE_JOIN_OUTPUT_COUNTER("Merge Join Operator output tuples");

    private static final String GROUP_NAME = "Cubert Operators";
    private final String name;

    CubertCounter(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
    
    public String groupName()
    {
        return GROUP_NAME;
    }

    public Counter getCounter()
    {
        return PhaseContext.getCounter(getName(), groupName());
    }
}
