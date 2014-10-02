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

package com.linkedin.cubert.block;

import java.io.IOException;

/**
 * An adaptor class that provides a common interface to Hadoop's Mapper.Context and
 * Reducer.Context interfaces.
 * 
 * @author Maneesh Varshney
 * 
 */
public interface CommonContext
{
    boolean nextKeyValue() throws IOException,
            InterruptedException;

    Object getCurrentKey() throws IOException,
            InterruptedException;

    Object getCurrentValue() throws IOException,
            InterruptedException;

    void write(Object key, Object value) throws IOException,
            InterruptedException;
}
