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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
import org.apache.pig.data.Tuple;

/**
 * Default serialization of tuple using hte write() methods of the tuple.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DefaultTupleSerializer implements Serializer<Tuple>
{
    private DataOutput out;

    @Override
    public void open(OutputStream out) throws IOException
    {
        this.out = new DataOutputStream(out);
    }

    @Override
    public void serialize(Tuple t) throws IOException
    {
        t.write(out);
    }

    @Override
    public void close() throws IOException
    {
        // TODO Auto-generated method stub

    }

}
