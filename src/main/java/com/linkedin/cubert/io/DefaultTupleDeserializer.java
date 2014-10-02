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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.pig.data.Tuple;

/**
 * Default deserialization of tuple using the readField() method of the tuple.
 * 
 * @author Maneesh Varshney
 * 
 */
public class DefaultTupleDeserializer implements Deserializer<Tuple>
{
    private DataInput in;

    @Override
    public void open(InputStream in) throws IOException
    {
        this.in = new DataInputStream(in);
    }

    @Override
    public Tuple deserialize(Tuple t) throws IOException
    {
        t.readFields(in);
        return t;
    }

    @Override
    public void close() throws IOException
    {

    }

}
