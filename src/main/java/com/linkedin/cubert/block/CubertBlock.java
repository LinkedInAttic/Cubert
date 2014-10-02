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
import java.util.Iterator;

import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonNode;

import com.linkedin.cubert.io.rubix.RubixTupleCreator;

/**
 * 
 * Wrapper around BlockIterator that implements the Block interface. Needed for
 * loading/reading a block from a Cubert file.
 * 
 * @author Krishna Puttaswamy
 * 
 */

public class CubertBlock implements Block
{
    private final BlockProperties props;
    private BlockIterator<Tuple> tupleIterator;
    private RubixTupleCreator tupleCreator;

    public CubertBlock(BlockProperties props, Iterator<Tuple> tupleIterator)
    {
        this.props = props;
        this.tupleIterator = (BlockIterator<Tuple>) tupleIterator;
        this.tupleCreator = new RubixTupleCreator();
    }

    public Tuple next() throws IOException
    {
        Tuple n = tupleIterator.next();
        if (n == null)
            return null;
        else
            return tupleCreator.create(null, n);
    }

    @Override
    public void configure(JsonNode json) throws IOException
    {
        tupleCreator.setup(json);
    }

    @Override
    public void rewind() throws IOException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public BlockProperties getProperties()
    {
        return props;
    }
}
