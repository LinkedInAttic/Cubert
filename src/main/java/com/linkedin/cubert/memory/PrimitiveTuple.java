/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.cubert.memory;

import org.apache.pig.data.Tuple;

/**
 * Get primitive data from Tuple. Avoids boxing of primitive types
 *
 * Created by spyne on 1/14/15.
 */
public interface PrimitiveTuple extends Tuple
{
    public int getInt(int fieldNum);

    public long getLong(int fieldNum);

    public double getDouble(int fieldNum);
}
