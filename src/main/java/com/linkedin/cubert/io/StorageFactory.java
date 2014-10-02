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

import com.linkedin.cubert.io.avro.AvroStorage;
import com.linkedin.cubert.io.rubix.RubixStorage;
import com.linkedin.cubert.io.shuffle.ShuffleStorage;
import com.linkedin.cubert.io.text.TextStorage;

public final class StorageFactory
{
    public static final Storage get(String type)
    {
        // these are built-in storage type
        if (type.equalsIgnoreCase("AVRO"))
            return new AvroStorage();

        if (type.equalsIgnoreCase("TEXT"))
            return new TextStorage();

        if (type.equalsIgnoreCase("RUBIX"))
            return new RubixStorage();

        if (type.equalsIgnoreCase("SHUFFLE"))
            return new ShuffleStorage();

        // if not built-in, it may be a path to external class
        try
        {
            Class<? extends Storage> cls = Class.forName(type).asSubclass(Storage.class);
            return cls.newInstance();
        }
        catch (ClassNotFoundException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InstantiationException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }
}
