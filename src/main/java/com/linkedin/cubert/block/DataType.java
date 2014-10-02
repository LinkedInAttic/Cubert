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

import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Supported data types by Tuples.
 * 
 * @author Maneesh Varshney
 * 
 */
public enum DataType
{
    BYTE(org.apache.pig.data.DataType.BYTE),

    BOOLEAN(org.apache.pig.data.DataType.BOOLEAN),

    INT(org.apache.pig.data.DataType.INTEGER),

    LONG(org.apache.pig.data.DataType.LONG),

    FLOAT(org.apache.pig.data.DataType.FLOAT),

    DOUBLE(org.apache.pig.data.DataType.DOUBLE),

    STRING(org.apache.pig.data.DataType.CHARARRAY),

    MAP(org.apache.pig.data.DataType.MAP),

    TUPLE(org.apache.pig.data.DataType.TUPLE),

    BAG(org.apache.pig.data.DataType.BAG),

    ARRAY(org.apache.pig.data.DataType.BAG),

    RECORD(org.apache.pig.data.DataType.TUPLE),

    ENUM(org.apache.pig.data.DataType.CHARARRAY),

    BYTES(org.apache.pig.data.DataType.BYTEARRAY),

    UNKNOWN(org.apache.pig.data.DataType.UNKNOWN);

    private byte pigDataType;

    DataType(byte pigDataType)
    {
        this.pigDataType = pigDataType;
    }

    public byte getPigDataType()
    {
        return pigDataType;
    }

    public boolean isNumerical()
    {
        return this == BYTE || this == BOOLEAN || this == INT || this == LONG
                || this == FLOAT || this == DOUBLE;
    }

    public boolean isIntOrLong()
    {
        return this == LONG || this == INT;
    }

    public boolean isReal()
    {
        return this == DOUBLE || this == FLOAT;
    }

    public boolean isPrimitive()
    {
        return isNumerical() || this == STRING;
    }

    public boolean allowShallowCopy()
    {
        return isNumerical() || this == STRING || this == ENUM;
    }

    public static DataType getDataType(Object obj)
    {
        if (obj instanceof Byte)
            return DataType.BYTE;
        if (obj instanceof Boolean)
            return DataType.BOOLEAN;
        if (obj instanceof Integer)
            return DataType.INT;
        if (obj instanceof Long)
            return DataType.LONG;
        if (obj instanceof Float)
            return DataType.FLOAT;
        if (obj instanceof Double)
            return DataType.DOUBLE;
        if (obj instanceof String)
            return DataType.STRING;
        if (obj instanceof Map)
            return DataType.MAP;
        if (obj instanceof Tuple)
            return DataType.TUPLE;
        if (obj instanceof List)
            return DataType.ARRAY;
        if (obj instanceof DataBag)
            return DataType.BAG;

        return DataType.UNKNOWN;
    }

    public static DataType getWiderType(DataType type1, DataType type2)
    {
        // Note: JLS 5.1.2 specifies that float is a wider type than long

        switch (type1)
        {
        case DOUBLE:
        {
            return DataType.DOUBLE;
        }
        case FLOAT:
        {
            switch (type2)
            {
            case DOUBLE:
                return DataType.DOUBLE;
            case FLOAT:
            case INT:
            case LONG:
                return DataType.FLOAT;
            default:
                break;
            }
            break;
        }
        case INT:
        {
            switch (type2)
            {
            case DOUBLE:
                return DataType.DOUBLE;
            case FLOAT:
                return DataType.FLOAT;
            case INT:
                return DataType.INT;
            case LONG:
                return DataType.LONG;
            default:
                break;

            }
            break;
        }
        case LONG:
        {
            switch (type2)
            {
            case DOUBLE:
                return DataType.DOUBLE;
            case FLOAT:
                return DataType.FLOAT;
            case INT:
            case LONG:
                return DataType.LONG;
            default:
                break;

            }
            break;
        }
        default:
            break;

        }

        return null;
    }
}
