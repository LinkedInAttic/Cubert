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

package com.linkedin.cubert.operator.cube;

import com.linkedin.cubert.block.DataType;
import com.linkedin.cubert.operator.PreconditionException;
import com.linkedin.cubert.operator.PreconditionExceptionType;

/**
 * Factory class for creating {@link ValueAggregator}.
 * 
 * @author Maneesh Varshney
 * 
 */
public class ValueAggregatorFactory
{
    /**
     * Creates {@link ValueAggregator} objects.
     * <p>
     * This method can throw {@link PreconditionException} if the data type of the input
     * field is not valid (e.g. SUM of string fields).
     * 
     * @param type
     *            the type of aggregator
     * @param inputType
     *            the data type of the input values
     * @param colName
     *            the name of the input column
     * @return ValueAggregator object
     * @throws PreconditionException
     */
    public static ValueAggregator get(ValueAggregationType type,
                                      DataType inputType,
                                      String colName) throws PreconditionException
    {
        switch (type)
        {
        case COUNT:
            return new CountAggregator();
        case COUNT_TO_ONE:
            return new CountToOneAggregator();
        case MAX:
            assertNumericalType("MAX", colName, inputType);
            return new MaxAggregator(inputType);
        case MIN:
            assertNumericalType("MIN", colName, inputType);
            return new MinAggregator(inputType);
        case SUM:
            assertNumericalType("SUM", colName, inputType);
            return new SumAggregator(inputType);
        }
        return null;
    }

    private static void assertNumericalType(String aggregator,
                                            String colName,
                                            DataType inputType) throws PreconditionException
    {
        if (inputType.isIntOrLong() || inputType.isReal())
            return;

        String msg =
                String.format("Expected type for %s(%s): int, long, float or double. Found: %s",
                              aggregator,
                              colName,
                              inputType);

        throw new PreconditionException(PreconditionExceptionType.INVALID_SCHEMA, msg);
    }

    private ValueAggregatorFactory()
    {

    }

    /**
     * Counts the number of values.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class CountAggregator implements ValueAggregator
    {
        @Override
        public long initialValue()
        {
            return 0L;
        }

        @Override
        public long aggregate(long lastAggregate, Object currentValue)
        {
            return lastAggregate + 1;
        }

        @Override
        public Object output(long value)
        {
            return value;
        }

        @Override
        public DataType outputType()
        {
            return DataType.LONG;
        }
    }

    /**
     * Reports 0 if no value is seen, or 1 if one or more value are seen.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class CountToOneAggregator implements ValueAggregator
    {
        @Override
        public long initialValue()
        {
            return 0L;
        }

        @Override
        public long aggregate(long lastAggregate, Object currentValue)
        {
            return 1L;
        }

        @Override
        public Object output(long value)
        {
            return value;
        }

        @Override
        public DataType outputType()
        {
            return DataType.LONG;
        }
    }

    /**
     * Computes sum of the values.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class SumAggregator implements ValueAggregator
    {
        private final DataType type;

        SumAggregator(DataType type)
        {
            this.type = type;
        }

        @Override
        public long initialValue()
        {
            switch (type)
            {
            case INT:
            case LONG:
                return 0L;
            default:
                return Double.doubleToLongBits(0.0);
            }
        }

        @Override
        public long aggregate(long lastAggregate, Object currentValue)
        {
            switch (type)
            {
            case INT:
            case LONG:
                return lastAggregate + ((Number) currentValue).longValue();
            default:
                return Double.doubleToLongBits(Double.longBitsToDouble(lastAggregate)
                        + ((Number) currentValue).doubleValue());
            }
        }

        @Override
        public Object output(long value)
        {
            switch (type)
            {
            case INT:
                return (int) value;
            case LONG:
                return value;
            case FLOAT:
                return (float) Double.longBitsToDouble(value);
            default:
                return Double.longBitsToDouble(value);
            }
        }

        @Override
        public DataType outputType()
        {
            return type;
        }
    }

    /**
     * Computes minimum of the values.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class MinAggregator implements ValueAggregator
    {
        private final DataType type;

        MinAggregator(DataType type)
        {
            this.type = type;
        }

        @Override
        public long initialValue()
        {
            switch (type)
            {
            case INT:
            case LONG:
                return Long.MAX_VALUE;
            default:
                return Double.doubleToLongBits(Double.MAX_VALUE);
            }
        }

        @Override
        public long aggregate(long lastAggregate, Object currentValue)
        {
            switch (type)
            {
            case INT:
            case LONG:
            {
                long current = ((Number) currentValue).longValue();
                return current < lastAggregate ? current : lastAggregate;

            }
            default:
            {
                double last = Double.longBitsToDouble(lastAggregate);
                double current = ((Number) currentValue).doubleValue();

                double min = current < last ? current : last;
                return Double.doubleToLongBits(min);
            }
            }

        }

        @Override
        public Object output(long value)
        {
            switch (type)
            {
            case INT:
                return (value == Long.MAX_VALUE) ? null : (int) value;
            case LONG:
                return (value == Long.MAX_VALUE) ? null : value;
            case FLOAT:
            {
                double dvalue = Double.longBitsToDouble(value);
                return (dvalue == Double.MAX_VALUE) ? null : (float) dvalue;
            }
            default:
            {
                double dvalue = Double.longBitsToDouble(value);
                return (dvalue == Double.MAX_VALUE) ? null : dvalue;
            }
            }
        }

        @Override
        public DataType outputType()
        {
            return type;
        }
    }

    /**
     * Computes maximum of the values.
     * 
     * @author Maneesh Varshney
     * 
     */
    private static final class MaxAggregator implements ValueAggregator
    {
        private final DataType type;

        MaxAggregator(DataType type)
        {
            this.type = type;
        }

        @Override
        public long initialValue()
        {
            switch (type)
            {
            case INT:
            case LONG:
                return Long.MIN_VALUE;
            default:
                return Double.doubleToLongBits(Double.MIN_VALUE);
            }
        }

        @Override
        public long aggregate(long lastAggregate, Object currentValue)
        {
            switch (type)
            {
            case INT:
            case LONG:
            {
                long current = ((Number) currentValue).longValue();
                return current > lastAggregate ? current : lastAggregate;

            }
            default:
            {
                double last = Double.longBitsToDouble(lastAggregate);
                double current = ((Number) currentValue).doubleValue();

                double min = current > last ? current : last;
                return Double.doubleToLongBits(min);
            }
            }

        }

        @Override
        public Object output(long value)
        {
            switch (type)
            {
            case INT:
                return (value == Long.MAX_VALUE) ? null : (int) value;
            case LONG:
                return (value == Long.MAX_VALUE) ? null : value;
            case FLOAT:
            {
                double dvalue = Double.longBitsToDouble(value);
                return (dvalue == Double.MAX_VALUE) ? null : (float) dvalue;
            }
            default:
            {
                double dvalue = Double.longBitsToDouble(value);
                return (dvalue == Double.MAX_VALUE) ? null : dvalue;
            }
            }
        }

        @Override
        public DataType outputType()
        {
            return type;
        }
    }
}
