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

import com.linkedin.cubert.memory.LongArrayList;


/**
 * Builtin default implementation of a {@link DupleCubeAggregator} that aggregates input
 * columns using inner and outer {@link ValueAggregator}.
 * <p>
 * Implementation notes: this object creates an extra copy of long[] array (in addition to
 * the one created by the superclass {@link DefaultCubeAggregator}) for aggregating values
 * for inner dimension.
 * 
 * @see DefaultCubeAggregator
 * @author Maneesh Varshney
 * 
 */
public final class DefaultDupleCubeAggregator extends DefaultCubeAggregator implements
        DupleCubeAggregator
{
    // ValueAggregator for aggregating across inner dimension
    private final ValueAggregator innerAgg;

    // array for storing aggregated values across inner dimension
    private final LongArrayList innerValueTable;

    public DefaultDupleCubeAggregator(ValueAggregator outerAgg, ValueAggregator innerAgg)
    {
        super(outerAgg);
        this.innerAgg = innerAgg;

        innerValueTable = new LongArrayList();
        innerValueTable.setDefaultValue(innerAgg.initialValue());
    }

    @Override
    public void allocate(int size)
    {
        super.allocate(size);

        // allocate and initialize the inner aggregation table
        innerValueTable.ensureCapacity(size);
    }

    @Override
    public void clear()
    {
        super.clear();

        innerValueTable.reset();
    }

    @Override
    public void innerAggregate(int index)
    {
        if (currentValue == null)
            return;

        innerValueTable.ensureCapacity(index);

        final long previousValue = innerValueTable.getLong(index);
        innerValueTable.updateLong(index, innerAgg.aggregate(previousValue, currentValue));
    }

    @Override
    public void aggregate(int index)
    {
        // obtain the inner aggregate value
        Object innerVal = innerAgg.output(innerValueTable.getLong(index));

        // reset it to initial value again
        innerValueTable.updateLong(index, innerAgg.initialValue());

        if (innerVal == null)
            return;
        // aggregate the "outer" aggregation table
        valueTable.updateLong(index, aggregator.aggregate(valueTable.getLong(index), innerVal));
    }

}
