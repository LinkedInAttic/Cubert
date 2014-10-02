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

/**
 * A memory-efficient implementation of [SUM, COUNT_TO_ONE] DupleCubeAggregator, for the
 * special case when the aggregated column is known to be never null. For example, when
 * the aggregated column is also the inner dimension.
 * 
 * @author Maneesh Varshney
 * 
 */
public class CountDistinctCubeAggregator extends DefaultCubeAggregator implements
        DupleCubeAggregator
{
    public CountDistinctCubeAggregator(String colName) throws PreconditionException
    {
        super(ValueAggregatorFactory.get(ValueAggregationType.COUNT,
                                         DataType.UNKNOWN,
                                         colName));
    }

    @Override
    public void innerAggregate(int index)
    {
        // do nothing!!
    }

}
