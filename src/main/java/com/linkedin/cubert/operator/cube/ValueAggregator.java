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

/**
 * Stateless aggregator of values.
 * <p>
 * The steps for aggregation are as follows:
 * <ul>
 * <li>this aggregator provides an initial value (via the {@link initialValue} method).</li>
 * 
 * <li>it aggregates the current value with the last aggregate (via the {@link aggregate}
 * method) and reports the new aggregate</li>
 * </ul>
 * <p>
 * This object must accept as well as report all aggregate values as long. The
 * implementation is reponsible for converting other data types to long.
 * 
 * 
 * @see ValueAggregationType
 * @see ValueAggregatorFactory
 * @see DefaultCubeAggregator
 * @see DefaultDupleCubeAggregator
 * 
 * @author Maneesh Varshney
 * 
 */
public interface ValueAggregator
{
    /**
     * The initial value for aggregation.
     * 
     * @return the initial value for aggregation.
     */
    long initialValue();

    /**
     * Computes aggregation of the last aggregated value with the current value.
     * 
     * The invoker will ensure that the currentValue is never null.
     * 
     * @param lastAggregate
     *            previous aggregated value
     * @param currentValue
     *            current value
     * @return aggregated result of previous aggregated value and the current value
     */
    long aggregate(long lastAggregate, Object currentValue);

    /**
     * Output the value in correct data type
     * 
     * @param value
     *            the aggregated value as long
     * @return the aggregated value in current data type
     */
    Object output(long value);

    /**
     * The data type of the aggregated value
     * 
     * @return the data type of the aggregated value
     */
    DataType outputType();
}
