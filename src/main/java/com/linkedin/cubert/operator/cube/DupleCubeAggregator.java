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

/**
 * Interface that specifies aggregators for the CUBE operator that can rollup inner
 * dimensions.
 * <p>
 * Specifically, this aggregator applies for the cases where the grain of the data is
 * greater as the grain of the CUBE. In other words, there are "inner dimensions" that
 * need to be rolled up before computing the cube.
 * <p>
 * This interface extends the {@link CubeAggregator} interface, and specified one extra
 * method:
 * <ul>
 * <li>{@link innerAggregate}: aggregate values for inner dimension for the specified
 * value cuboid.</li>
 * </ul>
 * 
 * @see CubeAggregator
 * @author Maneesh Varshney
 * 
 */
public interface DupleCubeAggregator extends CubeAggregator
{
    /**
     * Aggregate value for inner dimensions for the specified value cuboid.
     * 
     * @param index
     *            the index for the value cuboid.
     */
    void innerAggregate(int index);
}
