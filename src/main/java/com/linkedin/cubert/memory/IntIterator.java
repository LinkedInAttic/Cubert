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

package com.linkedin.cubert.memory;

/**
 * An iterator for integer values that does not box them to Integer objects.
 * <p>
 * This interface is a subset of {@link java.util.Iterator}; most prominently,
 * {@code remove()} method is supported.
 * 
 * @author Maneesh Varshney
 * 
 */
public interface IntIterator
{
    /**
     * Returns true if there are more integers available.
     * 
     * @return true if there are more integers available.
     * 
     */
    boolean hasNext();

    /**
     * Returns the next integer.
     * 
     * @return next integer
     */
    int next();
}
