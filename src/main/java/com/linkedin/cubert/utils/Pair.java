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

package com.linkedin.cubert.utils;

import java.io.Serializable;

public class Pair<A, B> implements Serializable
{
    private static final long serialVersionUID = -1868433840993562692L;
    private A first;
    private B second;

    public Pair(A x, B y)
    {
        first = x;
        second = y;
    }

    public int hashCode()
    {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    public boolean equals(Object other)
    {
        if (other instanceof Pair)
        {
            @SuppressWarnings("unchecked")
            Pair<A, B> otherPair = (Pair<A, B>) other;
            return ((this.first == otherPair.first || (this.first != null
                    && otherPair.first != null && this.first.equals(otherPair.first))) && (this.second == otherPair.second || (this.second != null
                    && otherPair.second != null && this.second.equals(otherPair.second))));
        }

        return false;
    }

    public String toString()
    {
        return "(" + first + ", " + second + ")";
    }

    public A getFirst()
    {
        return first;
    }

    public void setFirst(A first)
    {
        this.first = first;
    }

    public B getSecond()
    {
        return second;
    }

    public void setSecond(B second)
    {
        this.second = second;
    }
}
