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

package com.linkedin.cubert.operator;

public class PreconditionException extends Exception
{
    private static final long serialVersionUID = 4455297080955040436L;
    private PreconditionExceptionType exceptionType;
    private String message;

    public PreconditionException(PreconditionExceptionType exceptionType)
    {
        this.exceptionType = exceptionType;
    }

    public PreconditionException(PreconditionExceptionType exceptionType, String message)
    {
        this.exceptionType = exceptionType;
        this.message = message;
    }

    public PreconditionExceptionType getExceptionType()
    {
        return exceptionType;
    }

    public String getMessage()
    {
        return message;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("PreconditionException [")
               .append(exceptionType)
               .append("] ")
               .append(message);
        return builder.toString();
    }

}
