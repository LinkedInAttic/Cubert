package com.linkedin.cubert.utils;

import com.linkedin.cubert.block.BlockSchema;
import com.linkedin.cubert.block.ColumnType;
import com.linkedin.cubert.block.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**
 * @author Mani Parkhe
 */

public class TupleCopier
{
    private final Copier copier;
    private final BlockSchema schema;

    public TupleCopier(BlockSchema schema)
    {
        this.schema = schema;

        if (schemaSupportShallowCopy(schema))
        {
            copier = new ShallowCopier();
        }
        else if (schemaSupportsDeepCopyWithReuse(schema))
        {
            copier = new DeepReuseCopier();
        }
        else
        {
            copier = new DeepCopier();
        }
    }

    interface Copier
    {
        public void copy(Tuple src, Tuple dest)
            throws ExecException;

        public Tuple initializeOutput(BlockSchema schema)
            throws ExecException;

    }

    class ShallowCopier implements Copier
    {
        @Override
        public void copy(Tuple src, Tuple dest)
            throws ExecException
        {
            TupleUtils.copy(src, dest);
        }

        @Override
        public Tuple initializeOutput(BlockSchema schema)
        {
            return TupleFactory.getInstance().newTuple(schema.getNumColumns());
        }
    }

    class DeepCopier implements Copier
    {
        @Override
        public void copy(Tuple src, Tuple dest)
            throws ExecException
        {
            TupleUtils.deepCopy(src, dest);
        }

        @Override
        public Tuple initializeOutput(BlockSchema schema)
        {
            return TupleFactory.getInstance().newTuple(schema.getNumColumns());
        }
    }

    class DeepReuseCopier implements Copier
    {
        @Override
        public void copy(Tuple src, Tuple dest)
            throws ExecException
        {
            TupleUtils.deepCopyWithReuse(src, dest);
        }

        @Override
        public Tuple initializeOutput(BlockSchema schema)
            throws ExecException
        {
            Tuple tuple = TupleFactory.getInstance().newTuple(schema.getNumColumns());

            int idx = 0;
            for (ColumnType ct : schema.getColumnTypes())
            {
                DataType type = ct.getType();
                if (type == DataType.RECORD || type == DataType.TUPLE)
                {
                    tuple.set(idx, TupleFactory.getInstance().newTuple(ct.getColumnSchema().getNumColumns()));
                }

                idx++;
            }

            return tuple;
        }
    }

    public Tuple newTuple()
        throws ExecException
    {
        return copier.initializeOutput(schema);
    }

    public void copy(Tuple src, Tuple dest)
        throws ExecException
    {
        copier.copy(src, dest);
    }

    private boolean schemaSupportsDeepCopyWithReuse(BlockSchema schema)
    {
        for (ColumnType ct : schema.getColumnTypes())
        {
            DataType type = ct.getType();

            if (type.allowShallowCopy())
            {
                // Numeric, String or Enum type
                continue;
            }
            else if (type == DataType.RECORD || type == DataType.TUPLE)
            {
                if (schemaSupportsDeepCopyWithReuse(ct.getColumnSchema()))
                {
                    continue;
                }
                else
                {
                    return false;
                }
            }

            // Everything else
            return false;
        }

        return true;
    }

    /**
     * @param schema
     * @return true if all columns are either numeric, string or enum type.
     */
    private boolean schemaSupportShallowCopy(BlockSchema schema)
    {
        for (ColumnType ct : schema.getColumnTypes())
        {
            if (ct.getType().allowShallowCopy())
            {
                continue;
            }

            return false;
        }

        return true;
    }

}
