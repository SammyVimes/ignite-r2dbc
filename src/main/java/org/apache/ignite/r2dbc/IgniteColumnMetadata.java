package org.apache.ignite.r2dbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

import java.sql.ResultSetMetaData;
import java.util.List;

public class IgniteColumnMetadata implements ColumnMetadata {

    private final String name;

    private final int idx;

    private final int scale;

    private final int precision;

    private final Class<?> type;

    private final int nullability;

    public IgniteColumnMetadata(final String name, final int idx, final int scale, final int precision, final Class<?> type, final int nullability) {
        this.name = name;
        this.idx = idx;
        this.scale = scale;
        this.precision = precision;
        this.type = type;
        this.nullability = nullability;
    }


    @Override
    public Class<?> getJavaType() {
        return type;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public int getIndex() {
        return this.idx;
    }

    @Override
    public Nullability getNullability() {
        switch (nullability) {
            case ResultSetMetaData.columnNoNulls:
                return Nullability.NON_NULL;
            case ResultSetMetaData.columnNullable:
                return Nullability.NULLABLE;
            case ResultSetMetaData.columnNullableUnknown:
            default:
                return Nullability.UNKNOWN;
        }
    }

    @Override
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    public static IgniteColumnMetadata toColumnMetadata(final QueryCursorImpl<List<?>> result, final int i) {
        String fieldName = result.getFieldName(i);
        GridQueryFieldMetadata gridQueryFieldMetadata = result.fieldsMeta().get(i);
        int nullability = gridQueryFieldMetadata.nullability();
        int scale = gridQueryFieldMetadata.scale();
        int precision = gridQueryFieldMetadata.precision();
        String typeName = gridQueryFieldMetadata.fieldTypeName();
        Class<?> type = null;
        try {
            type = Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            type = null;
        }
        return new IgniteColumnMetadata(fieldName, i, scale, precision, type, nullability);
    }
}
