package org.apache.ignite.r2dbc;

import io.r2dbc.spi.Row;

import java.util.List;

public class IgniteRow implements Row {

    private final IgniteRowMetadata rowMetadata;

    private final List<Object> cols;

    public IgniteRow(final IgniteRowMetadata rowMetadata, final List<Object> cols) {
        this.rowMetadata = rowMetadata;
        this.cols = cols;
    }

    public static IgniteRow toRow(final List<Object> values, final IgniteRowMetadata rowMetadata) {
        return new IgniteRow(rowMetadata, values);
    }


    @Override
    public <T> T get(final int index, final Class<T> type) {
        //noinspection unchecked
        return (T) cols.get(index);
    }

    @Override
    public <T> T get(final String name, final Class<T> type) {
        //noinspection unchecked
        return (T) cols.get(rowMetadata.getColumn(name).getIndex());
    }
}
