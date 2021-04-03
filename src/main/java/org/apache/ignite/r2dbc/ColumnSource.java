package org.apache.ignite.r2dbc;

import org.jetbrains.annotations.Nullable;

import java.util.*;

public class ColumnSource {

    private final List<IgniteColumnMetadata> columns;

    private final Map<String, IgniteColumnMetadata> nameKeyedColumns;

    ColumnSource(List<IgniteColumnMetadata> columns) {
        this.columns = columns;
        this.nameKeyedColumns = getNameKeyedColumns(columns);
    }

    private static Map<String, IgniteColumnMetadata> getNameKeyedColumns(List<IgniteColumnMetadata> columns) {

        if (columns.size() == 1) {
            return Collections.singletonMap(columns.get(0).getName(), columns.get(0));
        }

        Map<String, IgniteColumnMetadata> byName = new LinkedHashMap<>(columns.size(), 1);

        for (IgniteColumnMetadata column : columns) {
            IgniteColumnMetadata old = byName.put(column.getName(), column);
            if (old != null) {
                byName.put(column.getName(), old);
            }
        }

        return byName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnSource)) {
            return false;
        }
        ColumnSource that = (ColumnSource) o;
        return Objects.equals(columns, that.columns) &&
                Objects.equals(nameKeyedColumns, that.nameKeyedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, nameKeyedColumns);
    }

    List<IgniteColumnMetadata> getColumnMetadatas() {
        return this.columns;
    }

    int getColumnCount() {
        return this.columns.size();
    }

    /**
     * Lookup {@link IgniteColumnMetadata} by its {@code index}.
     *
     * @param index the column index. Must be greater zero and less than the number of columns.
     * @return the {@link IgniteColumnMetadata}.
     */
    IgniteColumnMetadata getColumn(int index) {

        if (this.columns.size() > index && index >= 0) {
            return this.columns.get(index);
        }

        throw new IllegalArgumentException(String.format("Column index %d is larger than the number of columns %d", index, this.columns.size()));
    }

    /**
     * Lookup {@link IgniteColumnMetadata} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link IgniteColumnMetadata}.
     */
    IgniteColumnMetadata getColumn(String name) {

        Objects.requireNonNull(name, "name must not be null");

        IgniteColumnMetadata column = findColumn(name);

        if (column == null) {
            throw new IllegalArgumentException(String.format("Column name '%s' does not exist in column names %s", name.toUpperCase(), this.nameKeyedColumns.keySet()));
        }

        return column;
    }

    /**
     * Lookup {@link IgniteColumnMetadata} by its {@code name}.
     *
     * @param name the column name.
     * @return the {@link IgniteColumnMetadata}.
     */
    @Nullable
    IgniteColumnMetadata findColumn(String name) {

        IgniteColumnMetadata column = this.nameKeyedColumns.get(name);

        if (column == null) {
            name = getColumnName(name, this.nameKeyedColumns.keySet());
            if (name != null) {
                column = this.nameKeyedColumns.get(name);
            }
        }

        return column;
    }

    @Nullable
    private static String getColumnName(String name, Collection<String> names) {

        for (String s : names) {
            if (s.equalsIgnoreCase(name)) {
                return s;
            }
        }

        return null;
    }

}
