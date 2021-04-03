package org.apache.ignite.r2dbc;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;

import java.util.*;

public class IgniteRowMetadata extends ColumnSource implements RowMetadata, Collection<String> {

    IgniteRowMetadata(List<IgniteColumnMetadata> columnMetadatas) {
        super(Objects.requireNonNull(columnMetadatas, "columnMetadatas must not be null"));
    }

    @Override
    public ColumnMetadata getColumnMetadata(int index) {
        return getColumn(index);
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
        return getColumn(name);
    }

    @Override
    public List<IgniteColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(super.getColumnMetadatas());
    }

    static IgniteRowMetadata toRowMetadata(QueryCursorImpl<List<?>> result) {
        Objects.requireNonNull(result, "result must not be null");

        return new IgniteRowMetadata(getColumnMetadatas(result));
    }

    private static List<IgniteColumnMetadata> getColumnMetadatas(QueryCursorImpl<List<?>> result) {
        List<IgniteColumnMetadata> columnMetadatas = new ArrayList<>(result.getColumnsCount());

        for (int i = 0; i < result.getColumnsCount(); i++) {
            columnMetadatas.add(IgniteColumnMetadata.toColumnMetadata(result, i));
        }

        return columnMetadatas;
    }

    @Override
    public Collection<String> getColumnNames() {
        return this;
    }

    @Override
    public int size() {
        return this.getColumnCount();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {

        if (o instanceof String) {
            return this.findColumn((String) o) != null;
        }

        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {

        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Iterator<String> iterator() {

        Iterator<IgniteColumnMetadata> iterator = super.getColumnMetadatas().iterator();

        return new Iterator<String>() {


            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().getName();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return (T[]) toArray();
    }

    @Override
    public Object[] toArray() {
        Object[] result = new Object[size()];

        for (int i = 0; i < size(); i++) {
            result[i] = this.getColumn(i).getName();
        }

        return result;
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

}
