package org.apache.ignite.r2dbc;

import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A collection of {@link Object}s for a single bind invocation of an {@link ClientWrapper}.
 */
public final class Binding {

    static final Binding EMPTY = new Binding();

    private final SortedMap<Integer, Object> parameters = new TreeMap<>();

    /**
     * Add a {@link Object} to the binding.
     *
     * @param index the index of the {@link Object}
     * @param value the {@link Object}
     * @return this {@link Binding}
     * @throws NullPointerException if {@code index} or {@code parameter} is {@code null}
     */
    public Binding add(Integer index, Object value) {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(value, "value must not be null");

        this.parameters.put(index, value);

        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Binding)) {
            return false;
        }
        Binding that = (Binding) o;
        return Objects.equals(this.parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.parameters);
    }

    @Override
    public String toString() {
        return "Binding{" +
                "parameters=" + this.parameters +
                '}';
    }

    SortedMap<Integer, Object> getParameters() {
        return this.parameters;
    }
}
