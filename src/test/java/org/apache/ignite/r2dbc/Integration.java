package org.apache.ignite.r2dbc;

import io.r2dbc.spi.Row;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class Integration {

    @Test
    public void test() {
        Ignite igniteServer = Ignition.start();

        Ignite client = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("client").setClientMode(true));

        // hack!
        IgniteCache<Object, Object> test = client.getOrCreateCache("test");

        ClientWrapper clientWrapper = new ClientWrapper(client);

        IgniteConnection igniteConnection = new IgniteConnection(clientWrapper);

        igniteConnection.createStatement("CREATE TABLE City (id int primary key, name varchar, region varchar) WITH \"ATOMICITY=TRANSACTIONAL_SNAPSHOT\"").execute().collectList().block();

        igniteConnection.beginTransaction().block();

        igniteConnection
                .createStatement("INSERT INTO City(id, name, region) VALUES(?, ?, ?)")
                .bind("?1", 1)
                .bind("?2", "Saint-Petersburg")
                .bind("?3", "North-West")
                .execute().blockFirst();

        igniteConnection.commitTransaction().block();

        BinaryObject city = (BinaryObject) (igniteServer).cache("SQL_PUBLIC_CITY").withKeepBinary().get(1);

        Assertions.assertEquals("Saint-Petersburg", city.<String>field("name"));

        igniteConnection.rollbackTransaction().block();

        igniteConnection.beginTransaction().block();

        igniteConnection
                .createStatement("INSERT INTO City(id, name, region) VALUES(?, ?, ?)")
                .bind("?1", 2)
                .bind("?2", "Moscow")
                .bind("?3", "Center")
                .execute().blockFirst();

        List<IgniteResult> resultBeforeRollback = igniteConnection.createStatement("SELECT * FROM City").execute().collectList().block();

        List<Row> beforeRollback = resultBeforeRollback.get(0).map((row, rowMetadata) -> row).collectList().block();

        Assertions.assertEquals("Moscow", beforeRollback.get(1).get("name"));

        igniteConnection.rollbackTransaction().block();

        List<IgniteResult> afterRollback = igniteConnection.createStatement("SELECT * FROM City").execute().collectList().block();

        Assertions.assertEquals(1, afterRollback.size());
    }

}
