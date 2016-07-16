package org.learn.akka.store;

import com.datastax.driver.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test to measure the performance of different sync and async operations.
 * Created by abhiso on 6/14/16.
 */
@Component
public class CassandraStore {

    /** C* cluster */
    private Cluster cluster;

    /** C* session */
    private Session session;

    @Value("${cassandra.contactPoints}")
    private String contactPoints;

    @Value("${cassandra.keyspace}")
    private String keyspace;

    /** caching prepared statements. */
    private Map<String, PreparedStatement> statementMap = new ConcurrentHashMap<>();

    public CassandraStore() {
        String[] cp = contactPoints.split(",");
        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(cp)
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
                .withPort(9042);
        cluster = builder.build();
        session = cluster.connect(keyspace);
    }

    /**
     * get the prepared statement
     * @param sql sql string
     * @return prepared statement
     */
    private PreparedStatement getStatement(String sql) {
        Optional<PreparedStatement> statement = Optional.ofNullable(statementMap.get(sql));
        if (!statement.isPresent()) {
            statementMap.put(sql, session.prepare(sql));
            return statementMap.get(sql);
        }
        return statement.get();
    }

    /**
     * perform the sync operation
     * @param cql sql to be executed
     * @param args args for the cql statment
     */
    public void sync(String cql, Object[] args) {
        BoundStatement boundStatement = new BoundStatement(getStatement(cql));
        boundStatement.bind(args);
        session.execute(boundStatement);
    }

    /**
     * async operation
     * @param cql cql to be executed
     * @param args args to be inserted
     */
    public ResultSetFuture async(String cql, Object[] args) {
        BoundStatement boundStatement = new BoundStatement(getStatement(cql));
        boundStatement.bind(args);
        return session.executeAsync(boundStatement);
    }

    public void setContactPoints(String contactPoints) {
        this.contactPoints = contactPoints;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }
}
