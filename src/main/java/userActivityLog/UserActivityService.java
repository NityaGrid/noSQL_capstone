package userActivityLog;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class UserActivityService {
    private final CqlSession session;
    private final PreparedStatement insertActivityStatement;
    private final PreparedStatement fetchRecentActivitiesStatement;
    private final PreparedStatement fetchActivitiesInRangeStatement;

    public UserActivityService(String contactPoint, String keyspace) {
        this.session = CqlSession.builder()
                .addContactPoint(InetSocketAddress.createUnresolved(contactPoint, 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
                "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}");
        session.execute("USE " + keyspace);
        session.execute("CREATE TABLE IF NOT EXISTS user_activity_log (" +
                "user_id UUID, " +
                "activity_timestamp TIMESTAMP, " +
                "activity_id UUID, " +
                "activity_type TEXT, " +
                "PRIMARY KEY ((user_id), activity_timestamp, activity_id)) " +
                "WITH CLUSTERING ORDER BY (activity_timestamp DESC, activity_id ASC)");

        this.insertActivityStatement = session.prepare("INSERT INTO user_activity_log " +
                "(user_id, activity_timestamp, activity_id, activity_type) " +
                "VALUES (?, ?, ?, ?) USING TTL ?");

        this.fetchRecentActivitiesStatement = session.prepare("SELECT * FROM user_activity_log " +
                "WHERE user_id = ? LIMIT ?");

        this.fetchActivitiesInRangeStatement = session.prepare("SELECT * FROM user_activity_log " +
                "WHERE user_id = ? AND activity_timestamp >= ? AND activity_timestamp <= ?");
    }

    public void logUserActivity(UUID userId, String activityType, Instant activityTimestamp, int ttlDays) {
        BoundStatement statement = insertActivityStatement.bind()
                .setUuid(0, userId)
                .setInstant(1, activityTimestamp)
                .setUuid(2, UUID.randomUUID())
                .setString(3, activityType)
                .setInt(4, ttlDays * 86400)
                .setConsistencyLevel(ConsistencyLevel.ONE);

        session.execute(statement);
    }

    public List<Row> getRecentUserActivities(UUID userId, int limit) {
        BoundStatement statement = fetchRecentActivitiesStatement.bind()
                .setUuid(0, userId)
                .setInt(1, limit)
                .setConsistencyLevel(ConsistencyLevel.ONE);

        return session.execute(statement).all();
    }

    public List<Row> getUserActivitiesWithinRange(UUID userId, Instant start, Instant end) {
        BoundStatement statement = fetchActivitiesInRangeStatement.bind()
                .setUuid(0, userId)
                .setInstant(1, start)
                .setInstant(2, end)
                .setConsistencyLevel(ConsistencyLevel.ONE);

        return session.execute(statement).all();
    }

    public void close() {
        session.close();
    }
}
