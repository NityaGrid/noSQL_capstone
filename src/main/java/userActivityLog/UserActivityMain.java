package userActivityLog;

import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class UserActivityMain {
    public static void main(String[] args) {
        UserActivityService logger = new UserActivityService("127.0.0.1", "user_activity_keyspace");
        UUID userId = UUID.randomUUID();
        Instant baseTime = Instant.now();

        Object[][] activities = {
                {"user_login", 0L},
                {"access_dashboard", -30L},
                {"change_profile_photo", -90L},
                {"send_chat_message", -120L},
                {"react_to_post", -150L},
                {"post_comment", -180L},
                {"check_notifications", -210L},
                {"user_logout", -240L},
                {"make_purchase", -300L},
                {"add_new_friend", -360L},
                {"follow_an_account", -420L},
                {"modify_settings", -480L},
                {"find_content", -540L},
                {"reset_password", -600L},
                {"activate_2fa", -660L}
        };


        for (Object[] activity : activities) {
            long offset = ((Number) activity[1]).longValue();
            Instant timestamp = baseTime.plusSeconds(offset);
            logger.logUserActivity(
                    userId,
                    (String) activity[0],
                    timestamp,
                    30
            );
        }
        UUID anotherUserId = UUID.randomUUID();
        logger.logUserActivity(anotherUserId, "login", baseTime.minusSeconds(600), 30);
        logger.logUserActivity(anotherUserId, "view_dashboard", baseTime.minusSeconds(590), 30);
        System.out.println("\nRecent activities for main user:");
        List<Row> recentActivities = logger.getRecentUserActivities(userId, 5);
        for (Row row : recentActivities) {
            System.out.printf("user_id: %s, activity_id: %s, activity_timestamp: %s, activity_type: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getInstant("activity_timestamp"),
                    row.getString("activity_type"));
        }
        System.out.println("\nRecent activities for another user:");
        List<Row> anotherUserActivities = logger.getRecentUserActivities(anotherUserId, 5);
        for (Row row : anotherUserActivities) {
            System.out.printf("user_id: %s, activity_id: %s, activity_timestamp: %s, activity_type: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getInstant("activity_timestamp"),
                    row.getString("activity_type"));
        }
        Instant startTime = baseTime.minusSeconds(300);
        Instant endTime = baseTime.plusSeconds(60);
        System.out.println("\nActivities for main user within time range:");
        List<Row> activitiesInRange = logger.getUserActivitiesWithinRange(userId, startTime, endTime);
        for (Row row : activitiesInRange) {
            System.out.printf("user_id: %s, activity_id: %s, activity_timestamp: %s, activity_type: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getInstant("activity_timestamp"),
                    row.getString("activity_type"));
        }


        logger.close();
    }
}
