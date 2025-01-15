package com.ccs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class BatchInsert {
    String jdbcURL = "jdbc:postgresql://localhost:5432/mohithdev";
    String username = "postgres";
    String password = "postgres";
    String insertSQL = "INSERT INTO loyalty_points (user_id, points_earned) VALUES (?, ?)";
    String updateSQL = "UPDATE loyalty_points SET points_earned = ? where user_id = ?";
    String selectSQL = "SELECT points_earned from loyalty_points where user_id=?";

    PreparedStatement insertStatement = null;
    PreparedStatement updateStatement = null;

    public void insert(List<LoyaltyPoints> list) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcURL, username, password)) {
            connection.setAutoCommit(false);
            insertStatement = connection.prepareStatement(insertSQL);
            updateStatement = connection.prepareStatement(updateSQL);

            // Add records to the batch
            for (LoyaltyPoints loyaltyPoints : list) {
                PreparedStatement stmt = connection.prepareStatement(selectSQL);
                stmt.setString(1, loyaltyPoints.getUserId());
                ResultSet rs = stmt.executeQuery();
                long pointsEarned = 0;
                while (rs.next()) {
                    pointsEarned = rs.getLong("points_earned");
                }

                if (pointsEarned > 0) {
                    updateStatement.setLong(1, pointsEarned + loyaltyPoints.getPointsEarned());
                    updateStatement.setString(2, loyaltyPoints.getUserId());
                    updateStatement.addBatch();
                    updateStatement.executeBatch();
                } else {
                    insertStatement.setString(1, loyaltyPoints.getUserId());
                    insertStatement.setLong(2, loyaltyPoints.getPointsEarned());
                    // Add the insert, update statements to the batch
                    insertStatement.addBatch();
                    // Execute batch every 10 records to DB
                    insertStatement.executeBatch();
                }
            }

            // Commit the transaction
            connection.commit();
            System.out.println("Batch insert completed successfully!");

            // Keep hold of the lastly inserted record.
            // This will be helpful if the machine turns off abruptly, we use this deserialized object and continue
            // processing next from there.
            LoyaltyPoints.serialize(list.get(list.size() - 1));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
