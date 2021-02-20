package br.com.cesar.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

    private final Connection connection;

    public LocalDatabase(String fullPath) throws SQLException {
        var url = "jdbc:sqlite:" + fullPath + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean update(String statement, String... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(statement, params);
        return preparedStatement.execute();
    }

    public ResultSet query(String query, String... params) throws SQLException {
        PreparedStatement preparedStatement = prepare(query, params);
        return preparedStatement.executeQuery();
    }

    private PreparedStatement prepare(String statement, String... params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }
}
