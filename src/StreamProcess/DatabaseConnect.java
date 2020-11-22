package StreamProcess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

final class DatabaseConnect {

    private Connection connection;
    private Statement statement;

    DatabaseConnect(){
        String url = "jdbc:postgresql://localhost/postgres";
        Properties props = new Properties();
        props.setProperty("user","postgres");
        props.setProperty("password","Australia@89");
        //props.setProperty("ssl","true");
        try {
            this.connection = DriverManager.getConnection(url, props);
            this.connection.setAutoCommit(false);
            this.statement = this.connection.createStatement();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public Connection getConnection(){
        return this.connection;
    }

    public void close(){
        try {
            this.connection.close();
            this.statement.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void executeQuery(String query){
        try {
            this.statement.execute(query);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void commitData(){
        try {
            this.connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
