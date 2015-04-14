package DBConnector;

import java.sql.*;

/**
 * Created by ngoctranfire on 4/9/15.
 */
public class DBConnectorInitializer {

    public static final String ORACLE_DRIVER= "oracle.jdbc.driver.OracleDriver";

    private Connection myConnection;

    public DBConnectorInitializer(String user, String pass) {
        try
        {
            Class.forName(ORACLE_DRIVER);

        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            try {
                DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            } catch (SQLException e1) {
                System.out.println("Unable to load the driver class!");
            }
        }

        try {
            myConnection = DriverManager.getConnection("jdbc:oracle:thin:@db12.cse.cuhk.edu.hk:1521:db12", user, pass);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public Statement getStatement(){
        try {
            return myConnection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error! It did not actually get the statement for some reason!");
            return null;
        }
    }


}
