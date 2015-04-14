package DBManager;

import DataLoader.FileDataLoader;

import java.sql.*;

/**
 * Created by ngoctranfire on 4/9/15.
 */
public class DBManager {

    private Statement myStatement;
    public DBManager(Statement statement) {
        this.myStatement = statement;
    }

    public void createLibraryTables() {
        try
        {
            myStatement.executeUpdate ("CREATE TABLE category " +
                    "(cid NUMBER(1) NOT NULL, max NUMBER(2) NOT NULL, period NUMBER(2) NOT NULL, PRIMARY KEY (cid))");

            myStatement.executeUpdate ("CREATE TABLE libuser " +
                    "(libuid varchar2(10) NOT NULL, name varchar2(25) NOT NULL, address varchar2(100) NOT NULL, cid NUMBER(1) NOT NULL, " +
                    "PRIMARY KEY (libuid), FOREIGN KEY (cid) REFERENCES category(cid))");

            myStatement.executeUpdate ("CREATE TABLE book " +
                    "(callnum varchar2(8) NOT NULL, title varchar2(30) NOT NULL, publish DATE NOT NULL, " +
                    "PRIMARY KEY (callnum))");

            myStatement.executeUpdate ("CREATE TABLE copy " +
                    "(callnum varchar2(8) NOT NULL, copynum NUMBER(1) NOT NULL, " +
                    "PRIMARY KEY (callnum, copynum))");

            myStatement.executeUpdate ("CREATE TABLE borrow" +
                    "(libuid varchar2(10) NOT NULL, callnum varchar2(8) NOT NULL, copynum NUMBER(1) NOT NULL, checkout DATE NOT NULL," +
                    "return DATE, " + "PRIMARY KEY (libuid, callnum, copynum, checkout), " +
                    "FOREIGN KEY (libuid) REFERENCES libuser(libuid) ON DELETE CASCADE, " +
                    "FOREIGN KEY (callnum) REFERENCES book(callnum) ON DELETE CASCADE)");

            myStatement.executeUpdate ("CREATE TABLE AUTHORSHIP " +
                    "(aname varchar2(25) NOT NULL, callnum varchar2(8) NOT NULL, " +
                    "PRIMARY KEY (aname, callnum), " + "FOREIGN KEY (callnum) REFERENCES book (callnum))");



            System.out.println("Successfully printed all the tables requested!");
        }
        catch (Exception e )
        {
            e.printStackTrace();
            System.out.println("Error failed to create the tables requested!");
        }
    }

    public void dropAllTables() {
        System.out.println("Beginning dropping all tables!");
        try {
            PreparedStatement preStatement = myStatement.getConnection().prepareStatement("SELECT table_name FROM user_tables");
            ResultSet tableNames = preStatement.executeQuery();

            while(tableNames.next()){
                String tableName = tableNames.getString(1);
                myStatement.executeUpdate("DROP TABLE " + tableName + " CASCADE CONSTRAINT");
            }
            System.out.println("Dropped all the tables necessary!");
        } catch (SQLException e) {
            System.out.println("There was an error dropping your tables");
            e.printStackTrace();
        }

    }

    public Connection getConnection() {
        try {
            return myStatement.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void outputTablesSummary() {
        System.out.println("Number of records in each table:");

        try {
            PreparedStatement preStatement = myStatement.getConnection().prepareStatement("SELECT table_name FROM user_tables");
            ResultSet tableNames = preStatement.executeQuery();
            while(tableNames.next()){
                String tableName = tableNames.getString(1);
                ResultSet countResult = myStatement.executeQuery("SELECT COUNT(1) FROM " + tableName);
                countResult.next();
                System.out.println("Table " + tableName + ": " + countResult.getInt(1));
            }
            System.out.println("");
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public void insertFileData(String sourcePath) {
        FileDataLoader dataLoader = new FileDataLoader(getConnection(), sourcePath);
        try {
            dataLoader.insertData();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
