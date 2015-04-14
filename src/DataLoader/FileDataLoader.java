package DataLoader;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;

/**
 * Created by ngoctranfire on 4/10/15.
 */
public class FileDataLoader {

    public static final String CATEGORY_PATH = "category.txt";
    public static final String LIBUSER_PATH = "user.txt";
    public static final String BOOK_PATH = "book.txt";
    public static final String CHECKOUT_PATH = "check_out.txt";
    public static final String FILE_SEPARATOR = "/";
    public static final String TAB_DELIMITER = "\t";
    public static final String COMMA_SEPARATOR= ",";
    public static final String NULL_STRING = "null";
    private Connection myConnection;
    private String mySourcePath;

    public FileDataLoader(Connection connection, String sourcePath) {
        this.myConnection = connection;
        this.mySourcePath = sourcePath;
    }

    public void insertData() throws SQLException {
        readCategoryData();
        readUserData();
        readBookAuthorData();
        readCheckoutData();
    }

    private void readCategoryData() throws SQLException {
        try {
            Scanner categoryFile = new Scanner(new File(mySourcePath + FILE_SEPARATOR + CATEGORY_PATH));
            try {
                PreparedStatement preparedStatement = myConnection.prepareStatement("INSERT INTO category VALUES(?, ? , ?)");
                while (categoryFile.hasNextLine()) {
                    String[] results = categoryFile.nextLine().split(TAB_DELIMITER);
                    Integer cid = Integer.parseInt(results[0]);
                    Integer max = Integer.parseInt(results[1]);
                    Integer period = Integer.parseInt(results[2]);

                    preparedStatement.setInt(1, cid);
                    preparedStatement.setInt(2, max);
                    preparedStatement.setInt(3, period);

                    preparedStatement.executeUpdate();

                }
                categoryFile.close();
                System.out.println("Successfully inserted category data!");
            } catch (SQLException e) {
                myConnection.rollback();
                System.out.println("Error reading category data file into the prepared statement!");
                e.printStackTrace();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }




    }
    private void readUserData() throws SQLException {
        try {
            Scanner userFile = new Scanner(new File(mySourcePath + FILE_SEPARATOR + LIBUSER_PATH));

            try {
                PreparedStatement preparedStatement = myConnection.prepareStatement("INSERT INTO libuser VALUES(?, ? , ?, ?)");
                while (userFile.hasNextLine()) {
                    String[] results = userFile.nextLine().split(TAB_DELIMITER);
                    String uid = results[0];
                    String name = results[1];
                    String address = results[2];
                    Integer cid = Integer.parseInt(results[3]);

                    preparedStatement.setString(1, uid);
                    preparedStatement.setString(2, name);
                    preparedStatement.setString(3, address);
                    preparedStatement.setInt(4, cid);

                    preparedStatement.executeUpdate();

                }
                System.out.println("Successfully inserted user data!");
                userFile.close();
            } catch (SQLException e) {
                myConnection.rollback();
                System.out.println("Error reading user data file into the prepared statement!");
                e.printStackTrace();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }



    }

    private void readBookAuthorData() throws SQLException{

        try {
            Scanner bookFile = new Scanner(new File(mySourcePath + FILE_SEPARATOR + BOOK_PATH));
            try{
                PreparedStatement preparedStatement = myConnection.prepareStatement(
                        "INSERT INTO book VALUES(?, ?, TO_DATE(?, 'DD/MM/YYYY'))");
                while(bookFile.hasNextLine()) {
                    String[] results = bookFile.nextLine().split(TAB_DELIMITER);
                    String callNumber = results[0];
                    String bookName = results[2];
                    String publish = results[4];
                    String authors = results[3];

                    preparedStatement.setString(1, callNumber);
                    preparedStatement.setString(2, bookName);


                    preparedStatement.setString(3, publish);
                    preparedStatement.executeUpdate();

                    //Authorship time
                    try {
                        String [] resultsAuthors = authors.split(COMMA_SEPARATOR);
                        for (String resultsAuthor : resultsAuthors) {
                            PreparedStatement preparedStatements2 = myConnection.prepareStatement("INSERT INTO AUTHORSHIP VALUES(?, ?)");
                            preparedStatements2.setString(1, resultsAuthor);
                            preparedStatements2.setString(2, callNumber);
                            preparedStatements2.executeUpdate();
                        }
//                        System.out.println("Successsfully inserted Author Data!");
                    } catch (Exception e) {
                        System.out.println("Error trying to read into the author!");
                    }

                    //Fill out Book Copies Time
                    Integer numberOfCopies = Integer.parseInt(results[1]);

                    for(int i = 0; i < numberOfCopies; i++) {
                        PreparedStatement preparedStatement3 = myConnection.prepareStatement("INSERT INTO copy VALUES(?, ?)");
                        preparedStatement3.setString(1, callNumber);
                        preparedStatement3.setInt(2, i);
                        preparedStatement3.executeUpdate();
                    }

                }
                System.out.println("Successfully inserted book data!");
                bookFile.close();
            } catch (Exception e) {
                myConnection.rollback();
                System.out.println("Error reading book data file into the prepared statement!");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
//            e.printStackTrace();
        }

    }

    private void readCheckoutData() throws SQLException {
        try {
            Scanner checkoutFile = new Scanner(new File(mySourcePath + FILE_SEPARATOR + CHECKOUT_PATH));

            try {
                PreparedStatement preparedStatement = myConnection.
                        prepareStatement("INSERT INTO borrow VALUES(?, ?, ?, TO_DATE(?, 'DD/MM/YYYY'), TO_DATE(?, 'DD/MM/YYYY'))");
                while(checkoutFile.hasNextLine()) {
                    String [] lineResults = checkoutFile.nextLine().split(TAB_DELIMITER);
                    String userId = lineResults[0];
                    String callNumber = lineResults[1];
                    Integer copyNumber = Integer.parseInt(lineResults[2]);
                    String checkoutDate = lineResults[3];

                    preparedStatement.setString(1, userId);
                    preparedStatement.setString(2, callNumber);
                    preparedStatement.setInt(3, copyNumber);
                    preparedStatement.setString(4, checkoutDate);
                    String returnDate = lineResults[4];

                    if(returnDate.toLowerCase().equals(NULL_STRING)) {
                        preparedStatement.setNull(5, Types.VARCHAR);
                    } else {
                        preparedStatement.setString(5, returnDate);
                    }

                    preparedStatement.executeUpdate();
                }
//                System.out.println("Successfully inserted checkout data!");
                checkoutFile.close();

            } catch(Exception e) {
                System.out.println("Error reading checkout data file into the prepared statement");
                e.printStackTrace();
                myConnection.rollback();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

}
