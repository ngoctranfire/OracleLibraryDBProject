package DBManager;


import javax.xml.transform.Result;
import java.sql.*;

/**
 * Created by ngoctranfire on 4/10/15.
 */
public class QueryManager {

    private Connection myConnection;

    public QueryManager(Connection connection) {
        this.myConnection = connection;
    }

    public void queryBookCallNumber(String callNumber) {
        String whereClause = "WHERE BOOK.callnum='" + callNumber + "'";
        queryBook(whereClause);
    }

    public void queryBookTitle(String titlePartial) {
        String whereClause = "WHERE BOOK.title LIKE " + "'%" + titlePartial + "%'";
        queryBook(whereClause);
    }
    public void queryUserRecord(String userId) {

        try {
            PreparedStatement preparedStatement = myConnection.prepareStatement("select BORROW.callnum, BORROW.copynum, " +
                    "BOOK.title, BORROW.checkout, BORROW.return from BOOK, BORROW, AUTHORSHIP " +
                    "where BORROW.libuid = ? AND BORROW.callnum = AUTHORSHIP.callnum " +
                    "AND AUTHORSHIP.callnum = BOOK.callnum ORDER BY BORROW.checkout DESC;");
            preparedStatement.setString(1, userId);


        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public void queryUserInfo(String userId) {
        try {
            PreparedStatement preparedStatement = myConnection.prepareStatement("SELECT * FROM LIBUSER WHERE libuid=?");
            preparedStatement.setString(1, userId);
            ResultSet myResults = preparedStatement.executeQuery();
            if(myResults.next()) {
                System.out.println("User ID: " + myResults.getString(1));
                System.out.println("Name: " + myResults.getString(2));
                System.out.println("Address: " + myResults.getString(3));
                System.out.println("User Category: " + myResults.getString(4));
                System.out.println("Loan Record: \n");

//
                 myConnection.prepareStatement("Create TABLE loan_record as select " +
                        "BORROW.callnum, BORROW.copynum, BOOK.title, LISTAGG(AUTHORship.ANAME, ', ') WITHIN " +
                        "GROUP (ORDER BY AUTHORSHIP.ANAME) AUTHORS, TO_CHAR(BORROW.checkout, 'DD/MM/YYYY') AS checkout, BORROW.return from BOOK, " +
                        "BORROW, AUTHORSHIP where BORROW.libuid ='" +userId + "'" +
                        " AND BORROW.callnum = AUTHORSHIP.callnum AND AUTHORSHIP.callnum = BOOK.callnum" +
                        " GROUP BY BORROW.callnum, BORROW.copynum, BOOK.title, BORROW.checkout, BORROW.return").executeUpdate();

                PreparedStatement newPreparedStatement = myConnection.prepareStatement("SELECT callnum, copynum, title, authors, checkout, " +
                        "CASE when return is null then 'no' else 'yes' end as returned from loan_record order by checkout desc");

                ResultSet userResult =  newPreparedStatement.executeQuery();

                while(userResult.next()) {
                    System.out.format("%1s%8s%1s%8s%1s%30s%1s%100s%1s%10s%1s%10s%1s\n", "|", "CalNum", "|", "CopyNum", "|", "Title", "|","Author",
                            "|", "Check-out", "|", "Returned?", "|");
                    String callNum = userResult.getString(1);
                    Integer copyNum = userResult.getInt(2);
                    String title = userResult.getString(3);
                    String author = userResult.getString(4);
                    String checkoutDate = userResult.getString(5);
                    String returned = userResult.getString(6);

                    System.out.format("%1s%8s%1s%8s%1s%30s%1s%100s%1s%10s%1s%10s%1s\n", "|", callNum, "|",
                            copyNum.toString(), "|", title, "|",author,
                            "|", checkoutDate, "|", returned, "|");
                }
                myConnection.prepareStatement("DROP TABLE loan_record").executeUpdate();
                System.out.println("\nEnd of query!");

            }




        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    public void queryBookByAuthor(String authorPartial) {
        try {
              PreparedStatement tempStatement= myConnection.
                    prepareStatement("CREATE TABLE author_books As " +
                            "SELECT authorship.callnum, book.title " +
                            "FROM authorship, book WHERE aname " +
                            "LIKE " + "'%" + authorPartial + "%'" + "AND authorship.callnum = book.callnum");
              tempStatement.executeUpdate();

              PreparedStatement preparedStatement = myConnection.prepareStatement(
                      "SELECT tmp.CALLNUM, tmp.TITLE, LISTAGG(AUTHORship.ANAME, ', ') " +
                      "WITHIN GROUP (ORDER BY AUTHORship.ANAME) AUTHORS FROM author_books as tmp, " +
                      "AUTHORSHIP WHERE tmp.callnum=authorship.callnum " +
                              "GROUP BY tmp.callnum, tmp.title order by tmp.callnum");

                ResultSet results = preparedStatement.executeQuery();
                System.out.format("%1s%8s%1s%30s%1s%100s%1s%10s%1s\n", "|", "CalNum", "|", "Title", "|", "Author", "|","Available", "|");
                System.out.format("%1s%8s%1s%30s%1s%100s%1s%10s%1s\n", "|", "", "|", "", "|", "", "|", "Num Copies", "|");

                while(results.next()) {
                    String callNumber = results.getString(1);
                    String title = results.getString(2);
                    String authors = results.getString(3);
                    int copiesAvailable = getTotalCopiesFromCallNumber(callNumber) - getBorrowedCopiesFromCallNumber(callNumber);
                    displayBookSearchResults(callNumber, title, authors, copiesAvailable);
                }
                System.out.println("End of Query Results!");

        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    private void queryBook(String whereClause) {
        try {
            PreparedStatement preparedStatement = myConnection.
                    prepareStatement("SELECT BOOK.callnum, BOOK.title, LISTAGG(AUTHORSHIP.aname, ', ')" +
                            "WITHIN GROUP (ORDER by AUTHORSHIP.aname) authors " +
                            "FROM BOOK, AUTHORSHIP " + whereClause +
                            " AND BOOK.callnum=AUTHORSHIP.callnum " +
                            "GROUP BY BOOK.callnum, BOOK.Title " +
                            "ORDER BY BOOK.callnum");

            ResultSet results = preparedStatement.executeQuery();
            System.out.format("%1s%8s%1s%30s%1s%100s%1s%10s%1s\n", "|", "CalNum", "|", "Title", "|", "Author", "|","Available", "|");
            System.out.format("%1s%8s%1s%30s%1s%100s%1s%10s%1s\n", "|", "", "|", "", "|", "", "|", "Num Copies", "|");


            while(results.next()) {
                String callNumber = results.getString(1);
                String title = results.getString(2);
                String authors = results.getString(3);
                int copiesAvailable = getTotalCopiesFromCallNumber(callNumber) - getBorrowedCopiesFromCallNumber(callNumber);
                displayBookSearchResults(callNumber, title, authors, copiesAvailable);
            }
            System.out.println("End of Query Results!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private int getTotalCopiesFromCallNumber(String callNumber) throws SQLException {
        PreparedStatement getTotalCopies =
                myConnection.prepareStatement("SELECT COUNT(*) as numcopies " +
                        "FROM copy WHERE copy.callnum=?");
        getTotalCopies.setString(1, callNumber);
        ResultSet totalCopiesResult = getTotalCopies.executeQuery();
        totalCopiesResult.next();
        return totalCopiesResult.getInt(1);
    }

    private int getBorrowedCopiesFromCallNumber(String callNumber) throws SQLException {
        PreparedStatement copiesTaken =
                myConnection.prepareStatement("SELECT COUNT(*) FROM borrow WHERE callnum=?");
        copiesTaken.setString(1, callNumber);
        ResultSet copiesTakenSet = copiesTaken.executeQuery();
        copiesTakenSet.next();

        return copiesTakenSet.getInt(1);

    }
    private void displayBookSearchResults(String callNumber, String title, String author, int copyAvailable) {
        System.out.format("%1s%8s%1s%30s%1s%100s%1s%10d%1s\n\n", "|", callNumber, "|", title, "|", author, "|", copyAvailable, "|");
    }

    public void queryMostOverdueBooks(int numberOverdue) {
        try {
            myConnection.prepareStatement("CREATE TABLE USERS_DURING_PERIOD " +
                    "AS SELECT libuid, period " +
                    "FROM category c, libuser l " +
                    "WHERE c.cid = l.cid").executeUpdate();

            myConnection.prepareStatement("CREATE TABLE OVERDUE_CALLNUM AS " +
                    "(SELECT borrow.callnum, count(borrow.callnum) AS count " +
                    "FROM borrow " +
                    "WHERE return >= checkout + " +
                    "(SELECT period " +
                    "FROM USERS_DURING_PERIOD " +
                    "WHERE borrow.libuid = USERS_DURING_PERIOD.libuid) " +
                    "GROUP BY borrow.callnum)").executeUpdate();

            PreparedStatement preparedStatement = myConnection.prepareStatement(
                    "SELECT OVERDUE_CALLNUM.callnum, book.title, OVERDUE_CALLNUM.count " +
                            "FROM OVERDUE_CALLNUM, book " +
                            "WHERE OVERDUE_CALLNUM.callnum = book.callnum " + "AND ROWNUM <=? " +
                            "ORDER BY OVERDUE_CALLNUM.count DESC");
            preparedStatement.setInt(1, numberOverdue);
            ResultSet results = preparedStatement.executeQuery();
            System.out.format("%1s%8s%1s%30s%1s%20s%1s\n", "|", "CallNum", "|", "Title", "|", "Total overdue num", "|");

            while(results.next()) {
                String callNum = results.getString(1);
                String bookTitle = results.getString(2);
                int numberTimesOverdue = results.getInt(3);
                displayOverdueSearchResults(callNum, bookTitle, numberTimesOverdue);
            }
            myConnection.prepareStatement("DROP TABLE USERS_DURING_PERIOD").executeUpdate();
            myConnection.prepareStatement("DROP TABLE OVERDUE_CALLNUM").executeUpdate();
            System.out.println("\nEnd of query results!");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void displayOverdueSearchResults(String callNum, String bookTitle, int numberOverdue) {
        System.out.format("%1s%8s%1s%30s%1s%20d%1s\n", "|", callNum, "|", bookTitle, "|", numberOverdue, "|");
    }

    public void queryBooksCheckedOutInPeriod(String startDate, String endDate) {
        try {
            PreparedStatement preparedStatement = myConnection.prepareStatement("SELECT COUNT(*) FROM BORROW " +
                    "WHERE checkout >= TO_DATE(?, 'DD/MM/YYYY') AND checkout <= TO_DATE(?, 'DD/MM/YYYY')");
            preparedStatement.setString(1, startDate);
            preparedStatement.setString(2, endDate);
            ResultSet myResults = preparedStatement.executeQuery();
            myResults.next();
            int totalCount = myResults.getInt(1);
            System.out.println("Total books checked out within the period [" + startDate +", " + endDate +"] is: " + totalCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
