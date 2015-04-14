import DBConnector.DBConnectorInitializer;
import DBManager.DBManager;
import DBManager.QueryManager;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * Created by ngoctranfire on 4/9/15.
 */
public class DBExecutor {
    public static final String DB_USER = "db035";
    public static final String DB_PASS = "gojnkjfr";

    public static final String WELCOME_MESSAGE = "Welcome to the library inquery system!";

    public static final String WELCOME_MENU = "-----Main Menu-----\n" +
                                        "What kinds of operation would you like to perform?\n" +
                                        "1. Operations for administrator \n" +
                                        "2. Operations for Librarian \n" +
                                        "3. Operations for library director \n" +
                                        "4. Exit this program";

    public static final String ENTER_CHOICE = "Enter your choice: ";

    public static final String TYPE_PATH = "Type in the Source Data Folder Path: ";
    public static final String ERROR_CHOICE = "INVALID INPUT";
    public static final String WHAT_OPERATIONS = "What kinds of operation would you like to perform?\n";
    private static DBConnectorInitializer dbConnector;
    private static DBManager dbManager;
    private static QueryManager queryManager;


    public static void main(String[] args) throws SQLException {
        dbConnector = new DBConnectorInitializer(DB_USER, DB_PASS);
        dbManager= new DBManager(dbConnector.getStatement());
        queryManager= new QueryManager(dbConnector.getStatement().getConnection());

        System.out.println(WELCOME_MESSAGE);
        handleMainMenuCommands();


    }


    private static void handleMainMenuCommands(){
        boolean nonSuccessInput = true;
        while(nonSuccessInput) {
            System.out.println(WELCOME_MENU);
            int choice = getSelectedChoiceNumber(ENTER_CHOICE);
            switch (choice) {
                case 1:
                    handleAdministratorCommands();
                    nonSuccessInput = false;
                    break;
                case 2:
                    handleLibrarianCommands();
                    nonSuccessInput = false;
                    break;
                case 3:
                    handleDirectorCommands();
                    nonSuccessInput = false;
                    break;
                case 4:
                    nonSuccessInput = false;
                    System.exit(0);
                    break;
                default: //Error case
                    System.out.println(ERROR_CHOICE);
                    break;
            }
        }

    }

    private static int getSelectedChoiceNumber(String displayMessage) {
        while(true) {
            try{
                System.out.print(displayMessage);
                Scanner input = new Scanner(System.in);
                return input.nextInt();
            } catch (Exception e) {
                System.out.println(ERROR_CHOICE);
            }
        }

    }

    private static String getStringInput(String messageDisplay) {
        while(true) {
            try{
                System.out.print(messageDisplay);
                Scanner input = new Scanner(System.in);
                return input.nextLine();
            } catch (Exception e) {
                System.out.println(ERROR_CHOICE);
            }

        }

    }

    public static final String ADMIN_MENU = "\n-----Operations for administrator menu-----\n" +
            WHAT_OPERATIONS +
            "1. Create all tables\n" +
            "2. Delete all tables\n" +
            "3. Load from datafile\n" +
            "4. Show number of records in each table\n" +
            "5. Return to the main menu";


    /**
     * Handles the administrator commands
     */
    private static void handleAdministratorCommands(){
        boolean keepLooping = true;
        while (keepLooping) {
            System.out.println(ADMIN_MENU);
            int choice = getSelectedChoiceNumber(ENTER_CHOICE);
            switch (choice) {
                case 1:
                    dbManager.createLibraryTables();
                    System.out.println("Processing...Done! Database is initialized!");
                    break;
                case 2:
                    dbManager.dropAllTables();
                    System.out.println("Processing...Done! Database is removed!");
                    break;
                case 3:
                    dbManager.insertFileData(getStringInput(TYPE_PATH));
                    System.out.println("Processing...Done! Data is inputted to the database!");
                    break;
                case 4:
                    dbManager.outputTablesSummary();
                    break;
                case 5:
                    keepLooping = false;
                    handleMainMenuCommands();
                    break;
                default: //Error case
                    System.out.println(ERROR_CHOICE);
                    break;

            }
        }

    }
    public static final String LIBRARIAN_MENU = "\n-----Operations for librarian menu-----\n" +
            "What kinds of operation would you like to perform?\n" +
            "1. Search for books\n" +
            "2. Show load record of a user\n" +
            "3. Return to main menu";
    public static final String CHOOSE_SEARCH_CRITERION = "Choose the search criterion: ";

    private static void handleLibrarianCommands() {
        boolean keepLooping = true;

        while(keepLooping) {
            System.out.println(LIBRARIAN_MENU);
            int choice = getSelectedChoiceNumber(ENTER_CHOICE);
            switch(choice) {
                case 1:
                    startBookSearchRequest();
                    break;
                case 2:
                    startUserRecordRequest();
                    break;
                case 3:
                    keepLooping = false;
                    handleMainMenuCommands();
                    break;
                default:
                    System.out.println(ERROR_CHOICE);
                    break;
            }
            System.out.println(CHOOSE_SEARCH_CRITERION);
        }
        return;
    }

    public static final String DIRECTOR_MENU = "\n-----Operations for manager menu-----\n" +
            WHAT_OPERATIONS +
            "1. Show the N Books that are most often to be overdue\n" +
            "2. Show total number of books checked-out within a period\n" +
            "3. Return to main menu";

    private static void handleDirectorCommands() {
        boolean keepLooping = true;

        while(keepLooping) {
            System.out.println(DIRECTOR_MENU);
            int choice = getSelectedChoiceNumber(ENTER_CHOICE);
            switch(choice) {
                case 1:
                    displayNMostOverdueBooks();
                    break;
                case 2:
                    displayNumBooksInPeriod();
                    break;
                case 3:
                    handleMainMenuCommands();
                    keepLooping = false;
                    break;
                default:
                    System.out.println(ERROR_CHOICE);

            }
        }

        return;
    }

    public static final String SEARCH_CRITERION_MENU = "1. call number \n"
                                                     + "2. title \n"
                                                     + "3. author";
    private static void startBookSearchRequest() {
        System.out.println(CHOOSE_SEARCH_CRITERION);
        System.out.println(SEARCH_CRITERION_MENU);
        System.out.println(CHOOSE_SEARCH_CRITERION + ": ");
        int choice = getSelectedChoiceNumber(ENTER_CHOICE);
        try {
            QueryManager queryManager = new QueryManager(dbConnector.getStatement().getConnection());
            while(true) {

                String searchString = getStringInput("Type in the Search Keyword: ");
                switch(choice) {
                    case 1:
                        queryManager.queryBookCallNumber(searchString);
                        return;
                    case 2:
                        queryManager.queryBookTitle(searchString);
                        return;
                    case 3:
                        queryManager.queryBookByAuthor(searchString);
                        return;
                    default:
                        System.out.println(ERROR_CHOICE);
                        break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private static void startUserRecordRequest() {
        String enteredId = getStringInput("Enter The User ID: ");
        queryManager.queryUserInfo(enteredId);
    }

    public static final String TYPE_NUM_BOOKS = "Type in the number of books: ";

    private static void displayNMostOverdueBooks() {
        int choice = getSelectedChoiceNumber(TYPE_NUM_BOOKS);
        queryManager.queryMostOverdueBooks(choice);
    }
    private static void displayNumBooksInPeriod() {
        String startDate = getStringInput("Type in the starting date [dd/mm/yyyy]: ");
        String endDate = getStringInput("Type in the ending date [dd/mm/yyyy]: ");

        queryManager.queryBooksCheckedOutInPeriod(startDate, endDate);

    }
}


