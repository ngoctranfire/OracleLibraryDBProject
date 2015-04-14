# OracleLibraryDBProject
This is my implementation of the Database class for the Library Database with the features requested by the document.

Please do not copy this project! The professor will find out!

To run the code, please do the following on the command line.
cd src
javac -cp ojdbc6.jar DataLoader/*.java DBConnector/*.java DBManager/*.java DBExecutor.java
java -cp ./ojdbc6.jar:./ DBExecutor

Here is a description of the modules and components of this program as defined below:

DBExecutor: This is the main running java code that executes and runs all of the other components. This deals directly
with presenting the user interface;

QueryManager: This is used for all querying types of functions, such as searching for the student loan record. 
This is more geared towards the uses of the Librarian and the Director who has to query for data.

DBConnectorInitializer: This is the class that allows me to connect to the database and start transactions.

FileDataLoader: This is used to interact with the system and load text files contained in it to the database. 

DBManager: This is geared towards the Aministrator. It interacts with the database as a whole. This means that
it can remove and add user data as well as give some information on the database. 
