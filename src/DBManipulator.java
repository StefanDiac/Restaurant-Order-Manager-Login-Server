
import java.io.*;
import java.net.Socket;
import java.sql.*;
import java.util.ArrayList;

/**
 * Created by air_book on 11/3/17.
 */
public class DBManipulator extends Thread {
    private Socket socket;

    public DBManipulator(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try{
            //BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

            System.out.println("Someone connected !");
            Connection connection = null;
            try{
                Class.forName("org.sqlite.JDBC");
                connection = DriverManager.getConnection("jdbc:sqlite:orderManager.db");
                connection.setAutoCommit(false);

                System.out.println("Someone connected !");
                //if serverWipe
                try{
                    checkDB(connection);
                }catch (SQLException e) {
                    createTables(connection);
                }

                System.out.println("Someone connected !");
                BufferedReader receivedData = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter response = new PrintWriter(socket.getOutputStream(), true);
                String data = receivedData.readLine();
                String[] dataParts = data.split(",");

                System.out.println(data);
                switch(dataParts[0]) {
                    case "0":
                        String CUID = dataParts[1];
                        String companyName = dataParts[2];
                        boolean hasCollision = addNewUser(connection,CUID,companyName);

                        if(!hasCollision) {
                            response.println("1");
                        } else {
                            response.println("0");
                        }
                        break;
                    case "1":
                        CUID = dataParts[1];
                        boolean exists = checkCollisionUser(connection,CUID);

                        if(exists) {
                            response.println("1");
                        } else {
                            response.println("0");
                        }
                        break;
                    case "2":
                        ArrayList<String> waiterProperties = new ArrayList<>();
                        boolean hadError = false;
                        for(int i = 1; i < dataParts.length;i++) {
                            waiterProperties.add(dataParts[i]);
                        }
                        try {
                            addNewWaiter(connection, waiterProperties);
                        }catch (SQLException e) {
                            response.println("1");
                            hadError = true;
                        }
                        if(!hadError) {
                            response.println("0");
                        }
                        break;

                    case "3":
                        response.println("0");
                        CUID = dataParts[1];

                        try{
                            getAllWaiters(connection,CUID,response,receivedData);
                        }catch (SQLException e) {
                            e.printStackTrace();
                            response.println("2");
                        }
                        break;


                    default:
                        throw new Exception("Invalid server action detected !");
                }
                connection.close();

            }catch (ClassNotFoundException e) {
                e.printStackTrace();
            }catch (SQLException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch(Exception e){
            e.printStackTrace();
        }finally {
            try{
                socket.close();
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private ArrayList<String> getWaiters(Connection connection, int companyUID) {
        return null;
    }

    private ArrayList<String> getOrders(Connection connection, int companyUID) {
        return null;
    }

    private void createTables(Connection connection) throws SQLException {

        System.out.println("Called");
        Statement statement = connection.createStatement();


        String sqlCreateCompany = "create table COMPANY" + "(CUID TEXT PRIMARY KEY NOT NULL," + "COMPANY_NAME TEXT)";
        String sqlCreateWaiters = "create table WAITERS" + "(CNP TEXT PRIMARY KEY NOT NULL," + "FIRST_NAME TEXT," +
                "LAST_NAME TEXT," + "SALARY REAL," + "AGE INT," + "PFA INT," + "CUID TEXT," + "FOREIGN KEY(CUID) REFERENCES COMPANY(CUID))";

        statement.executeUpdate(sqlCreateCompany);
        statement.executeUpdate(sqlCreateWaiters);

        connection.commit();
    }

    private void dbWipe(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();

        String dropWaiters = "drop table WAITERS";
        //String dropOrders = "drop table ORDERS";
        String dropCompany = "drop table COMPANY";


        statement.executeUpdate(dropWaiters);
        statement.executeUpdate(dropCompany);
        createTables(connection);
    }

    private void checkDB(Connection connection) throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet tables = databaseMetaData.getTables(null,null,"COMPANY",null);
        if(!tables.next()) {
            throw new SQLException();
        }
    }

    private boolean addNewUser(Connection connection, String CUID, String companyName){
        boolean collisionFound = false;
        try {
            Statement statement = connection.createStatement();

            String addUserSQL = "INSERT INTO COMPANY VALUES " + " ('" + CUID + "', '" + companyName + "')";

            statement.executeUpdate(addUserSQL);

            connection.commit();
        }catch (SQLException e) {
            System.out.println(e.getMessage());
            collisionFound = true;
        }
        return collisionFound;
    }

    private boolean checkCollisionUser(Connection connection, String CUID) {
        boolean found = true;
        try {
            Statement statement = connection.createStatement();
            String checkSQL = "Select * FROM COMPANY WHERE CUID = '" + CUID + "';";
            statement.execute(checkSQL);
        } catch (SQLException e) {
            found = false;
        }
        return found;
    }

    private void addNewWaiter(Connection connection, ArrayList<String> waiterValues) throws SQLException {
        Statement statement = connection.createStatement();

        String addNewWaiterSQL = "INSERT INTO WAITERS VALUES " + " ('" + waiterValues.get(waiterValues.size()-1) + "', '" +
                waiterValues.get(1) + "', '" + waiterValues.get(2) + "', " + waiterValues.get(3) + ", " + waiterValues.get(4) +
                ", " + waiterValues.get(5) + ", '" + waiterValues.get(0) + "')";
        statement.executeUpdate(addNewWaiterSQL);
        connection.commit();

    }

    private void getAllWaiters(Connection connection, String CUID, PrintWriter responseSender, BufferedReader getResponse) throws SQLException {
        Statement statement = connection.createStatement();
        String sqlSelect = "SELECT * FROM WAITERS WHERE CUID = '" + CUID + "'";

        ResultSet resultSet = statement.executeQuery(sqlSelect);
        while(resultSet.next()) {
            String cnp = resultSet.getString("CNP");
            String firstName = resultSet.getString("FIRST_NAME");
            String lastName = resultSet.getString("LAST_NAME");
            double salary = resultSet.getDouble("SALARY");
            int age = resultSet.getInt("AGE");
            int pfaInt = resultSet.getInt("PFA");

            String responseString = cnp + "," + firstName + "," + lastName + "," + salary + "," + age + "," + pfaInt;
            responseSender.println(responseString);
            try {
                getResponse.readLine();
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        responseSender.println("1");
    }
 }
