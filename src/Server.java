import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created by air_book on 11/3/17.
 */
public class Server {

    public static void main(String[] args) {
        try(ServerSocket serverSocket = new ServerSocket(5050)) {
            while(true) {
                new DBManipulator(serverSocket.accept()).start();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
