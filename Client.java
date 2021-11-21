import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Client {
    private Socket socket = null;
    /*
     * in: used to read the results of query from the server side. out: used to
     * write the query to the server. stdin: used to get the query from the user
     * through terminal.
     */
    private DataInputStream in = null;
    private DataOutputStream out = null;
    private Scanner stdin = null;

    public Client(String address, int port) {
        try {
            InetAddress inet_address = InetAddress.getByName(address);

            // Creating the socket with the address of the server and the port
            // port number of leader is 6000
            socket = new Socket(inet_address, port);
            System.out.println("You have been connected successfully to the Database Server!");
            stdin = new Scanner(System.in);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
        } catch (UnknownHostException u) {
            System.err.println("Don't know about this host" + address);
            return;
        } catch (IOException io) {
            System.err.println("Problem in getting I/O for the connection to the host: " + address);
            return;
        }
    }

    private void start() throws IOException {
        String clientMessage = "";
        String serverResponse = "";
        String disconnect_codeword = "disconnect";

        serverResponse = in.readUTF();
        System.out.println(serverResponse);
        while (true) {
            try {
                // Get the query from user through terminal
                clientMessage = stdin.nextLine();

                // Send the query to the server using out
                out.writeUTF(clientMessage);

                // Check if the user has asked to Close the connection using the word "Over"
                if (clientMessage.toLowerCase().equals(disconnect_codeword)) {
                    System.out.println("Disconnect message received!! Closing the connection, Bye!");
                    break;
                }
                // otherwise read the server's response to the user's query and display it.
                else {
                    serverResponse = in.readUTF();
                    System.out.println(serverResponse+"\n");
                }
            } catch (IOException io) {
                System.err
                        .println("Problem in getting I/O while sending/recieving from the connection to the host" + io);
                return;
            }
        }
    }

    // Close the connections and the streams
    private void stop() {
        try {
            stdin.close();
            in.close();
            out.close();
        } catch (IOException io) {
            System.err.println("Problem in Closing the socket or streams");
            return;
        }
    }

    public static void main(String[] args) {
        int port = 0;
        try {
            if (args.length > 1) {
                System.out.println("More number of arguments given while Only one argument(Port Number) is required!");
                return;
            } else {
                port = Integer.parseInt(args[0]);

                String host = "localhost";
                Client client = new Client(host, port);
                client.start();
                client.stop();
            }
        } catch (NumberFormatException nfe) {
            System.out.println("First argument must be an integer!");
            return;
        } catch (IOException e) {
            System.out.println(e);
            return;
        }
    }
}