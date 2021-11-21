import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.sql.*;

class HeartbeatAgent extends Thread{
    Socket heartbeat;
    DataOutputStream out=null;
    HeartbeatAgent(Socket heartbeat) throws IOException
    {
        this.heartbeat=heartbeat;
        out=new DataOutputStream(heartbeat.getOutputStream());
    }
    @Override
    public void run()
    {
        System.out.println("Starting heartbeat thread");
        while(!Thread.interrupted()){
            try{
                out.writeUTF("Active");
                Thread.sleep(2000);
            }
            catch(IOException e)
            {
                System.out.println("Heartbeat sending failed server down");
                e.printStackTrace();
                break;
            }
            catch(InterruptedException e)
            {
                System.out.println("Heart Beat Thread Stopping\n");
                break;
            }
        }
        Worker.exit=true;
    }
}


public class Worker {
    
    InetAddress inet_address;
    Socket socket = null;
    Socket heartbeat=null;
    Thread heartbeat_t;
    DataInputStream dis = null;
    DataOutputStream dos = null;


    private static final String dbClassName = "com.mysql.cj.jdbc.Driver";
    private static String CONNECTION = "jdbc:mysql://localhost:3306/project";
    Connection c = null;
    Statement stmt = null;
    
    
    private int node_id;
    static boolean exit=false;
    private int last_commited_transaction_id=0;

    Worker(String address,int port,int node_id) throws IOException, ClassNotFoundException, SQLException{
            inet_address = InetAddress.getByName(address);
            socket=new Socket(inet_address,port);
            this.node_id=node_id;
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());
            
            System.out.println("Successfully connected to central server!!");
            System.out.println("Sending node id to central server");
            dos.writeInt(this.node_id);
            CONNECTION = CONNECTION + "_" + node_id + "?autoReconnect=true&useSSL=false";
            Class.forName(dbClassName);
            c = DriverManager.getConnection(CONNECTION, "project", "rock@_1206");
            stmt = c.createStatement();
        }

    // Execute a query and send response in specific format
    String process_query(String query) {
        ResultSet res;
        String to_client = "";
        try {
            res = stmt.executeQuery(query);
            // Metadata used to get the column names and count
            ResultSetMetaData rsmd = res.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            if (columnsNumber >= 1) {
                for (int i = 1; i <= columnsNumber; i++) {
                    // The first line of response is the column names
                    to_client += rsmd.getColumnName(i) + " ";
                }
            }
            to_client += "\n";
            // Add the data values of all tuple to the response
            while (res.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = res.getString(i);
                    to_client += columnValue + " ";
                }
                to_client += '\n';
            }
        } catch (SQLException e) {
            System.out.println(e);
            to_client = "error";
        }
        return to_client;
    }
    void start_heartbeat(String address,int port) throws IOException
    {
        heartbeat=new Socket(inet_address,port);
        heartbeat_t=new Thread(new HeartbeatAgent(heartbeat));
        heartbeat_t.start();
    }
    void update_database() throws IOException
    {
        try{
            ResultSet res=stmt.executeQuery("select * from NodeTransactionLog;");
            res.next();
            last_commited_transaction_id=res.getInt(2);
            String last_commited_transaction=res.getString(1)+" "+last_commited_transaction_id;
            dos.writeUTF(last_commited_transaction);
            String response=dis.readUTF();
            int updated_transaction=-1;
            while(!response.toLowerCase().equals("finish"))
            {
                updated_transaction=dis.readInt();
                try{
                    stmt.executeUpdate(response);
                    dos.writeInt(0);
                }
                catch(SQLException e)
                {
                    System.out.println("Error executing query.Updation Failed "+e);
                    e.printStackTrace();
                    dos.writeInt(-1);
                    exit=true;
                    break;
                }
                response=dis.readUTF();
            }
            if(updated_transaction!=-1)
                {
                    stmt.executeUpdate("update NodeTransactionLog set LastCommitedLog=" +updated_transaction+";");
                    last_commited_transaction_id=updated_transaction;
                }
            System.out.println("Database Node up to date");
        }
        catch(SQLException e){
            System.out.println("Cannot connect to database. Updation failed");
            e.printStackTrace();
            exit=true;
        }
    }
    // This is where communication between Leader and worker takes place
    void start() throws IOException {
        String input = null;
        String[] query=null;
        System.out.println("Waiting for query from central server");
        while (!exit) {
            input = dis.readUTF();
            System.out.println("Input received:- \n"+input);
            String response="";
            query = input.split(" ",2);
            if (query[1].toLowerCase().contains("select")) {
                response = process_query(query[1]);
                dos.writeUTF(response);
            } 
            else 
            {
                try {
					stmt.executeUpdate(query[1]);
                    last_commited_transaction_id=Integer.parseInt(query[0])+1;
                    stmt.executeUpdate("update NodeTransactionLog set LastCommitedLog="+last_commited_transaction_id+";");
                    response="Updated successfully";
				} 
                catch (SQLException e) {
                    response="error";
					e.printStackTrace();
				}
                finally{
                dos.writeUTF(response);}
            }
            System.out.println(response);
        }
    }

    void start_shutdown_handler()
    {
        Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run()
        {   

            heartbeat_t.interrupt();
            try {
                socket.close();
                heartbeat.close();
                dis.close();
                dos.close();
            } catch (IOException e) {
                System.out.println("Exception in shutdown handler");
                e.printStackTrace();
            }
        }
        });
    }
    // java Worker 8000 (thread_id)
    public static void main(String[] args) throws ClassNotFoundException {
        int port = 0,node_id=0;
        // can be changed as per requirement
        String address = "127.0.0.1";
            try{
                if (args.length > 2) {
                    System.out.println("More number of arguments given while Only argument(Port_Number, Node_id) are required!");
                    return;
                } 
                else if (args.length < 2) {
                    System.out.println("Less number of arguments given while arguments(Port_Number, Node_id) are required!");
                    return;
                } 
                else{
                    port = Integer.parseInt(args[0]);
                    node_id=Integer.parseInt(args[1]);
                    Worker worker = new Worker(address, port,node_id);
                    worker.start_shutdown_handler();
                    worker.start_heartbeat(address,8001);
                    worker.update_database();
                    worker.start();
                }
            }
            catch (ClassNotFoundException e) {
                System.out.println("Jdbc class not found");
                e.printStackTrace();
            }
            catch (SQLException e) {
                System.out.println("Problem in connecting to jdbc");
                e.printStackTrace();
            }
            catch (UnknownHostException e) {
                System.out.println("Unable to resolve host");
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
    }
}