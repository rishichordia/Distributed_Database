import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;
    

    class ClientSide_Interface extends Thread {
        BlockingQueue<String> query_q;
        BlockingQueue<String> response_q;
        ServerSocket socket_listening_to_client = null;
        Socket soc = null;
        DataInputStream in = null;
        DataOutputStream out = null;

        // Listening on port 6000
        ClientSide_Interface(int port, BlockingQueue<String> queryQ, BlockingQueue<String> responseQ) {
            try {
                socket_listening_to_client = new ServerSocket(port);
            } catch (IOException e) {
                System.out.println("Unable to create socket to listen at client interface");
                e.printStackTrace();
            }
            this.query_q = queryQ;
            this.response_q = responseQ;
        }

        String generate_table(String column_name, String response) {
            String[] columns = column_name.split("\\s+");
            int col = columns.length;
            response.trim();
            String[] responses = response.split("\\r?\\n");
            int[] size = new int[col];
            for (int i = 0; i < col; i++) {
                size[i] = columns[i].length() + 4;
            }
            for (int i = 0; i < responses.length; i++) {
                responses[i] = responses[i].trim();
                String[] values = responses[i].split("\\s+");
                for (int j = 0; j < values.length; j++) {
                    size[j] = Math.max(size[j], values[j].length());
                }
            }
            String table = "";
            String header = "+";
            for (int i = 0; i < col; i++) {
                for (int j = 0; j < size[i]; j++)
                    header += "-";
                if (i < col - 1)
                    header += "-";
                else
                    header += "+";
            }
            table = header + "\n" + "|";
            for (int i = 0; i < col; i++) {
                table = table + columns[i];
                for (int j = 0; j < size[i] - columns[i].length(); j++) {
                    table += " ";
                }
                table = table + "|";
            }
            table += "\n" + header + "\n";
    
            for (int i = 0; i < responses.length; i++) {
                table += "|";
                String[] values = responses[i].split("\\s+");
                for (int j = 0; j < values.length; j++) {
                    table = table + values[j];
                    for (int k = 0; k < size[j] - values[j].length(); k++)
                        table += " ";
                    table += "|";
                }
                table += "\n";
            }
            table += header + "\n";
            return table;
        }
        

        @Override
        public void run() {
            System.out.println("Client-Interface Started!!");
            while (true) {
                try {
                    soc=socket_listening_to_client.accept();
                    System.out.println("Client Connected!");
                    in = new DataInputStream(soc.getInputStream());
                    out = new DataOutputStream(soc.getOutputStream());
                    String leader_msg = "Please enter your MYSQL query to get results!";
                    String client_msg = null;
                    out.writeUTF(leader_msg);
                    while (true) {
                        leader_msg = "";
                        try{
                            client_msg = in.readUTF();
                        }
                        catch(IOException e){
                            System.out.println("Client disconnected");
                            break;
                        }
                        if (client_msg.toLowerCase().equals("disconnect")||client_msg.isEmpty()){
                            System.out.println("Disconnected with the client as per their request!!");
                            break;
                        }
                        if (client_msg.isEmpty()){
                            out.writeUTF("Invalid query");
                            continue;
                        }
                        System.out.println("\nQuery received from the client:- "+client_msg);
                        query_q.put(client_msg);    
                        leader_msg = response_q.take();
                        if(!leader_msg.toLowerCase().contains("error"))
                            {
                                String[] temp = leader_msg.split("\n");
                                String column_name = temp[0];
                                String column_data="";
                                    for (int j = 1; j < temp.length; j++) {
                                            column_data += temp[j] + "\n";
                                    }   
                                leader_msg=generate_table(column_name,column_data);
                            }
                        out.writeUTF(leader_msg);
                    }
                    soc.close();
                } catch (InterruptedException | IOException e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
            }
    }}


//--------------------------------------------------------------------------------------------------------------------------------------

    class Manager extends Thread {
        BlockingQueue<String> query_q;
        BlockingQueue<String> response_q;
        BlockingQueue<String>[] query_node;
        BlockingQueue<String>[] response_node;

        private static final String dbClassName = "com.mysql.cj.jdbc.Driver";
        private String CONNECTION = "jdbc:mysql://localhost:3306/Metadata_Manager?autoReconnect=true&useSSL=false";
        private Connection c = null;
        private Statement stmt = null;
        private HashMap<String, Integer> lookup_replicationGroup = new HashMap<>();
        private String table_name="";
        private int last_commited_Transaction_Id;
        
        // Listening on port 6000
        Manager(BlockingQueue<String> queryQ, BlockingQueue<String> responseQ,BlockingQueue<String>[] query_node, BlockingQueue<String>[] response_node) throws ClassNotFoundException, SQLException {
            this.query_q = queryQ;
            this.response_q = responseQ;
            this.query_node=query_node;
            this.response_node=response_node;

            Class.forName(dbClassName);
            c = DriverManager.getConnection(CONNECTION, "project", "rock@_1206");
            stmt = c.createStatement();
        }

        void fetch_metadata() throws SQLException
        {
            ResultSet res=stmt.executeQuery("select * from Lookup_RG;");
            while(res.next())
            {
                lookup_replicationGroup.put(res.getString(1),res.getInt(2));
            }
            res=stmt.executeQuery("select max(Transaction_ID) from TransactionLogs;");
            res.next();
            last_commited_Transaction_Id=res.getInt(1);
        }
        
        int verify_query(String query)
        {
            query.trim();
            String[] aftersplit = query.split("\\s+");
            table_name="";
            switch(aftersplit[0].toLowerCase())
            {
                case "select": 
                    if(!query.toLowerCase().contains("from"))
                        return -1;
                    for(int i=0;i<aftersplit.length;i++)
                    {
                        if(aftersplit[i].toLowerCase().equals("from"))
                        {
                            if(i==aftersplit.length-1)
                                return -1;
                            table_name=aftersplit[i+1];
                            break;
                        }
                    }
                break;
                case "update":
                    if(aftersplit.length<4)
                        return -1;
                    table_name=aftersplit[1];
                break;
                case "insert":
                    if(aftersplit.length<4)
                        return -1;
                    table_name=aftersplit[2];
                break;
                case "delete":
                    if(aftersplit.length<3)
                        return -1;
                    table_name=aftersplit[2];
                break;
                default :  return -1;
            }
            table_name=table_name.replaceAll(";$|^;", "");
            if(lookup_replicationGroup.containsKey(table_name)  )
                return lookup_replicationGroup.get(table_name);
            else return -2;
        }
        boolean check_node_availaibility(int replication_Group_Id)
        {
            int c=0;
            for(int j=replication_Group_Id;j<9;j+=3)
                if(CentralServer.available[j]==true)
                    c++;
            if(c>=2)
                return true;
            return false;
        }
        void commit_transaction_to_log(String query) throws SQLException
        {
            int updated_transaction_ID=last_commited_Transaction_Id+1;
            query=query.replaceAll("'","''");
            String final_query="insert into TransactionLogs values ('"+table_name+"',"+updated_transaction_ID+",'"+query+"');";
            stmt.executeUpdate(final_query);
            last_commited_Transaction_Id=updated_transaction_ID;
        }
        String send_transaction(String query,int replication_Group_Id) throws InterruptedException
        {
            String response="";
            for(int i=replication_Group_Id;i<9;i+=3)
            {
                if(CentralServer.available[i]==true)
                    {
                        query_node[i].put(query);
                        response=response_node[i].take();
                        if(response.toLowerCase().equals("error"))
                            break;
                    }
            }
            return response;
        }

        @Override
        public void run() {
            System.out.println("Data Manager Started!!");
            System.out.println("Fetching metadata");
            try {
                fetch_metadata();
                System.out.println("Successfully fetched!");
            } 
            catch (SQLException e1) {
                e1.printStackTrace();
            }
            String client_query="",message_to_nodes="";
            String response="YO";
            while (true) {
                try{
                    client_query = query_q.take();
                    
                    int replication_Group_Id=verify_query(client_query);
                    if(replication_Group_Id==-1)
                    {
                        System.out.println("Invalid Query Received");
                        response_q.put("Error :- Invalid Query Received");
                        continue;
                    }
                    else if(replication_Group_Id==-2)
                    {
                        System.out.println("Table does not exist");
                        response_q.put("Error:- Table does not exist");
                        continue;
                    }

                    if(check_node_availaibility(replication_Group_Id))
                    {
                        message_to_nodes=last_commited_Transaction_Id +" " +client_query;
                        System.out.println("\nTable "+table_name+" exists in database\nReplication Group "+replication_Group_Id+" available. Executing query");
                        response=send_transaction(message_to_nodes,replication_Group_Id);
                        if(!response.equals("error")&&client_query.toLowerCase().contains("select")==false)
                            commit_transaction_to_log(client_query);
                    }
                    else
                    {
                        System.out.println("Replication Group "+replication_Group_Id+" unavailable. Aborting Query");
                        response="Error:- Database Cluster currently unavailable. Aborting Query";
                    }
                    response_q.put(response);
                }
                catch(InterruptedException e)
                {
                    System.out.println("Exception detected in put and take of queue:"+e);
                    e.printStackTrace();
                }
                catch(SQLException e)
                {
                    System.out.println("Exception detected in query:"+e);
                    e.printStackTrace();
                }
            }
        }
    }

//------------------------------------------------------End Of Server Manager----------------------------------------------------------------------------------------

//------------------------------------------------Start Of Node Interface Threads------------------------------------------------------------
    class DataNode_Interface extends Thread{
        BlockingQueue<String> query_q;
        BlockingQueue<String> response_q;
        Socket soc = null;
        Socket heartbeat=null;
        DataInputStream in = null;
        DataOutputStream out = null;
        DataInputStream heartbeat_in = null;
        int node_id;
        boolean exit=false;
        private static final String dbClassName = "com.mysql.cj.jdbc.Driver";
        private String CONNECTION = "jdbc:mysql://localhost:3306/Metadata_Manager?autoReconnect=true&useSSL=false";
        private Connection c = null;
        private Statement stmt = null;



        DataNode_Interface(Socket socket,Socket heartbeat, BlockingQueue<String> queryQ, BlockingQueue<String> responseQ,int node_id) throws IOException, SQLException, ClassNotFoundException {
            this.soc = socket;
            this.heartbeat=heartbeat;
            this.query_q = queryQ;
            this.response_q = responseQ;
            this.node_id=node_id;
            in = new DataInputStream(soc.getInputStream());
            out = new DataOutputStream(soc.getOutputStream());
            heartbeat_in=new DataInputStream(heartbeat.getInputStream());
        
            Class.forName(dbClassName);
            c = DriverManager.getConnection(CONNECTION, "project", "rock@_1206");
            stmt = c.createStatement();
        }

        void close_thread() throws IOException
        {
            soc.close();
            heartbeat.close();
            in.close();
            out.close();
            heartbeat_in.close();
        }
        void update_datanode() throws SQLException, IOException
        {
            String last_transaction=in.readUTF();
            String[] transaction=last_transaction.split("\\s+");
            String query="select Transaction_ID,Log_Query from TransactionLogs where TableName='"+transaction[0]+"' AND Transaction_ID >"+transaction[1]+";";
            int r;
            ResultSet res=stmt.executeQuery(query);
            while(res.next())
            {
                r=res.getInt(1);
                query=res.getString(2);
                out.writeUTF(query);
                out.writeInt(r);
                r=in.readInt();
            }
            out.writeUTF("finish");
        }



        @Override
        public void run() {
            String client_query=null;
            String resp = null;
            new Thread(new Runnable() {
                    public void run()
                    {
                        String input=null;
                        while(true)
                        {
                            try{
						    	input=heartbeat_in.readUTF();
						    }
                            catch (IOException e) {
						    	System.out.println("\nDatanode "+node_id+" disconnected");
                                exit=true;
                                break;
						    }
                        }
                    }
                }).start();
            
            try {
				update_datanode();
			} catch (SQLException | IOException e1) {
				System.out.println("Update failed");
				e1.printStackTrace();
                exit=true;
			}
            
            while (!exit) {
                try {
                    client_query=null;
                    while(!exit && client_query==null){
                            client_query=query_q.poll(2, TimeUnit.SECONDS);
                        }
                    if(exit){
                        System.out.println("Closing thread");
                        break;
                    }
                    System.out.println("Sending the query to Worker: " + Thread.currentThread().getName());
                    out.writeUTF(client_query);
                    resp = in.readUTF();
                    if (resp.isEmpty())
                        resp = "error";
                    response_q.put(resp);
                }
                catch(InterruptedException e) {
                    System.out.println(e);
                    e.printStackTrace();
                }
                catch(IOException e){
                    System.out.println("Data Node disconnected");
                    break;
                }
            }
            CentralServer.available[node_id]=false;
            System.out.println("Value of datanode set to:- "+ CentralServer.available[node_id]);
            try{
                close_thread();
            }
            catch(IOException e)
            {
                System.out.println("Exception while closing thread");
                e.printStackTrace();
            }
        }
    }

//-------------------------------------------------End Of Datanode Interface-----------------------------------------------------------


//-----------------------------------------------Start Of Main Program (Central Server)-----------------------------------------------
    public class CentralServer {
        private static final int total_data_nodes=9;
        //Queue for communication with client thread
        BlockingQueue<String> query_client=null;
        BlockingQueue<String> response_client=null;

        //Queue for communication with database node threads
        BlockingQueue<String> query_node[] = new LinkedBlockingQueue[total_data_nodes];
        BlockingQueue<String> response_node[] = new LinkedBlockingQueue[total_data_nodes];

        Thread clientThread=null;
        Thread server_manager=null;

        //Socket listening to distributed database nodes
        ServerSocket socket_listening_to_serverNodes = null;
        ServerSocket heartbeat_socket=null;
        DataInputStream dis = null;
        DataOutputStream dos = null;
        
        public static volatile boolean available[]=new boolean[total_data_nodes];

        CentralServer(int port,int heartbeat_port) throws IOException{
            socket_listening_to_serverNodes = new ServerSocket(port);
            heartbeat_socket=new ServerSocket(heartbeat_port);
            
            query_client=new LinkedBlockingQueue<>();
            response_client=new LinkedBlockingQueue<>();
            
            for(int i=0;i<available.length;i++)
                available[i]=false;
            
            for (int i = 0; i < total_data_nodes; i++) {
                query_node[i] = new LinkedBlockingQueue<String>();
                response_node[i] = new LinkedBlockingQueue<String>();
            }
        }

        void listen_to_clients()
        {
            clientThread = new Thread(new ClientSide_Interface(6000,this.query_client,this.response_client));
            clientThread.start();
        }
        void start_manager() throws ClassNotFoundException, SQLException
        {
            server_manager = new Thread(new Manager(this.query_client,this.response_client,this.query_node,this.response_node));
            server_manager.start();
        }
        void listen_to_database_nodes() throws IOException{
            while(true)
            {
                Socket tempsoc=socket_listening_to_serverNodes.accept();
                Socket temp_heartbeat=heartbeat_socket.accept();
                dis=new DataInputStream(tempsoc.getInputStream());
                
                int thread_id=dis.readInt();
                available[thread_id]=true;
                try {
					new Thread(
					    new DataNode_Interface
					        (tempsoc,temp_heartbeat, query_node[thread_id],response_node[thread_id],thread_id)).start();
				} catch (ClassNotFoundException | SQLException e) {
					System.out.println("Error in connecting to database");
					e.printStackTrace();
				}
            }
        }
        public static void main(String[] args){
            int port = 8000,heartbeat_port=8001;
            CentralServer leader;
            try {
                leader = new CentralServer(port,heartbeat_port);
                leader.listen_to_clients();
                leader.start_manager();
                leader.listen_to_database_nodes(); 
            } catch (IOException e) {
                System.out.println("Failed to open socket at port 8000");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                System.out.println("Cannot find jdbc class");
                e.printStackTrace();
            } catch (SQLException e) {
                System.out.println("Cannot connect to SQL database");
                e.printStackTrace();
            }
        }
    }