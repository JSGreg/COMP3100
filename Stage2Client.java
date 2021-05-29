import java.io.*;
import java.net.*;
import java.util.*;

import javax.print.attribute.standard.JobHoldUntil;
import javax.xml.crypto.Data;

public class Stage2Client {

    public static class Server implements Comparable<Server> { // Server class to hold server info
        String Type = "";
        int ID;
        int core;
        int mem;
        int disk;
        int waitingJob;
        int runningJob;

        public Server(String Type, String core, String mem, String disk, String waitingJob, String runningJob) {
            this.Type = Type;
            this.core = Integer.parseInt(core);
            this.mem = Integer.parseInt(mem);
            this.disk = Integer.parseInt(disk); 
            this.waitingJob = Integer.parseInt(waitingJob);
            this.runningJob = Integer.parseInt(runningJob); 
        }

        public String getType() {
            return Type;
        }

        public String getCore() {
            return Integer.toString(core);
        }

        public String getMem () {
            return Integer.toString(mem);
        }

        public String getDisk () {
            return Integer.toString(disk);
        }

        public String getWaiting () {
            return Integer.toString(waitingJob);
        }

        public String getRunning () {
            return Integer.toString(runningJob);
        }

        public void setType(String newType) {
            this.Type = newType;
        }

        public void setCore(String newCore) {
            this.core = Integer.parseInt(newCore);
        }

        public void setMem(String newMem) {
            this.mem = Integer.parseInt(newMem);
        }

        public void setDisk(String newDisk) {
            this.disk = Integer.parseInt(newDisk);
        }

        public void setWaiting (String newWaiting) {
            this.waitingJob = Integer.parseInt(newWaiting);
        }

        public void setRunning (String newRunning) {
            this.waitingJob = Integer.parseInt(newRunning);
        }
        

        @Override
        public int compareTo(Stage2Client.Server o) {
            // sort by core then type ascending order.
            if (this.core - o.core == 0) {
                return o.Type.compareTo(this.Type);
            }
            return this.core - o.core;
        }

    }

    public static String[] parsing(String data) {
        String delims = "[ ]+"; // set the space as the splitting element for parsing messages.
        String[] splitData = data.split(delims);
        return splitData;
    }

    public static void sendMSG(String msg, DataOutputStream out) {
        try {
            out.write(msg.getBytes());
            out.flush();
        } catch (IOException e) {
            System.out.println(e);
        }

    }

    public static String readMSG(BufferedReader in) throws IOException {
        String message = in.readLine();
        System.out.println("server says: " + message);
        return message;
    }

    public static void doHandShake(BufferedReader in, DataOutputStream out) {
        try {
            String received = ""; // holds received message from server

            sendMSG("HELO\n", out); // initiate handshake by sending HELO

            received = readMSG(in);
            if (received.equals("OK")) {
                sendMSG("AUTH Group34\n", out);
            } else {
                System.out.println("ERROR: OK was not received");
            }

            received = readMSG(in);
            if (received.equals("OK")) {
                sendMSG("REDY\n", out);
            } else {
                System.out.println("ERROR: OK was not received");
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static int getServerIndex (String job[], BufferedReader in, DataOutputStream out) throws IOException {
        String messg = "";
        int jobCore;
        int jobMem;
        int jobDisk;
        int serverWait = 1000;
        int serverRun = 1000;
        

        sendMSG("GETS All\n", out); // get server DATA
        messg = readMSG(in);
        String[] Data = parsing(messg); // parse DATA to find the amount of servers
        sendMSG("OK\n", out);
        // Initialise variable for server DATA
        int totalServer = Integer.parseInt(Data[1]); // Number of servers on system.
        Server[] updatedServerList = new Server[totalServer]; // Create server array.
        int serverIndex = totalServer-1;

        // Loop through all servers to create server list
        for (int i = 0; i < totalServer; i++) {
            messg = readMSG(in);
            String[] updatedStringList = parsing(messg);
            updatedServerList[i] = new Server(updatedStringList[0], updatedStringList[4], updatedStringList[5], updatedStringList[6], updatedStringList[7], updatedStringList[8]);
        }

        sendMSG("OK\n", out); // catch the "." at end of data stream.
        messg = readMSG(in);

        //loop through all servers to find the server with the least waiting and running jobs
        for (int i = totalServer-1; i>=0; i--){ 
        jobCore = Integer.parseInt(job[4]);
        jobMem = Integer.parseInt(job[5]);
        jobDisk = Integer.parseInt(job[6]);
        // serverWait = serverList[i].waitingJob; 

        if (jobCore< updatedServerList[i].core && jobMem < updatedServerList[i].mem && jobDisk < updatedServerList[i].disk && serverWait >= updatedServerList[i].waitingJob &&  serverRun >= updatedServerList[i].runningJob){
            serverIndex=i;
            updatedServerList[i].waitingJob ++;
            updatedServerList[i].runningJob ++;
            serverWait = updatedServerList[i].waitingJob;
            serverRun = updatedServerList[i].runningJob;
        // } else {
        //  break;
        }

    }

        return serverIndex;
    }

    // public static String getServer (String[] job, BufferedReader in, DataOutputStream out) throws IOException {
    
    //      // System.out.println(rcvd);

    //      sendMSG("GETS Avail " + job[4] + " " + job[5] + " " + job[6] +"\n" , out);
    //      String rcvdString = readMSG(in);
    //      String[] Data = parsing(rcvdString);
    //      sendMSG("OK\n", out);
            
    //      int numServer = Integer.parseInt(Data[1]);
    //      Server[] serverList= new Server [numServer];

    //      for (int i=0; i<numServer; i++){
    //          rcvdString = readMSG(in);
    //          String[] stringList = parsing(rcvdString);
    //          serverList [i] = new Server (stringList[0], stringList[4]);
    //      }

    //      sendMSG("OK\n", out);

    //      String server=serverList[0].Type;
            
    //      return server;
    // }

    // public static String findSuitable (String requiredCores, Server[] cores, BufferedReader in, DataOutputStream out) throws IOException {
    //  String suitable = "";

    //  String numCores = cores;

    //  for (int i=0; i<numServer; i++){
    //      if (){

    //      }
    //  }

    //  return suitable;
    // }

    public static void main(String[] args) {

        try {

            Socket s = new Socket("localhost", 50000);

            BufferedReader din = new BufferedReader(new InputStreamReader(s.getInputStream()));
            DataOutputStream dout = new DataOutputStream(s.getOutputStream());

            String rcvd = "";

            // Handshake with server
            doHandShake(din, dout);

            // hold first job for later
            rcvd = readMSG(din);
            String firstjob = rcvd;
            System.out.println(firstjob + " and " + rcvd);
            // String firstjob = rcvd;

            sendMSG("GETS All\n", dout); // get server DATA
            rcvd = readMSG(din);
            String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
            sendMSG("OK\n", dout);
            // Initialise variable for server DATA
            int numServer = Integer.parseInt(Data[1]); // Number of servers on system.
            Server[] serverList = new Server[numServer]; // Create server array.

            // Loop through all servers to create server list
            for (int i = 0; i < numServer; i++) {
                rcvd = readMSG(din);
                String[] stringList = parsing(rcvd);
                serverList[i] = new Server(stringList[0], stringList[4], stringList[5], stringList[6], stringList[7], stringList[8]);
            }

            sendMSG("OK\n", dout); // catch the "." at end of data stream.
            rcvd = readMSG(din);
            // Arrays.sort(serverList); // Sort Servers

            // Schedule jobs to server
            // Currently it schedules jobs to the first possible server regardless of its state (Jobs are never assigned to larger servers so turnaround time is longer)
            rcvd=firstjob;


            while (!rcvd.equals("NONE")) {
                String[] job = parsing(rcvd); // Get job id and job type for switch statement
                int serverIndex = numServer-1;
                
                switch (job[0]) {
                case "JOBN": // Schedule job
                // System.out.println("This is a error");
                    
                    serverIndex=getServerIndex(job, din, dout);

                    sendMSG("SCHD " + job[2] + " " + serverList[serverIndex].Type + " " + serverList[serverIndex].ID + " \n", dout); 
                    //write a function to pick out capable server
                    // sendMSG("SCHD " + job[2] + " " + job[4] + " 0\n", dout);
                    
                    break;
                case "JCPL": // If job is being completed send REDY
                    sendMSG("REDY\n", dout);
                    break;
                case "OK": // Ask for next job
                    sendMSG("REDY\n", dout);
                    break;
                }
                rcvd = readMSG(din);
            }

            sendMSG("QUIT\n", dout);

            dout.close();
            s.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}