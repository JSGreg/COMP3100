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

        public Server(String Type, String ID, String core, String mem, String disk, String waitingJob,
                String runningJob) {
            this.Type = Type;
            this.ID = Integer.parseInt(ID);
            this.core = Integer.parseInt(core);
            this.mem = Integer.parseInt(mem);
            this.disk = Integer.parseInt(disk);
            this.waitingJob = Integer.parseInt(waitingJob);
            this.runningJob = Integer.parseInt(runningJob);
        }

        public String getType() {
            return Type;
        }

        public String getID() {
            return Integer.toString(ID);
        }

        public String getCore() {
            return Integer.toString(core);
        }

        public String getMem() {
            return Integer.toString(mem);
        }

        public String getDisk() {
            return Integer.toString(disk);
        }

        public String getWaiting() {
            return Integer.toString(waitingJob);
        }

        public String getRunning() {
            return Integer.toString(runningJob);
        }

        public void setType(String newType) {
            this.Type = newType;
        }

        public void setID(String newID) {
            this.ID = Integer.parseInt(newID);
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

        public void setWaiting(String newWaiting) {
            this.waitingJob = Integer.parseInt(newWaiting);
        }

        public void setRunning(String newRunning) {
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
                sendMSG("AUTH Jonathan\n", out);
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

    // Gets the minimum requirements for jobs
    public static String getHalfAvail(String job[], BufferedReader in, DataOutputStream out) throws IOException {
        String serverInfo = "";
        int jobCore = Integer.parseInt(job[4]);

        int jobMem = Integer.parseInt(job[5]);
        int jobDisk = Integer.parseInt(job[6]);

        sendMSG("GETS Avail " + job[4] + " " + job[5] + " " + job[6] + "\n", out);
        String rcvdString = readMSG(in);
        String[] Data = parsing(rcvdString);
        sendMSG("OK\n", out);
        int totalServer;
        if (Data[1].equals("0")) {
            sendMSG("GETS All\n", out);
            rcvdString = readMSG(in);
            rcvdString = readMSG(in);
            Data = parsing(rcvdString);

            sendMSG("OK\n", out);
            totalServer = Integer.parseInt(Data[1]);
        } else {
            // Initialise variable for server DATA
            totalServer = Integer.parseInt(Data[1]); // Number of servers on system.
        }

        Server[] updatedServerList = new Server[totalServer]; // Create server array.

        // Loop through all servers to create server list
        for (int i = 0; i < totalServer; i++) {
            rcvdString = readMSG(in);
            String[] updatedStringList = parsing(rcvdString);
            updatedServerList[i] = new Server(updatedStringList[0], updatedStringList[1], updatedStringList[4],
                    updatedStringList[5], updatedStringList[6], updatedStringList[7], updatedStringList[8]);
        }

        sendMSG("OK\n", out); // catch the "." at end of data stream.
        rcvdString = readMSG(in);

        int waitingTemp = 0;

        serverInfo = updatedServerList[totalServer - 1].getType() + " " + updatedServerList[totalServer - 1].getID();
        // loop through all servers to find the server with the least waiting and
        // running jobs
        for (int i = totalServer - 1; i >= 0; i--) {
            if (jobCore <= updatedServerList[i].core && updatedServerList[i].ID % 2 != 0) {
                if (jobMem <= updatedServerList[i].mem) {
                    if (jobDisk <= updatedServerList[i].disk) {
                        if (waitingTemp >= updatedServerList[i].waitingJob) {
                            waitingTemp = updatedServerList[i].waitingJob;
                            serverInfo = updatedServerList[i].getType() + " " + updatedServerList[i].getID();
                        }
                    }
                }
            } else if (waitingTemp < updatedServerList[i].waitingJob) {
                waitingTemp = updatedServerList[i].waitingJob;
            }
        }
        return serverInfo;
    }

    public static void main(String[] args) {

        try {

            Socket s = new Socket("localhost", 50000);

            BufferedReader din = new BufferedReader(new InputStreamReader(s.getInputStream()));
            DataOutputStream dout = new DataOutputStream(s.getOutputStream());

            String rcvd = "";

            // Handshake with server
            doHandShake(din, dout);

            rcvd = readMSG(din);

            while (!rcvd.equals("NONE")) {
                String[] job = parsing(rcvd); // Get job id and job type for switch statement

                switch (job[0]) {
                    case "JOBN": // Schedule job
                        sendMSG("SCHD " + job[2] + " " + getHalfAvail(job, din, dout) + "\n", dout);
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