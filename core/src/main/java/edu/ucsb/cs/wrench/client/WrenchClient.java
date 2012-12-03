package edu.ucsb.cs.wrench.client;

import edu.ucsb.cs.wrench.config.Member;
import edu.ucsb.cs.wrench.config.WrenchConfiguration;
import edu.ucsb.cs.wrench.messaging.DatabaseSnapshot;
import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class WrenchClient {

    private static ArrayList<Member> gradeServers = new ArrayList<Member>();
    private static ArrayList<Member> statServers = new ArrayList<Member>();

    private static boolean verbose = false;

    public static void main(String[] args) throws IOException {
        System.out.println("Welcome to Wrench Client");
        System.out.println("Enter 'help' to see a list of supported commands...\n");

        Collections.addAll(gradeServers, WrenchConfiguration.getConfiguration().getMembers());
        Collections.addAll(statServers, WrenchConfiguration.getConfiguration().getPeers());

        printEndpoints();
        System.out.println();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("> ");
            try {
                String command = reader.readLine();
                command = command.trim();
                if ("quit".equals(command)) {
                    break;
                } else if ("read".equals(command)) {
                    read(false);
                } else if ("reads".equals(command)) {
                    read(true);
                } else if (command.startsWith("appendr")) {
                    if (command.equals("appendr")) {
                        appendRandom(1, 1);
                    } else {
                        String[] commandArgs = command.split(" ");
                        appendRandom(Integer.parseInt(commandArgs[1]), Integer.parseInt(commandArgs[2]));
                    }
                } else if (command.startsWith("append ")) {
                    String[] commandArgs = command.split(" ");
                    if (commandArgs.length != 11) {
                        System.out.println("append must be followed by 10 integers");
                    } else {
                        int[] data = new int[10];
                        for (int i = 0; i < data.length; i++) {
                            data[i] = Integer.parseInt(commandArgs[i + 1]);
                        }
                        if (append(data)) {
                            System.out.println("Append successful");
                        }
                    }
                } else if (command.startsWith("sgp")) {
                    String[] commandArgs = command.split(" ");
                    if (commandArgs.length == 2) {
                        setPrimary(gradeServers, commandArgs[1]);
                    } else {
                        printEndpoints();
                    }
                } else if (command.startsWith("ssp")) {
                    String[] commandArgs = command.split(" ");
                    if (commandArgs.length == 2) {
                        setPrimary(statServers, commandArgs[1]);
                    } else {
                        printEndpoints();
                    }
                } else if ("verbose".equals(command)) {
                    verbose = true;
                    System.out.println("Verbose mode enabled");
                } else if ("silent".equals(command)) {
                    verbose = false;
                    System.out.println("Silent mode enabled");
                } else if ("help".equals(command)) {
                    System.out.println("append [int x 10] - Append the given 10 integers to the database");
                    System.out.println("appendr [concurrency requests] - Append random entries to the database");
                    System.out.println("help - Displays this help message");
                    System.out.println("quit - Quits the Wrench client");
                    System.out.println("read - Read and output the current contents of the database");
                    System.out.println("reads - Read the current contents of the database and report on the consistency level");
                    System.out.println("sgp [processId] - Set the primary grades server");
                    System.out.println("silent - Turns off the verbose mode");
                    System.out.println("ssp [processId] - Set the primary stats server");
                    System.out.println("verbose - Activates the verbose mode");
                } else if ("".equals(command)) {
                } else {
                    System.out.println("Unrecognized command: " + command);
                }
            } catch (NumberFormatException e) {
                System.out.println("Number format error: Possible syntax error in command");
            }
        }
    }

    private static void setPrimary(List<Member> list, String primary) {
        int index = -1;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getProcessId().equals(primary)) {
                index = i;
                break;
            }
        }

        if (index == -1) {
            System.out.println("No endpoint found with process ID: " + primary);
        } else if (index == 0) {
            System.out.println(primary + " is already the primary");
        } else {
            Member member = list.remove(index);
            list.add(0, member);
            printEndpoints();
        }
    }

    private static void printEndpoints() {
        System.out.println("Current endpoint priorities");
        boolean first = true;
        for (Member member : gradeServers) {
            if (!first) {
                System.out.print(", ");
            }
            System.out.print(member.getProcessId());
            first = false;
        }

        System.out.println();

        first = true;
        for (Member member : statServers) {
            if (!first) {
                System.out.print(", ");
            }
            System.out.print(member.getProcessId());
            first = false;
        }
        System.out.println();
    }

    private static void read(boolean silent) {
        for (Member member : gradeServers) {
            TTransport transport = new TSocket(member.getHostname(), member.getPort());
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
                DatabaseSnapshot snapshot = client.read();
                List<String> grades = snapshot.getGrades();
                List<String> stats = snapshot.getStats();
                if (grades.size() == stats.size()) {
                    boolean valid = true;
                    List<String> errors = new ArrayList<String>();
                    for (int i = 0; i < grades.size(); i++) {
                        String msg = grades.get(i) + "\t\t" + stats.get(i);
                        if (!silent) {
                            System.out.println(msg);
                        }
                        boolean result = validate(grades.get(i), stats.get(i));
                        if (!result) {
                            errors.add(msg);
                        }
                        valid = valid && result;
                    }
                    if (valid) {
                        if (grades.size() > 0) {
                            if (!silent) {
                                System.out.println();
                            }
                            System.out.println("Returned " + grades.size() + " lines");
                            System.out.println("All entries consistent...");
                        } else {
                            System.out.println("No data available");
                        }
                    } else {
                        log("\nSome inconsistencies detected...", true);
                        for (String error : errors) {
                            log(error, true);
                        }
                    }
                } else {
                    log("Size Mismatch!", true);
                }
                System.out.flush();
                return;
            } catch (Exception e) {
                log("Error contacting member: " + member.getProcessId() + " " + e.getMessage(), false);
            } finally {
                transport.close();
            }
        }
        log("No live members were found", true);
    }

    private static boolean validate(String grades, String stats) {
        String[] gradeValues = grades.split(" ");
        int[] gradeData = new int[gradeValues.length];
        for (int i = 0; i < gradeData.length; i++) {
            gradeData[i] = Integer.parseInt(gradeValues[i]);
        }

        String[] statValues = stats.split(" ");
        double[] statData = new double[statValues.length];
        for (int i = 0; i < statData.length; i++) {
            statData[i] = Double.parseDouble(statValues[i]);
        }

        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        double sum = 0D;
        for (int grade : gradeData) {
            if (grade < min) {
                min = grade;
            }
            if (grade > max) {
                max = grade;
            }
            sum += grade;
        }

        return min == (int) statData[0] && max == (int) statData[1] &&
                (sum/gradeData.length) == statData[2];
    }

    private static void appendRandom(int concurrency, int requests) {
        ClientThread[] t = new ClientThread[concurrency];
        for (int i = 0; i < concurrency; i++) {
            t[i] = new ClientThread(requests);
            t[i].start();
        }

        int totalSuccess = 0;
        for (int i = 0; i < concurrency; i++) {
            try {
                t[i].join();
                totalSuccess += t[i].success;
            } catch (InterruptedException ignored) {
            }
        }

        System.out.println(totalSuccess + " of " + (concurrency*requests) + " appends were successful");
    }

    private static boolean append(int[] data) {
        String transactionId = UUID.randomUUID().toString();
        for (int i = 0; i < 10; i++) {
            if (sendToGrades(transactionId, data)) {
                boolean committed = sendToStats(transactionId, data);
                if (committed) {
                    log("Transaction " + transactionId + " completed", false);
                    return true;
                } else {
                    log("Transaction " + transactionId + " failed at stats - Possible 2pc " +
                            "coord fail-over - Restarting the transaction", false);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                }
            } else {
                log("Transaction " + transactionId + " failed at grades", true);
                return false;
            }
        }
        log("Transaction " + transactionId + " could not be completed", true);
        return false;
    }

    private static void log(String msg, boolean error) {
        if (verbose || error) {
            System.out.println(msg);
        }
    }

    private static boolean sendToGrades(String transactionId, int[] data) {
        String dataString = "";
        for (int d : data) {
            dataString += d + " ";
        }
        for (Member member : gradeServers) {
            TTransport transport = new TSocket(member.getHostname(), member.getPort());
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
                if (client.append(transactionId, dataString.trim())) {
                    return true;
                }
            } catch (Exception e) {
                log("Error contacting member: " + member.getProcessId() + " " + e.getMessage(), false);
            } finally {
                transport.close();
            }
        }
        return false;
    }

    private static boolean sendToStats(String transactionId, int[] data) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        double sum = 0D;
        for (int d : data) {
            if (d < min) {
                min = d;
            }
            if (d > max) {
                max = d;
            }
            sum += d;
        }
        String dataString = min + " " + max + " " + sum/data.length;

        for (Member member : statServers) {
            TTransport transport = new TSocket(member.getHostname(), member.getPort());
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
                if (client.append(transactionId, dataString)) {
                    return true;
                }
            } catch (Exception e) {
                log("Error contacting member: " + member.getProcessId() + " " + e.getMessage(), false);
            } finally {
                transport.close();
            }
        }
        return false;
    }

    private static class ClientThread extends Thread {

        private int requests;
        private int success = 0;

        private ClientThread(int requests) {
            this.requests = requests;
        }

        @Override
        public void run() {
            Random rand = new Random();

            for (int i = 0; i < requests; i++) {
                int[] data = new int[10];
                for (int j = 0; j < data.length; j++) {
                    data[j] = rand.nextInt(101);
                }
                if (append(data)) {
                    success++;
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {

                }
            }
        }
    }

}
