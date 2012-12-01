package edu.ucsb.cs.wrench.client;

import edu.ucsb.cs.wrench.WrenchException;
import edu.ucsb.cs.wrench.messaging.DatabaseSnapshot;
import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.List;
import java.util.Random;
import java.util.UUID;

public class WrenchClient {

    private static final int THREADS = 2;
    private static final int REQUESTS = 100;

    public static void main(String[] args) throws Exception {
        /*ClientThread[] threads = new ClientThread[THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ClientThread();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }*/
        read();
    }

    private static void read() {
        TTransport transport = new TSocket("localhost", 9091);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
            DatabaseSnapshot snapshot = client.read();
            List<String> grades = snapshot.getGrades();
            List<String> stats = snapshot.getStats();
            if (grades.size() == stats.size()) {
                for (int i = 0; i < grades.size(); i++) {
                    System.out.println(grades.get(i) + "\t" + stats.get(i));
                    validate(grades.get(i), stats.get(i));
                }
            } else {
                System.err.println("Size Mismatch!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }

    private static void validate(String grades, String stats) {
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

        if (min != (int) statData[0] || max != (int) statData[1] ||
                (sum/gradeData.length) != statData[2]) {
            throw new WrenchException("Data mismatch: " + grades + "\t" + stats);
        }
    }

    private static class ClientThread extends Thread {

        @Override
        public void run() {
            Random rand = new Random();
            int[] data = new int[10];
            for (int i = 0; i < data.length; i++) {
                data[i] = rand.nextInt(101);
            }

            for (int i = 0; i < REQUESTS; i++) {
                String transactionId = UUID.randomUUID().toString();
                if (sendToGrades(transactionId, data)) {
                    boolean committed = sendToStats(transactionId, data);
                    if (committed) {
                        System.out.println(transactionId + " COMPLETED");
                    } else {
                        System.out.println("OOPS");
                    }
                } else {
                    System.err.println("Grades cluster didn't accept");
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {

                }
            }
        }

        private boolean sendToGrades(String transactionId, int[] data) {
            TTransport transport = new TSocket("localhost", 9091);
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
                String dataString = "";
                for (int d : data) {
                    dataString += d + " ";
                }
                return client.append(transactionId, dataString.trim());
            } catch (Exception e) {
                return false;
            } finally {
                transport.close();
            }
        }

        private boolean sendToStats(String transactionId, int[] data) {
            TTransport transport = new TSocket("localhost", 8081);
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
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
                WrenchManagementService.Client client = new WrenchManagementService.Client(protocol);
                return client.append(transactionId, dataString);
            } catch (Exception e) {
                return false;
            } finally {
                transport.close();
            }
        }
    }

}
