package edu.ucsb.cs.wrench.client;

import edu.ucsb.cs.wrench.messaging.WrenchManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.Random;
import java.util.UUID;

public class WrenchClient {

    private static final int THREADS = 2;
    private static final int REQUESTS = 100;

    public static void main(String[] args) throws Exception {
        ClientThread[] threads = new ClientThread[THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ClientThread();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
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
            String transactionId = UUID.randomUUID().toString();

            for (int i = 0; i < REQUESTS; i++) {
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
            TTransport transport = new TSocket("localhost", 9090);
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
