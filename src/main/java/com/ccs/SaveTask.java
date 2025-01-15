package com.ccs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SaveTask implements Runnable {
    private static int BATCH_SIZE = 10;
    private static String delimiter = ",";
    private final String line;

    static BlockingQueue<LoyaltyPoints> queue = new LinkedBlockingQueue<>(BATCH_SIZE);
    public static List<LoyaltyPoints> list = new ArrayList<>();

    public SaveTask(String line) {
        this.line = line;
    }

    @Override
    public void run() {
        // Split the columns with comma(delimiter)
        String[] columns = line.split(delimiter);

        // Process each row to object (Parse it further as needed)
        LoyaltyPoints loyaltyPoints = new LoyaltyPoints(Integer.valueOf(columns[0]), columns[1], Long.valueOf(columns[3]));
        queue.add(loyaltyPoints);

        if (queue.size() >= BATCH_SIZE) {
            synchronized (list) {
                BatchInsert batch = new BatchInsert();
                try {
                    while (list.size() < BATCH_SIZE && !queue.isEmpty()) {
                        list.add(queue.take());
                        System.out.println("List of loyalty points persisting to Db : " + list);
                        batch.insert(list);
                        list.clear();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
