package com.ccs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.ccs.LoyaltyPoints.deserialize;

public class CreditCardDataProcessor {
    public static void main(String[] args) {
        int noOfThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(noOfThreads);

        int serialNo = 0;

        LoyaltyPoints deserializedObj = deserialize();
        if (deserializedObj != null) {
            serialNo = deserializedObj.getSerialNo();
        }
        
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader("credit_card_data.csv"))) {

            // This for loop will avoid processing the already persisted records.
            for (int i = -1; i < serialNo; i++) {
                // -1 --> Exclude first row which may contain headers
                br.readLine();
            }

            while ((line = br.readLine()) != null) {
                System.out.println(line);
                executorService.submit(new SaveTask(line));
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
