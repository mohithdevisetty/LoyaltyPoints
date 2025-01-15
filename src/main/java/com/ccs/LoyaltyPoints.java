package com.ccs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class LoyaltyPoints  implements Serializable {
    private static final long serialVersionUID = 1L;

    private int serialNo;
    private String userId;
    private long pointsEarned;

    public LoyaltyPoints(int serialNo, String userId, long pointsEarned) {
        this.serialNo = serialNo;
        this.userId = userId;
        this.pointsEarned = pointsEarned;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getPointsEarned() {
        return pointsEarned;
    }

    public void setPointsEarned(long pointsEarned) {
        this.pointsEarned = pointsEarned;
    }

    public int getSerialNo() {
        return serialNo;
    }

    public void setSerialNo(int serialNo) {
        this.serialNo = serialNo;
    }

    // Persist the state of the object to local machine
    public static void serialize(LoyaltyPoints loyaltyPoints) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("loyalPoints.ser"))) {
            // Serialize the object
            oos.writeObject(loyaltyPoints);
            System.out.println("LoyalPoints object serialized successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Deserialize if the object exists previously
    public static LoyaltyPoints deserialize() {
        String fileName = "loyalPoints.ser";
        LoyaltyPoints loyaltyPoints = null;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName))) {
            // Deserialize the loyal points obj
            loyaltyPoints = (LoyaltyPoints) ois.readObject();
            System.out.println("Deserialized object: " + loyaltyPoints);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return loyaltyPoints;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return loyaltyPoints;
    }
}
