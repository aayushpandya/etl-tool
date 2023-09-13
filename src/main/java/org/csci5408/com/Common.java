package org.csci5408.com;

import java.io.File;
import java.util.Scanner;

public class Common {

    public static String getDataFromEnv(String key){
        try {
            File myObj = new File(".env");
            Scanner myReader = new Scanner(myObj);
            String apiKey = "";
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if(data.split("=")[0].equals(key)){
                    apiKey =  data.split("=")[1];
                    break;
                }
            }
            myReader.close();
            return apiKey;
        } catch (Exception e) {
            return null;
        }
    }
}
