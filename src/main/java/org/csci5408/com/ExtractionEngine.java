package org.csci5408.com;


import java.net.HttpURLConnection;

import java.net.URL;
import java.util.ArrayList;
import java.util.Scanner;

public class ExtractionEngine {
    ProcessingEngine processingObj = new ProcessingEngine();

    /**
     * This method is used to extract data.
     * @param keyWords List of all the keywords.
     */
    public void extractData(ArrayList<String> keyWords){
        System.out.println("Extraction Engine running.........");
        System.out.println("Extracting data from \"newsapi.org\"..................");
        System.out.println("...\n...");
        ArrayList<String> responseData = new ArrayList<String>();
        for(String keyWord: keyWords) {
            String data =  getDataFromAPI(keyWord);
            if(data != null && data.length() > 0)
                responseData.add(data);

        }
        processingObj.procesNewsData(responseData, keyWords);
    }

    /**
     * This method is used to fetch the data from the news api endpoint
     * @param keyWord String of the keyword.
     * @return String. returns the response string from the API.
     */
    public String getDataFromAPI(String keyWord){
        try{
            String apiKey = Common.getDataFromEnv("API_KEY");
           // URL newsApiUrl = new URL("https://newsapi.org/v2/everything?q=" + keyWord + "&apiKey="+apiKey);
            String url = "https://newsapi.org/v2/everything?q=" + keyWord + "&pageSize=100&apiKey="+apiKey;
            URL newsApiUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) newsApiUrl.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            int responseCode = connection.getResponseCode();

            if (responseCode != 200) {
                 return null;
            } else {
                String stringData = "";
                Scanner scanner = new Scanner(newsApiUrl.openStream());
                while (scanner.hasNext()) {
                    stringData += scanner.nextLine();
                }
                scanner.close();
                return stringData;

            }
        } catch(Exception e){
            return null;
        }
    }


}
