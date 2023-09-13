package org.csci5408.com;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

public class TransformationEngine {
    /**
     * This method is used to read data from json files, clean the data and finally load the data to a Mongo database.
     * @param keyWords ArrayList<String>. List of keywords.
     * @param newsArticlesCount ArrayList<Integer>. List of the count of all the news articles per keyword.
     */
    public void transformRawData(ArrayList<String> keyWords, ArrayList<Integer> newsArticlesCount){
        try {
            ArrayList<Integer> documentCount = new ArrayList<Integer>();
            System.out.println("Transformation Engine running.......");
            ArrayList<String> rawDataArray = new ArrayList<String>();
            System.out.println("Reading json data from files.....");
            for (int i = 0; i < keyWords.size(); i++) {
                String  stringArray = readRawDataFromJsonFIle(keyWords.get(i), newsArticlesCount.get(i));

                rawDataArray.add(stringArray);
            }

            ArrayList<ArrayList<Document>>  cleanDataArray = cleanRawData(rawDataArray, newsArticlesCount);

            if (cleanDataArray != null && cleanDataArray.size() > 0) {
                documentCount = loadCleanedDataToDatabase(cleanDataArray, keyWords);
            }
            printSummary(newsArticlesCount, documentCount);
            //return documentCount;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            //return null;
        }
    }

    /**
     * This method is used to read news articles from a json file.
     * @param keyWord String of the keyword.
     * @param newsArticles Count of the news articles.
     * @return String. Returns the data read from a json file.
     */
    public String readRawDataFromJsonFIle(String keyWord, Integer newsArticles){
        String stringData = new String();
        try {
            for(int i = 1; i <= newsArticles/5; i++) {
                File myObj = new File("./" + keyWord + "/" + keyWord + i + ".json");
                Scanner myReader = new Scanner(myObj);
                stringData += myReader.nextLine();
                myReader.close();
            }
            return stringData;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            return stringData;
        }
    }

    /**
     * This method id used to clean the news article's data.
     * reference - https://stackoverflow.com/questions/22006218/replace-emoji-unicode-symbol-using-regexp-in-javascript
     * @param rawDataArray Array of raw data read from the json files.
     * @param newsArticlesCount Array of count of news articles per keyword.
     * @return returns the cleaned news article.
     *
     */
    public ArrayList<ArrayList<Document>>  cleanRawData(ArrayList<String> rawDataArray, ArrayList<Integer> newsArticlesCount){
        ArrayList<ArrayList<Document>> cleanedDocumentArray = new ArrayList<ArrayList<Document>>();
        try {
            System.out.println("Cleaning raw json data......");
            for (int i = 0; i < rawDataArray.size(); i++) {
                //JSONArray cleanedData = new JSONArray();
//                String stringSplit = rawDataArray.get(i).replaceAll("\\{\"title\":\"","").replaceAll("\",\"content\":\"","").replaceAll("\"}]","").substring(1);
                String[] stringSplit = rawDataArray.get(i).split("\\{\"title\":\"");
                ArrayList<Document> innerDocumentList = new ArrayList<Document>();
                for(int j = 1; j <= newsArticlesCount.get(i); j++){
                    try {
                        String rawTitle = stringSplit[j].split("\",\"content\":\"")[0];
                        String cleanedTitle = rawTitle.replaceAll("\\<.*?\\>", "").replaceAll("[^a-zA-Z0-9\\s'-]", "").replaceAll("[\\uD83C-\\uDBFF\\uDC00-\\uDFFF]+", "").replaceAll("\\s+", " ").trim();

                        String rawContent = stringSplit[j].split("\",\"content\":\"")[1].split("\"}")[0];
                        //String cleanedContent = rawContent.split("… \\[+")[0].replaceAll("\\<.*?\\>", "").replaceAll("[^a-zA-Z0-9\\s'-]", "").replaceAll("[\\uD83C-\\uDBFF\\uDC00-\\uDFFF]+", "").replaceAll("\\s+", " ").trim() + "… \\[+" + rawContent.split("… \\[+")[1];
                        String cleanedContent = rawContent.split("…")[0].replaceAll("\\<.*?\\>", "").replaceAll("[^a-zA-Z0-9\\s'-]", "").replaceAll("[\\uD83C-\\uDBFF\\uDC00-\\uDFFF]+", "").replaceAll("\\s+", " ").trim() + "…";
                        innerDocumentList.add(new Document("title", cleanedTitle).append("content", cleanedContent));
                    }
                    catch(Exception e){
                    }

                }
                cleanedDocumentArray.add(innerDocumentList);
            }
            return cleanedDocumentArray;
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * This method is used to load the cleaned news articles to mongodb.
     * @param cleanedDataArray Cleaned documents.
     * @param keyWords List of keywords.
     * @return returns the document count.
     */
    public ArrayList<Integer> loadCleanedDataToDatabase (ArrayList<ArrayList<Document>> cleanedDataArray, ArrayList<String> keyWords){
        try{
            ArrayList<Integer> documentCount = new ArrayList<Integer>();
            String dbConnectionString = Common.getDataFromEnv("DATABASE_CONNECTION");
            String dbName = Common.getDataFromEnv("DATABASE_NAME");
            MongoClient mongoClient = MongoClients.create(dbConnectionString + "?retryWrites=true&w=majority");
            MongoDatabase myDatabase = mongoClient.getDatabase(dbName);
            System.out.println("Connection to MongoDb established....\nNew database created: "+dbName + ".....");

            for(int i =0; i < cleanedDataArray.size(); i++) {
                MongoCollection<Document> dataCollection = myDatabase.getCollection(keyWords.get(i));
                dataCollection.insertMany(cleanedDataArray.get(i));

                System.out.println("New collection: \""+keyWords.get(i)+"\"\tDocuments inserted: "+cleanedDataArray.get(i).size());
                documentCount.add(cleanedDataArray.get(i).size());
            }
            System.out.println("MongoDb connection closed....");
            mongoClient.close();
            return documentCount;
        }
        catch(Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * This method is used to print a summary of the ETL process.
     * @param newsArticlesCount count of the news articles
     * @param documentCount count of the documents inserted.
     */
    public void printSummary(ArrayList<Integer> newsArticlesCount, ArrayList<Integer> documentCount){
        if(documentCount!= null && !documentCount.isEmpty()) {
            long totalNewsArticles = newsArticlesCount.stream().mapToLong(Integer::longValue).sum();
            long totalDocuments = documentCount.stream().mapToLong(Integer::longValue).sum();
            System.out.println("ETL Program execution completed!\n");
            System.out.println("\t-------------------------------------------");
            System.out.println("\t                  SUMMARY                  ");
            System.out.println("\t-------------------------------------------");
            System.out.println("\tDirectories created:\t"+newsArticlesCount.size());
            System.out.println("\tJson files created:\t"+totalNewsArticles/5);
            System.out.println("\tTotal News Articles:\t"+totalNewsArticles);
            System.out.println("\tMongoDB Database created:\t1");
            System.out.println("\tMongoDB Collections created:\t"+newsArticlesCount.size());
            System.out.println("\tAverage Documents per collection:\t"+Math.round((float)totalDocuments/documentCount.size() * 10)/10.0);
            System.out.println("\tTotal documents inserted:\t"+totalDocuments);
            System.out.println("\tLost news articles:\t"+(totalNewsArticles-totalDocuments));
            System.out.println("\t-------------------------------------------");
        }
    }
}
