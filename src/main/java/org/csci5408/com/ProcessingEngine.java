package org.csci5408.com;



import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

public class ProcessingEngine {

    TransformationEngine transformationObj = new TransformationEngine();

    /**
     * This method is used to process the news data from the API. 'title' and 'content' are extracted from the raw data and stored in a json file.
     * @param rawDataList ArrayList<String>. List of raw data extracted from the API.
     * @param keyWords ArrayList<String>. List of keywords.
     */
    public void procesNewsData (ArrayList<String> rawDataList, ArrayList<String> keyWords){
        try{
            System.out.println("Data-Processing Engine running......");
            System.out.println("Processing news data......\n...\n...");
            ArrayList<Integer> fileCount = new ArrayList<Integer>();
            for(int i = 0; i < rawDataList.size(); i++){

                String filteredData = "[";
                String[] titleSplit = rawDataList.get(i).split("\"title\":");
                int count = 1;
                int countNewsArticles = 100;
                for(int j = 1; j <= 100; j++){
                    try {
                        String title = titleSplit[j].split(",\"description")[0].replace("\"","").trim();
                        String content = titleSplit[j].split("\"content\":")[1].split("chars]\"")[0] + "chars]";
                        filteredData += "{\"title\":\"" + title + "\",\"content\":\"" + content + "\"}";
                        if (j % 5 == 0) {
                            filteredData += "]";
                            writeDataToJsonFile(filteredData, keyWords.get(i), count, count == 1);
                            filteredData = "[";
                            count++;
                        } else
                            filteredData += ",";
                    }
                    catch (Exception e){
                        filteredData += "]";
                        writeDataToJsonFile(filteredData, keyWords.get(i), count, count == 1);

                        countNewsArticles = j;
                        System.out.println(keyWords.get(i));
                        System.out.println(j);
                        break;
                    }
                }
                fileCount.add(countNewsArticles);
                System.out.println(countNewsArticles/5 + " Json files created in the \"" +keyWords.get(i) + "\" directory, News Articles added: " + countNewsArticles);
            }
            transformationObj.transformRawData(keyWords, fileCount);
           // return fileCount;
        } catch (Exception e){
            //return null;
        }
    }

    /**
     * This method is used to write data to a json file.
     * @param data String. Data to be written in the file.
     * @param fileName String. Name of the file.
     * @param count Integer. Count of the file.
     * @param createNewDirectory Boolean. Indicates whether to create a new directory.
     */
    public void writeDataToJsonFile (String data, String fileName, Integer count, Boolean createNewDirectory){
        try{
            File newDir = new File("./"+fileName);
            if (!newDir.exists()){
                newDir.mkdir();
            }
            //System.out.println(data.toString());
            FileWriter file = new FileWriter("./"+fileName+"/"+fileName + count + ".json");
            file.write(data);
            file.close();
        }
        catch(Exception e){
            //
            System.out.println(e.getMessage());
        }
    }
}
