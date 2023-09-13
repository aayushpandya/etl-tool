package org.csci5408.com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();

        System.out.println(">>>Starting CSCI 5408 Assignment2 ETL program");

        ExtractionEngine extractionObj = new ExtractionEngine();

        ArrayList<String> keyWords = new ArrayList<String>(Arrays.asList("canada", "university", "dalhousie", "halifax", "canada education", "moncton", "hockey", "fredericton", "celebration"));

        extractionObj.extractData(keyWords);

        long endTime   = System.nanoTime();
        long totalTime = (endTime - startTime)/1000000;
        System.out.println("\n\tTotal execution time:\t"+totalTime + " ms");

    }
}