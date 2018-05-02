package regressiontest.TFIDF;

import java.io.*;
import java.util.*;

public class TFIDF {


    //TF（词频除以文档单词总数）
    private static HashMap<String, HashMap<String, Float>> allTheTf = new HashMap<String, HashMap<String, Float>>();
    //词频
    private static HashMap<String, HashMap<String, Integer>> allTheNormalTF = new HashMap<String, HashMap<String, Integer>>();


    //建立词典，格式为id,word
    public static void storeDicToFile(String inputfileName,String outputfileName) throws FileNotFoundException, IOException {
        System.out.println("store begin!");
        HashSet<String> dicSet = new HashSet<>();
        File inputFile = new File(inputfileName);
        File outputFile = new File(outputfileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile,false));
        String line = bufferedReader.readLine();

        int id = 0;

        while (line != null) {
            String wordStr = line.split(" ",4)[3];
            String[] wordList = wordStr.split(",");
            for (String word : wordList){
                dicSet.add(word.trim());
            }
            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        for (String word : dicSet){
            bufferedWriter.write(word + " " + id);
            bufferedWriter.newLine();
            System.out.println(word + " " + id);
            id++;
        }

        bufferedWriter.close();
        System.out.println("store success!");
    }

    //加载词典
    public static HashMap<String,String> loadDicFromFile(String file) throws IOException {
        System.out.println("load begin!");
        HashMap<String,String> dicMap = new HashMap<>();
        StringBuffer sb = new StringBuffer();
        InputStreamReader is = new InputStreamReader(new FileInputStream(file), "utf-8");
        BufferedReader br = new BufferedReader(is);
        String line = br.readLine();

        int id = 0;

        while (line != null) {
            String[] wordList = line.split(" ");
            dicMap.put(wordList[0],wordList[1]);
            line = br.readLine();
        }
        br.close();
        System.out.println("load success!");
        return dicMap;
    }

    //统计词频
    public static void wordCount(String inputfileName,String outputfileName) throws FileNotFoundException, IOException {
        System.out.println("count begin!");
        HashMap<String,Integer> dicMap = new HashMap<>();
        File inputFile = new File(inputfileName);
        File outputFile = new File(outputfileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile,false));
        String line = bufferedReader.readLine();

        int id = 0;

        while (line != null) {
            String wordStr = line.split(" ",4)[3];
            String[] wordList = wordStr.split(",");
            for (String word : wordList){
                if (dicMap.containsKey(word.trim())){
                    int oldVaule = dicMap.get(word.trim());
                    dicMap.put(word.trim(),oldVaule+1);
                }else{
                    dicMap.put(word.trim(),1);
                }
            }
            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        Iterator<Map.Entry<String, Integer>> it = dicMap.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Integer> entry = it.next();
            bufferedWriter.write(entry.getKey() + " " + entry.getValue());
            bufferedWriter.newLine();
        }

        bufferedWriter.close();
        System.out.println("count success!");
    }


}
