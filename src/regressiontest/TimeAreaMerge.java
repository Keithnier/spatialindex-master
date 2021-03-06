package regressiontest;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class TimeAreaMerge {

    private static final String TYPE = "hour";//取值有hour和day

    public static void main(String[] args) {
        try {
            timeAreaMerge1("");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String,List<String>> timeAreaMerge1(String filePath) throws IOException {
        Map<String,List<String>> resultMap = new LinkedHashMap<>();
        BufferedReader bufferedReader = fileReader(filePath);
        String line = bufferedReader.readLine();
        while (line != null){
            String timestamp = timeChange(line.split(",",3)[1],TYPE);
            if (resultMap.containsKey(timestamp)){
                List<String> valueList = resultMap.get(timestamp);
                valueList.add(line);
                resultMap.put(timestamp,valueList);
            }else{
                List<String> valueList = new LinkedList<>();
                valueList.add(line);
                resultMap.put(timestamp,valueList);
            }
            line = bufferedReader.readLine();
        }
        return resultMap;
    }

    public static Map<String,List<String>> timeAreaMerge2(String filePath) throws IOException {
        Map<String,List<String>> resultMap = new LinkedHashMap<>();
        BufferedReader bufferedReader = fileReader(filePath);
        int lineNum = countLineNumber(bufferedReader);
        String line = bufferedReader.readLine();
        while (line != null){
            //TODO

            line = bufferedReader.readLine();
        }
        return resultMap;
    }

    private static String timeChange(String timestamp,String type){
        SimpleDateFormat sdf = null;
        if (type.equals("hour")){
             sdf = new SimpleDateFormat("yyyy-mm-dd-HH");
        }else if (type.equals("day")){
            sdf = new SimpleDateFormat("yyyy-mm-dd");
        }
        String result = sdf.format(new Date(Long.parseLong(timestamp)));
        return result;
    }

    private static BufferedReader fileReader(String filePath) throws FileNotFoundException {
        File file = new File(filePath);
        if (file.exists() == false){
            System.err.println("Error: File not exists!");
        }
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        return bufferedReader;
    }

    private static int countLineNumber(BufferedReader bufferedReader) throws IOException {
        String line = bufferedReader.readLine();
        int lineNum = 0;
        while (line != null){
            lineNum++;
            line = bufferedReader.readLine();
        }
        return lineNum;
    }

}
