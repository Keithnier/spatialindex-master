package util;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * @author Pulin Xie
 * @description 常量工具类，配置类
 * @date 2018.5.3
 */
public class Constants {
    public static String DATA_DIRECTORY = "twitter";
    public static String DATA_TEST_DIRECTORY = DATA_DIRECTORY + File.separator + "test";
    public static String PROPERTY_DIRECTORY = "src/util";

    public static HashMap<String, Integer> dictionary;

    public static String reduceData(String dataFilePath, int nRow) throws IOException {
        HashMap<Integer, String> dataName = new HashMap<>();
        HashSet<Integer> lines = new HashSet<>();
        Random random = new Random();
        dataName.put(160000, "160K.txt");
        dataName.put(320000, "320K.txt");
        dataName.put(640000, "640K.txt");
        dataName.put(1280000, "1280K.txt");
        // 构造精简的测试文件的名字
        String testName = dataName.get(nRow);
        if (testName == null || testName.equals("")) {
            System.err.println("nRow Error!");
            System.exit(-1);
        }
        String testFileName = Constants.DATA_TEST_DIRECTORY + File.separator + testName;
        File testFile = new File(testFileName);
        if (testFile.exists()) testFile.delete();
//        System.out.println(testFileName);
        // 精简文件
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath)));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(testFileName)));

        String line;
        int qline;


        for (int i = 0; i < nRow; i++) {
            qline = random.nextInt(13000000);
            if (lines.contains(qline))
                i--;
            else
                lines.add(qline);
        }

        int count = 0;
        int nWrite = 0;
        while ((line = in.readLine()) != null && nWrite < nRow) {
            if (lines.contains(count)) {
                nWrite++;
                System.out.println(count);
                out.write(line + "\n");
                out.flush();
            }
            count++;
        }

        out.close();
        in.close();

        return testFileName;
    }


    public static void main(String[] args) throws IOException {
        reduceData("twitter/7day.txt", 160000);
        reduceData("twitter/7day.txt", 320000);
        reduceData("twitter/7day.txt", 640000);
        reduceData("twitter/7day.txt", 1280000);
    }
}
