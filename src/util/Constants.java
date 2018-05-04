package util;

import java.io.*;
import java.util.HashMap;

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
        // 构造精简的测试文件的名字
        String testFileName = dataFilePath.substring(0, dataFilePath.lastIndexOf(".")) + "_back" +
                dataFilePath.substring(dataFilePath.lastIndexOf("."));
        File testFile = new File(testFileName);
        testFileName = Constants.DATA_TEST_DIRECTORY + File.separator + testFile.getName();
        testFile = new File(testFileName);
        if(testFile.exists()) testFile.delete();
//        System.out.println(testFileName);
        // 精简文件
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath)));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(testFileName)));

        String line;
        int count = 0;
        while ((line = in.readLine()) != null && count++ < nRow) {
            out.write(line);
            out.write("\n");
        }
        out.flush();
        out.close();
        in.close();

        return testFileName;
    }

    public static void main(String[] args) throws IOException {
        reduceData("twitter/1day_after2.txt", 10000);
    }
}
