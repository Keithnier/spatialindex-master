package regressiontest.TFIDF;

import java.io.IOException;
import java.util.HashMap;

public class Main {
    private static String testFileName = "D:\\爬虫数据备份\\test.txt";
    private static String fileName = "D:\\爬虫数据备份\\text_after2.txt";
    private static String outputfileName = "D:\\爬虫数据备份\\dic2.txt";

    public static void main(String[] args) throws IOException {
//        TFIDF.storeDicToFile(fileName,outputfileName);
//        HashMap<String,String> dic = TFIDF.loadDicFromFile(outputfileName);
        TFIDF.wordCount(fileName,outputfileName);
    }
}
