package regressiontest;

import java.io.*;
import java.util.Random;
import java.util.Scanner;
import java.util.zip.GZIPOutputStream;

public class BTreeDataGenerator {
    public static void main(String[] args) throws IOException {
        System.out.println("Usage: fileName(�����ļ���) docNum(�ĵ���Ŀ) wordRange(�ؼ���id��Χ)");
        Scanner in = new Scanner(System.in);
        String line = in.nextLine();
        while(line == null || line.equals(""))
            line = in.nextLine();
        String[] params = line.split(" ");
        String filepath = System.getProperty("user.dir") + File.separator + "src" +
                File.separator + "regressiontest" + File.separator + "test3" + File.separator + params[0] + ".gz";
        File file = new File(filepath);
        if(file.exists()) {
            file.delete();
        }
        file.createNewFile();
        GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(file));
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(gos)));
        int docNum = Integer.parseInt(params[1]);
        int wordRange = Integer.parseInt(params[2]);
        Random rand = new Random();
        for(int i = 0; i < docNum; i++) {
            StringBuilder line1 = new StringBuilder();
            line1.append(String.valueOf(i));
            line1.append(",");
            line1.append(String.valueOf(rand.nextDouble()));
            line1.append(",");
            line1.append(String.valueOf(rand.nextDouble()));
            line1.append(",");
            int wordNum = rand.nextInt(20);
            for(int j = 0; j < wordNum; j++) {
                line1.append(String.valueOf(rand.nextInt(wordRange)));
                line1.append(" ");
                float f = rand.nextFloat();
                line1.append(String.valueOf(rand.nextFloat()));
                line1.append(",");
            }
            pw.println(line1.toString());
        }
        pw.close();
    }
}
