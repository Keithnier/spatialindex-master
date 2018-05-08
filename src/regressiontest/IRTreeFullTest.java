package regressiontest;

import spatialindex.rtree.IRTree;

public class IRTreeFullTest {


    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: IRTree docsFileName btreeName indexFileName fanout buffersize.");
            System.exit(-1);
        }
        String docsFileName = args[0];
        String btreeName = args[1];
        String indexFileName = args[2];
        int fanout = Integer.parseInt(args[3]);
        int buffersize = Integer.parseInt(args[4]);
        // 首次使用，boolean值为true
        IRTree.build(docsFileName, btreeName, indexFileName, fanout, buffersize,false);
    }
}
