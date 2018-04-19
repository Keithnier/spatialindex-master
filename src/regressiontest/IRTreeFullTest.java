package regressiontest;

import spatialindex.rtree.BtreeStore;
import spatialindex.rtree.IRTree;
import spatialindex.storagemanager.*;

import java.io.File;

public class IRTreeFullTest {
//    public static void main(String[] args) throws Exception {
//        if (args.length != 4)
//        {
//            System.err.println("Usage: IRTree docstore tree_file fanout buffersize.");
//            System.exit(-1);
//        }
//        String docfile = args[0];
//        String treefile = args[1];
//        int fanout = Integer.parseInt(args[2]);
//        int buffersize = Integer.parseInt(args[3]);
//
//        // 输入文档管理
//        BtreeStore docstore = new BtreeStore(docfile, false);
//        // 配置属性
//        // Create a disk based storage manager.
//        PropertySet ps = new PropertySet();
//
//        ps.setProperty("FileName", treefile);
//        // .idx and .dat extensions will be added.
//
//        Integer i = new Integer(4096*fanout/100);
//        ps.setProperty("PageSize", i);
//        // specify the page size. Since the index may also contain user defined data
//        // there is no way to know how big a single node may become. The storage manager
//        // will use multiple pages per node if needed. Off course this will slow down performance.
//
//        ps.setProperty("BufferSize", buffersize);
//
//        // 索引文件管理
//        IStorageManager diskfile = new DiskStorageManager(ps);
//
//        IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);
//        // applies a main memory random buffer on top of the persistent storage manager
//        // (LRU buffer, etc can be created the same way).
//        // 构建IRTree
//        IRTree irtree = new IRTree(ps, file, false);
//
//
//        long start = System.currentTimeMillis();
//
//        irtree.build("src/regressiontest/test3/dataOfBtree.gz", treefile, fanout, buffersize);
//        irtree.buildInvertedIndex(docstore);
//
//        long end = System.currentTimeMillis();
//        boolean ret = irtree.isIndexValid();
//        if (ret == false) System.err.println("Structure is INVALID!");
//        irtree.close();
//
//        System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);
//
//    }

    public static void main(String[] args) throws Exception {
        if(args.length != 5) {
            System.err.println("Usage: IRTree docsFileName btreeName indexFileName fanout buffersize.");
            System.exit(-1);
        }
        String docsFileName = args[0];
        String btreeName = args[1];
        String indexFileName = args[2];
        int fanout = Integer.parseInt(args[3]);
        int buffersize = Integer.parseInt(args[4]);

        /**
         * 1. 用BTree管理docs文件集
         * 2. 利用docs文件集构建RTree索引层
         * 3. 利用BTree的信息构建倒排索引
         */
        //1. BTree管理docs
        BtreeStore bs = BtreeStore.process(docsFileName, btreeName);
        // 2. 构造索引层
            //索引文件管理器，磁盘
        PropertySet ps = new PropertySet();
            // .idx，.dat文件将被创建
        ps.setProperty("FileName", indexFileName);

        Integer pageSize = new Integer(4096 * fanout / 100);
        ps.setProperty("PageSize", pageSize);

        ps.setProperty("BufferSize", buffersize);

        IStorageManager diskfile = new DiskStorageManager(ps);

        IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);
        
        long start = System.currentTimeMillis();
            // 先填充RTree
        docsFileName = System.getProperty("user.dir") + File.separator + "src" +
                File.separator + "regressiontest" + File.separator + "test3" + File.separator + docsFileName + ".gz";
        IRTree.build(docsFileName, indexFileName, fanout, buffersize);
        IRTree irTree = new IRTree(ps, diskfile, false);
            // 创建倒排索引
        irTree.buildInvertedIndex(bs);
        long end = System.currentTimeMillis();

        boolean ret = irTree.isIndexValid();
        if (ret == false) System.err.println("Structure is INVALID!");
        irTree.close();

        System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);
    }
}
