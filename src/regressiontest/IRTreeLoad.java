package regressiontest;

import spatialindex.rtree.IRTree;
import spatialindex.spatialindex.*;
import spatialindex.storagemanager.*;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.Scanner;
import java.util.StringTokenizer;

public class IRTreeLoad {
    public static void main(String[] args) {
        System.out.println("Load Start!!");
        System.out.println("����������: input_file tree_file capacity query_type invertedIndex_filename");
        Scanner in = new Scanner(System.in);
        String input = in.nextLine();
        String[] inputList = input.split(" ");
        new IRTreeLoad(inputList);
    }

    IRTreeLoad(String[] inputList) {
        try {

            LineNumberReader lr = null;

            lr = new LineNumberReader(new FileReader(inputList[0]));

            // �����洢����������
            PropertySet ps = new PropertySet();
            // ����ļ����ڣ�����д
            ps.setProperty("Overwrite", new Boolean(true));
            // �ļ��� ���.idx�ļ���.dat�ļ�
            ps.setProperty("FileName", inputList[1]);
            // ����ҳ���С
            ps.setProperty("PageSize", new Integer(4096));

            // ѡ����̴洢������
            IStorageManager diskfile = new DiskStorageManager(ps);

            // ���ڴ����̵õ�������
            IBuffer file = new RandomEvictionsBuffer(diskfile, 10, false);

            // ����IRTree����
            PropertySet ps2 = new PropertySet();
            // ʹ��RTree��ͨ�����ã�����������������
            ps2.setProperty("BufferSize", new Integer(4096));
            ps2.setProperty("PageSize", new Integer(4096));
            ps2.setProperty("FileName", new String(inputList[4]));

            // ����IRTree
            ISpatialIndex tree = new IRTree(ps2, file, false);

            // ����
            int id, op;
            int count = 0;
            double x1, x2, y1, y2;
            double[] f1 = new double[2];
            double[] f2 = new double[2];

            long start = System.currentTimeMillis();
            String line = lr.readLine();

            while(line != null) {
                StringTokenizer st = new StringTokenizer(line);
                op = new Integer(st.nextToken()).intValue();
                id = new Integer(st.nextToken()).intValue();
                x1 = new Double(st.nextToken()).doubleValue();
                y1 = new Double(st.nextToken()).doubleValue();
                x2 = new Double(st.nextToken()).doubleValue();
                y2 = new Double(st.nextToken()).doubleValue();

                if (op == 0)
                {
                    //delete

                    f1[0] = x1; f1[1] = y1;
                    f2[0] = x2; f2[1] = y2;
                    Region r = new Region(f1, f2);

                    if (tree.deleteData(r, id) == false)
                    {
                        System.err.println("Cannot delete id: " + id + " , count: " + count + ".");
                        System.exit(-1);
                    }
                }
                else if (op == 1)
                {
                    //insert

                    f1[0] = x1; f1[1] = y1;
                    f2[0] = x2; f2[1] = y2;
                    Region r = new Region(f1, f2);

                    String data = r.toString();

                    //�����ݴ������ݿ������棬�ڴ��д�Ϊnull
                    tree.insertData(null, r, id);
                    // example of passing a null pointer as the associated data.
                }
                else if (op == 2)
                {
                    //query

                    f1[0] = x1; f1[1] = y1;
                    f2[0] = x2; f2[1] = y2;

                    MyVisitor vis = new MyVisitor();

                    if (inputList[3].equals("intersection"))
                    {
                        Region r = new Region(f1, f2);
                        tree.intersectionQuery(r, vis);
                        // this will find all data that intersect with the query range.
                    }
                    else if (inputList[3].equals("10NN"))
                    {
                        Point p = new Point(f1);
                        tree.nearestNeighborQuery(10, p, vis);
                        // this will find the 10 nearest neighbors.
                    }
                    else
                    {
                        System.err.println("Unknown query type.");
                        System.exit(-1);
                    }
                }

                if ((count % 1000) == 0) System.err.println(count);

                count++;
                line = lr.readLine();
            }

            long end = System.currentTimeMillis();

            System.err.println("Operations: " + count);
            System.err.println(tree);
            System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);

            // since we created a new RTree, the PropertySet that was used to initialize the structure
            // now contains the IndexIdentifier property, which can be used later to reuse the index.
            // (Remember that multiple indices may reside in the same storage manager at the same time
            //  and every one is accessed using its unique IndexIdentifier).
            Integer indexID = (Integer) ps2.getProperty("IndexIdentifier");
            System.err.println("Index ID: " + indexID);

            boolean ret = tree.isIndexValid();
            if (ret == false) System.err.println("Structure is INVALID!");

            // flush all pending changes to persistent storage (needed since Java might not call finalize when JVM exits).
            tree.flush();
            ((IRTree)tree).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }// IRTreeLoad

    class MyVisitor implements IVisitor
    {
        public void visitNode(final INode n) {}

        public void visitData(final IData d)
        {
            System.out.println(d.getIdentifier());
            // the ID of this data entry is an answer to the query. I will just print it to stdout.
        }
    }
}
