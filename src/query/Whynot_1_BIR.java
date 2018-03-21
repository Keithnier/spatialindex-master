package query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Vector;

import spatialindex.rtree.IRTree;
import spatialindex.rtree.Line;
import spatialindex.spatialindex.Point;
import spatialindex.storagemanager.DiskStorageManager;
import spatialindex.storagemanager.IBuffer;
import spatialindex.storagemanager.IStorageManager;
import spatialindex.storagemanager.PropertySet;
import spatialindex.storagemanager.TreeLRUBuffer;


public class Whynot_1_BIR{

	public static void main(String[] args) throws Exception{
		if(args.length != 6){
			System.out.println("Usage: LkT tree_file query_file topk fanout buffersize alpha");
			System.exit(-1);
		}

		String tree_file = args[0];			
		String query_file = args[1];		
		int topk = Integer.parseInt(args[2]);
		int fanout = Integer.parseInt(args[3]);
		int buffersize = Integer.parseInt(args[4]);
		double alpha = Double.parseDouble(args[5]);
		
		// Create a disk based storage manager.
		PropertySet ps = new PropertySet();

		ps.setProperty("FileName", tree_file);
		// .idx and .dat extensions will be added.

		Integer pagesize = new Integer(4096*fanout/100);
		ps.setProperty("PageSize", pagesize);
		// specify the page size. Since the index may also contain user defined data
		// there is no way to know how big a single node may become. The storage manager
		// will use multiple pages per node if needed. Off course this will slow down performance.

		ps.setProperty("BufferSize", buffersize);

		IStorageManager diskfile = new DiskStorageManager(ps);

		IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);
		// applies a main memory random buffer on top of the persistent storage manager
		// (LRU buffer, etc can be created the same way).

		// INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
		ps.setProperty("IndexIdentifier", new Integer(1));
		//System.out.println("open IRtree.");
		IRTree irtree = new IRTree(ps, file, false);
		
		FileInputStream queryfis = new FileInputStream(query_file);
		BufferedReader querybr = new BufferedReader(new InputStreamReader(queryfis));
		String line;
		String[] temp;
		
		double[] f = new double[2];
		Vector qwords = new Vector();
		int count = 0;
		int expected_ob = 0;
		Line l = null;
		
		long one_start = 0;
		long one_end = 0;
		long maxone = -1;
		long minone = 100000000;
		double [] time = new double [1005];
				
		long a_start = System.currentTimeMillis();
		while((line = querybr.readLine()) != null){
		    one_start = System.currentTimeMillis();
			System.out.println("query " + count);
			temp = line.split(",");
			qwords.clear();
			
			expected_ob = Integer.parseInt(temp[0]);
			f[0] = Double.parseDouble(temp[1]);
			f[1] = Double.parseDouble(temp[2]);
			Point qpoint = new Point(f);
			for(int j = 3; j < temp.length; j++){
				qwords.add(Integer.parseInt(temp[j]));
			}

			
			
			l = irtree.determine_rank(qwords, qpoint, topk, expected_ob, alpha, 0);
            System.out.println("initial_rank : " + l.initial_rank);
			
			irtree.answeringWhyNot_11(qwords, qpoint, l, 0.5, 1);

		
			
			one_end = System.currentTimeMillis();
			
			time[count] = one_end - one_start;
			
			count++;
			if(count==20) break;
		}
		long a_end = System.currentTimeMillis();
		double avg = (a_end - a_start)*1.0/count;
		double tmp = 0;
		
		for(int ii = 0; ii < count; ii++)
		{
			tmp = tmp+(time[ii]-avg)*(time[ii]-avg);
		}
		System.out.println("Millisecond time cost by Algorithm I : " + avg);
		System.out.println("variance : " + tmp/count);
		System.out.println("average IO: " + (irtree.getIO()-file.getHits())*1.0/count);		
	}
}
