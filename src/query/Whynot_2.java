package query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Vector;

import spatialindex.rtree.IRTree;
import spatialindex.rtree.Line;
import spatialindex.spatialindex.Point;
import spatialindex.storagemanager.DiskStorageManager;
import spatialindex.storagemanager.IBuffer;
import spatialindex.storagemanager.IStorageManager;
import spatialindex.storagemanager.PropertySet;
import spatialindex.storagemanager.TreeLRUBuffer;


public class Whynot_2 {

	public static void main(String[] args) throws Exception{
        
		int fanout = 100;
		int buffersize = 4096;
		double alpha = 0.5;
		String tree_file = "1Rtree";	
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

		int[] top_k = {3,10,30,100,300};
		PrintWriter bw = new PrintWriter("Varing_k0_Alg2_result.txt");
       for(int round = 0; round <=4; round++) {
    	
        bw.println();
		
		int topk = top_k[round];
		
    	bw.println("k0 = " + topk);
		String query_file = "1000_euro_4word_m"+(topk*10+1)+".txt";
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
		double [] time = new double [1005];
		double [] io = new double[1005];
		double io_start = 0;
		double hit_start = 0;
		double io_end =0;
		double hit_end = 0;

		while((line = querybr.readLine()) != null){
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

			
			io_start = irtree.getIO(); hit_start = file.getHits();
			one_start = System.currentTimeMillis();
			irtree.answeringWhyNot_222(qwords, qpoint, topk, expected_ob, alpha, 0.5, 1);
			one_end = System.currentTimeMillis();
			io_end = irtree.getIO(); hit_end = file.getHits();
			io[count] =  (io_end - io_start) - (hit_end - hit_start);
			time[count] = one_end - one_start;
			
			count++;
			if(count==100) break;
		}
		
		double time_total = 0;
		double io_total = 0;
		double tmp = 0;
		double max_one = -1;
		double min_one = Double.MAX_VALUE;
		for(int ii = 0; ii < count; ii++)
		{
			time_total += time[ii];
			io_total += io[ii];
			if(time[ii] > max_one) max_one = time[ii];
			if(time[ii] < min_one) min_one = time[ii];
		}
		
		double avg = time_total/count;
		for(int ii = 0; ii < count; ii++)
		{
			tmp = tmp+(time[ii]-avg)*(time[ii]-avg);
		}
		
		bw.println("Millisecond time cost by Algorithm I : " + avg);
		bw.println("average IO: " + (io_total/count));
		bw.println("variance : " + tmp/count);
		bw.println("max_time :" + max_one + "  min_time :"+min_one );
		bw.flush();
	}
       bw.flush();
       bw.close();
	}
}
