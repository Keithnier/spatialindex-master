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


public class Whynot_numberquerykeywords {

	public static void main(String[] args) throws Exception{
        
		int numofquery = 100;
		
		int fanout = 100;
		int buffersize = 4096;
		String tree_file = "1Rtree";	
		PropertySet ps = new PropertySet();
		ps.setProperty("FileName", tree_file);
		Integer pagesize = new Integer(4096*fanout/100);
		ps.setProperty("PageSize", pagesize);
		ps.setProperty("BufferSize", buffersize);
		IStorageManager diskfile = new DiskStorageManager(ps);
		IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);
		ps.setProperty("IndexIdentifier", new Integer(1));
		IRTree irtree = new IRTree(ps, file, false);

		int[] number_keywords = {2,4,8,16,32};
		PrintWriter bw = new PrintWriter("Varing_num_keywords_result.txt");
		
		
       for(int round = 0; round <=4; round++) {
    	
	        bw.println();
	        bw.println("=============================================================================");
		    bw.println(number_keywords[round] + "keywords : ");
			String query_file = "1000_euro_"+number_keywords[round]+"word_m101.txt";
			double[] f = new double[2];
			Vector qwords = new Vector();
			int count = 0;
			int expected_ob = 0;
			int topk = 10;
			Line l = null;
			double alpha = 0.5;
			long one_start = 0;
			long one_end = 0;
			double variance = 0;
			double [] time = new double [1005];
			double io_start = 0,hit_start = 0,io_end =0,hit_end = 0;
			double time_total, io_total, time_min, time_max, avg;
			String line;
			String[] temp;
			
			

			//Alg I
			bw.println("Alg I : ");
			FileInputStream queryfis1 = new FileInputStream(query_file);
			BufferedReader querybr1 = new BufferedReader(new InputStreamReader(queryfis1));
			count = 0;
			time_total = 0;
			io_total = 0;
			time_min = Double.MAX_VALUE;
		    time_max = -1;
		    variance = 0;
		    
			while((line = querybr1.readLine()) != null){
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
				
				l = irtree.determine_rank(qwords, qpoint, topk, expected_ob, alpha, 0);
				irtree.answeringWhyNot_111(qwords, qpoint, l, 0.5, 1);
				
				one_end = System.currentTimeMillis();
				io_end = irtree.getIO(); hit_end = file.getHits();
				
				time[count] = one_end - one_start;
				io_total +=  (io_end - io_start) - (hit_end - hit_start);
				time_total += time[count];
				
				count++;
			    if(count==numofquery) break;
			}

			avg = time_total/count;
			for(int ii = 0; ii < count; ii++)
			{
				variance += (time[ii]-avg)*(time[ii]-avg);				
				if(time[ii] > time_max) time_max = time[ii];
				if(time[ii] < time_min) time_min = time[ii];
			}
			
			bw.println("Milliseconds by Algorithm I : " + avg);
			bw.println("Average IO: " + (io_total/count));
			bw.println("Time Variance : " + (variance/count));
			bw.println("max_time :" + time_max + "  min_time :"+ time_min );
			bw.flush();
			
			
			bw.println();
			
			
			//Alg 2
			bw.println("Alg II  : ");
			FileInputStream queryfis2 = new FileInputStream(query_file);
			BufferedReader querybr2 = new BufferedReader(new InputStreamReader(queryfis2));
			count = 0;
			time_total = 0;
			io_total = 0;
			time_min = Double.MAX_VALUE;
		    time_max = -1;
		    variance = 0;
		    
			while((line = querybr2.readLine()) != null){
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
				
				time[count] = one_end - one_start;
				io_total +=  (io_end - io_start) - (hit_end - hit_start);
				time_total += time[count];
				
				count++;
			    if(count==numofquery) break;
			}

			avg = time_total/count;
			for(int ii = 0; ii < count; ii++)
			{
				variance += (time[ii]-avg)*(time[ii]-avg);				
				if(time[ii] > time_max) time_max = time[ii];
				if(time[ii] < time_min) time_min = time[ii];
			}
			
			bw.println("Milliseconds by Algorithm II : " + avg);
			bw.println("Average IO: " + (io_total/count));
			bw.println("Time Variance : " + (variance/count));
			bw.println("max_time :" + time_max + "  min_time :"+ time_min );
			bw.flush();
			
			
			
			bw.println();
			
			//Alg 3
			bw.println("Alg III  : ");
			FileInputStream queryfis3 = new FileInputStream(query_file);
			BufferedReader querybr3 = new BufferedReader(new InputStreamReader(queryfis3));
			count = 0;
			time_total = 0;
			io_total = 0;
			time_min = Double.MAX_VALUE;
		    time_max = -1;
		    variance = 0;
		    
			while((line = querybr3.readLine()) != null){
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
				
				irtree.answeringWhyNot_3_new(qwords, qpoint, topk, expected_ob, alpha, 0.5, 1);
				
				one_end = System.currentTimeMillis();
				io_end = irtree.getIO(); hit_end = file.getHits();
				
				time[count] = one_end - one_start;
				io_total +=  (io_end - io_start) - (hit_end - hit_start);
				time_total += time[count];
				
				count++;
			    if(count==numofquery) break;
			}

			avg = time_total/count;
			for(int ii = 0; ii < count; ii++)
			{
				variance += (time[ii]-avg)*(time[ii]-avg);				
				if(time[ii] > time_max) time_max = time[ii];
				if(time[ii] < time_min) time_min = time[ii];
			}
			
			bw.println("Milliseconds by Algorithm III : " + avg);
			bw.println("Average IO: " + (io_total/count));
			bw.println("Time Variance : " + (variance/count));
			bw.println("max_time :" + time_max + "  min_time :"+ time_min );
			bw.flush();
			
	}
       bw.flush();
       bw.close();
	}
}
