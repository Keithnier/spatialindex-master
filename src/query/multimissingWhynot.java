package query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Vector;

import spatialindex.rtree.IRTree;
import spatialindex.rtree.Line;
import spatialindex.spatialindex.Point;
import spatialindex.storagemanager.DiskStorageManager;
import spatialindex.storagemanager.IBuffer;
import spatialindex.storagemanager.IStorageManager;
import spatialindex.storagemanager.PropertySet;
import spatialindex.storagemanager.TreeLRUBuffer;


public class multimissingWhynot {

	public static void main(String[] args) throws Exception{
        
		int numofquery = 20;
		
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

		
		int[] rank_missing = {301, 1001, 3001};
		int[] numofmissing = {1,3, 10, 30, 100};
		PrintWriter bw = new PrintWriter("Varing_numofmissing_result.txt");
		double c = 1;
	  for (int out_round = 0; out_round<rank_missing.length; out_round++){
          bw.println("**************Missing before "+rank_missing[out_round]+" *********************************");
		  int round_length =4;
		  if(rank_missing[out_round] == 301 || rank_missing[out_round] == 1001) round_length =5;
			
       for(int round = 0; round < round_length; round++) {
    	
	        bw.println();
	        bw.println("=============================================================================");
		    bw.println(numofmissing[round] + " missing objects : ");
		    bw.flush();
		    
		    String query_file= "1000_euro_4word_m301_testmulti.txt";
			//String query_file = "1000_euro_4word_"+numofmissing[round]+"missing_before_"+rank_missing[out_round]+".txt";
			double[] f = new double[2];
			Vector qwords = new Vector();
			int count = 0;
			int topk = 10;
			double alpha = 0.5;
			long one_start = 0;
			long one_end = 0;
			double variance = 0;
			double [] time = new double [1005];
			double io_start = 0,hit_start = 0,io_end =0,hit_end = 0;
			double time_total, io_total, time_min, time_max, avg;
			String line;
			String[] temp;
			HashSet<Integer> missings = new HashSet<Integer> ();
			
			
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
				missings.clear();
				
				for(int i = 1; i <= numofmissing[round]; i++ )
					missings.add(Integer.parseInt(temp[i]));
      			
				//System.out.println(missings.size());
				
				f[0] = Double.parseDouble(temp[numofmissing[round]+1]);
				f[1] = Double.parseDouble(temp[numofmissing[round]+2]);
				Point qpoint = new Point(f);
				for(int j = numofmissing[round]+3; j < temp.length; j++){
					qwords.add(Integer.parseInt(temp[j]));
				}
	

				
				
				io_start = irtree.getIO(); hit_start = file.getHits();
				one_start = System.currentTimeMillis();
				irtree.multimissing_1(qwords, qpoint, topk, alpha, missings, 0.5, c);
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
				missings.clear();
				
				for(int i = 1; i <= numofmissing[round]; i++ )
					missings.add(Integer.parseInt(temp[i]));
      			
				//System.out.println(missings.size());
				
				f[0] = Double.parseDouble(temp[numofmissing[round]+1]);
				f[1] = Double.parseDouble(temp[numofmissing[round]+2]);
				Point qpoint = new Point(f);
				for(int j = numofmissing[round]+3; j < temp.length; j++){
					qwords.add(Integer.parseInt(temp[j]));
				}
	

				
				
				io_start = irtree.getIO(); hit_start = file.getHits();
				one_start = System.currentTimeMillis();
				irtree.multimissing_2(qwords, qpoint, topk, alpha, missings, 0.5, c);
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

			
	}
      
       bw.println();
       bw.println();
       bw.println();
       bw.println();
	  }
       bw.flush();
       bw.close();
	}
}
