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


public class Whynot_datasize {

	public static void main(String[] args) throws Exception{

        int numofquery = 1000;
        
		PrintWriter bw = new PrintWriter("Varing_datasize_result.txt");
		for(int datasize = 200; datasize<=1800; datasize+= 400){
		bw.println("GN"+datasize + " : ");
		bw.flush();
		String tree_file = "reorder_GN" + datasize + "Rtree";
		String query_file = "1000_GN" + datasize +"_4word_m101.txt";
		
		int topk = 10;
		int fanout = 100;
		int buffersize = 4096;
		double alpha = 0.5;

		PropertySet ps = new PropertySet();
		ps.setProperty("FileName", tree_file);
		Integer pagesize = new Integer(4096*fanout/100);
		ps.setProperty("PageSize", pagesize);
		ps.setProperty("BufferSize", buffersize);
		IStorageManager diskfile = new DiskStorageManager(ps);
		IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);
		ps.setProperty("IndexIdentifier", new Integer(1));
		IRTree irtree = new IRTree(ps, file, false);
		
		String line;
		String[] temp;
		
		double[] f = new double[2];
		Vector qwords = new Vector();
		int count = 0;
		int expected_ob = 0;
		Line l = null;
		double[] time = new double[1005];
		double io_total;
		double time_total;
		double io_start,io_end,hit_start,hit_end;
		double avg;
		double tmp;
        double time_min,time_max;
        

        /*
        //Alg I
		FileInputStream queryfis1 = new FileInputStream(query_file);
		BufferedReader querybr1 = new BufferedReader(new InputStreamReader(queryfis1));
		bw.println("Alg I : ");
		count = 0;
		io_total = 0;
		time_total = 0;
		tmp = 0;
		time_min = Double.MAX_VALUE;
		time_max = -1;
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

			
			io_start=irtree.getIO(); hit_start=file.getHits();
			long a_start = System.currentTimeMillis();
			l = irtree.determine_rank(qwords, qpoint, topk, expected_ob, alpha, 0);
			irtree.answeringWhyNot_111(qwords, qpoint, l, 0.5, 1);
			long a_end = System.currentTimeMillis();
			io_end = irtree.getIO(); hit_end = file.getHits();
			time[count] = a_end - a_start;
			time_total += time[count];
			io_total += (io_end - io_start) - (hit_end - hit_start);
			
			count ++;
			if(count == numofquery) break;
		}
		avg = time_total/count;
		for(int ii = 0; ii<count; ii++){
			tmp += (time[ii]-avg)*(time[ii]-avg);
		    if(time[ii]>time_max) time_max = time[ii];
		    if(time[ii]<time_min) time_min = time[ii];
		}
		bw.println("Average milliseconds per query by Alg I : " + avg);
		bw.println("Average IO per query by Alg I : " + (io_total/count));
		bw.println("Time variance : " + (tmp/count));
		bw.println("Maxtime : " + time_max + "  Mintime : " + time_min);
		bw.println();
		bw.flush();

		*/
		
		
		/*
		//Alg II
		FileInputStream queryfis2 = new FileInputStream(query_file);
		BufferedReader querybr2 = new BufferedReader(new InputStreamReader(queryfis2));
		bw.println("Alg II : ");
		count = 0;
		io_total = 0;
		time_total = 0;
		tmp = 0;
		time_min = Double.MAX_VALUE;
		time_max = -1;
		while((line = querybr2.readLine()) != null){
			//System.out.println("query " + count);
			temp = line.split(",");
			qwords.clear();
			expected_ob = Integer.parseInt(temp[0]);
			f[0] = Double.parseDouble(temp[1]);
			f[1] = Double.parseDouble(temp[2]);
			Point qpoint = new Point(f);
			for(int j = 3; j < temp.length; j++){
				qwords.add(Integer.parseInt(temp[j]));
			}

			
			io_start=irtree.getIO(); hit_start=file.getHits();
			long a_start = System.currentTimeMillis();
			irtree.answeringWhyNot_222(qwords, qpoint, topk, expected_ob, alpha, 0.5, 1);
			long a_end = System.currentTimeMillis();
			io_end = irtree.getIO(); hit_end = file.getHits();
			time[count] = a_end - a_start;
			time_total += time[count];
			io_total += (io_end - io_start) - (hit_end - hit_start);
			
			count ++;
			if(count == numofquery) break;
		}
		avg = time_total/count;
		for(int ii = 0; ii<count; ii++){
			tmp += (time[ii]-avg)*(time[ii]-avg);
		    if(time[ii]>time_max) time_max = time[ii];
		    if(time[ii]<time_min) time_min = time[ii];
		}
		bw.println("Average milliseconds per query by Alg II : " + avg);
		bw.println("Average IO per query by Alg II : " + (io_total/count));
		bw.println("Time variance : " + (tmp/count));
		bw.println("Maxtime : " + time_max + "  Mintime : " + time_min);
		bw.println();
		bw.flush();
      */
		
		
		
		//Alg III
	/*	FileInputStream queryfis3 = new FileInputStream(query_file);
		BufferedReader querybr3 = new BufferedReader(new InputStreamReader(queryfis3));
		bw.println("Alg III : ");
		count = 0;
		io_total = 0;
		time_total = 0;
		tmp = 0;
		time_min = Double.MAX_VALUE;
		time_max = -1;
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

			
			io_start=irtree.getIO(); hit_start=file.getHits();
			long a_start = System.currentTimeMillis();
			irtree.answeringWhyNot_3_new(qwords, qpoint, topk, expected_ob, alpha, 0.5, 1);
			long a_end = System.currentTimeMillis();
			io_end = irtree.getIO(); hit_end = file.getHits();
			time[count] = a_end - a_start;
			time_total += time[count];
			io_total += (io_end - io_start) - (hit_end - hit_start);
			
			count ++;
			if(count == numofquery) break;
		}
		avg = time_total/count;
		for(int ii = 0; ii<count; ii++){
			tmp += (time[ii]-avg)*(time[ii]-avg);
		    if(time[ii]>time_max) time_max = time[ii];
		    if(time[ii]<time_min) time_min = time[ii];
		}
		bw.println("Average milliseconds per query by Alg III : " + avg);
		bw.println("Average IO per query by Alg III : " + (io_total/count));
		bw.println("Time variance : " + (tmp/count));
		bw.println("Maxtime : " + time_max + "  Mintime : " + time_min);
		bw.println();
		bw.flush(); */
         
	}
}
}
