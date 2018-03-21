package spatialindex.rtree;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.ComparableComparator;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.TupleBrowser;
import jdbm.recman.CacheRecordManager;
import neustore.base.FloatData;
import neustore.base.IntKey;
import neustore.base.KeyData;
import spatialindex.spatialindex.Point;

public class ttttttt {


	public static void main(String[] args)throws Exception{

		String file = "Euro.good.gz";
		
		FileInputStream fin = new FileInputStream(file);
	    GZIPInputStream gzis = new GZIPInputStream(fin);
	    InputStreamReader xover = new InputStreamReader(gzis);
	    BufferedReader is = new BufferedReader(xover);
		String line;

		
		while ( (line=is.readLine()) != null){
                System.out.println(line);
	}
	}
	
}
