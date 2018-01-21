// NOTE: Please read README.txt before browsing this code.
package regressiontest;
import spatialindex.rtree.RTree;
import spatialindex.spatialindex.IData;
import spatialindex.spatialindex.INode;
import spatialindex.spatialindex.ISpatialIndex;
import spatialindex.spatialindex.IVisitor;
import spatialindex.spatialindex.Point;
import spatialindex.spatialindex.Region;
import spatialindex.storagemanager.DiskStorageManager;
import spatialindex.storagemanager.IBuffer;
import spatialindex.storagemanager.IStorageManager;
import spatialindex.storagemanager.PropertySet;
import spatialindex.storagemanager.RandomEvictionsBuffer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.StringTokenizer;

public class RTreeLoad
{
	public static void main(String[] args)
	{
		System.out.println("Load Start!!");
		new RTreeLoad(args);
	}
	
	RTreeLoad(String[] args)
	{
		try
		{
			if (args.length != 4)
			{
				System.err.println("Usage: RTreeLoad input_file tree_file capacity query_type [intersection | 10NN].");
				System.exit(-1);
			}

			LineNumberReader lr = null;

			try
			{
				lr = new LineNumberReader(new FileReader(args[0]));
			}
			catch (FileNotFoundException e)
			{
				System.err.println("Cannot open data file " + args[0] + ".");
				System.exit(-1);
			}

			// Create a disk based storage manager.
			PropertySet ps = new PropertySet();

			Boolean b = new Boolean(true);
			ps.setProperty("Overwrite", b);
				//overwrite the file if it exists.

			ps.setProperty("FileName", args[1]);
				// .idx and .dat extensions will be added.

			Integer i = new Integer(4096);
			ps.setProperty("PageSize", i);
				// specify the page size. Since the index may also contain user defined data
				// there is no way to know how big a single node may become. The storage manager
				// will use multiple pages per node if needed. Off course this will slow down performance.

			IStorageManager diskfile = new DiskStorageManager(ps);

			IBuffer file = new RandomEvictionsBuffer(diskfile, 10, false);
				// applies a main memory random buffer on top of the persistent storage manager
				// (LRU buffer, etc can be created the same way).

			// Create a new, empty, RTree with dimensionality 2, minimum load 70%, using "file" as
			// the StorageManager and the RSTAR splitting policy.
			PropertySet ps2 = new PropertySet();

			Double f = new Double(0.7);
			ps2.setProperty("FillFactor", f);

			i = new Integer(args[2]);
			ps2.setProperty("IndexCapacity", i);
			ps2.setProperty("LeafCapacity", i);
				// Index capacity and leaf capacity may be different.

			i = new Integer(2);
			ps2.setProperty("Dimension", i);

			ISpatialIndex tree = new RTree(ps2, file);

			int count = 0;
			int indexIO = 0;
			int leafIO = 0;
			int id, op;
			double x1, x2, y1, y2;
			double[] f1 = new double[2];
			double[] f2 = new double[2];

			long start = System.currentTimeMillis();
			String line = lr.readLine();

			while (line != null)
			{
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
					Region r = new Region(new double[] {1, 1}, new double[]{2, 10});
					String data = r.toString();
					tree.insertData(data.getBytes(), r, id);
				}
				else if (op == 2)
				{
					//query

					f1[0] = x1; f1[1] = y1;
					f2[0] = x2; f2[1] = y2;

					MyVisitor vis = new MyVisitor();

					if (args[3].equals("intersection"))
					{
						Region r = new Region(f1, f2);
						tree.intersectionQuery(r, vis);
							// this will find all data that intersect with the query range.
					}
					else if (args[3].equals("10NN"))
					{
						Point p = new Point(new double[]{92, 0210});
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
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	// example of a Visitor pattern.
	// see RTreeQuery for a more elaborate example.
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
