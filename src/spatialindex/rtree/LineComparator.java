package spatialindex.rtree;
import java.util.Comparator;

public class LineComparator implements Comparator {

	public int compare(Object o1, Object o2)
	{
		Line l1 = (Line) o1;
		Line l2 = (Line) o2;

		if (l1.a/l1.b > l2.a/l2.b) return -1;
		return 1;
	}

}
