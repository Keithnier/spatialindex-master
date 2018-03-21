package spatialindex.rtree;

import java.util.Comparator;

public class Cp3Comparator implements Comparator {

	public int compare(Object o1, Object o2)
	{
		Cp3 p1 = (Cp3) o1;
		Cp3 p2 = (Cp3) o2;

		if (p1.x < p2.x) return -1;
		if (p1.x > p2.x) return 1;
		return 0;
	}

}
