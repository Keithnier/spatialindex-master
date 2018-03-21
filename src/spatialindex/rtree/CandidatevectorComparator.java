package spatialindex.rtree;

import java.util.Comparator;

public class CandidatevectorComparator implements Comparator {

	public int compare(Object o1, Object o2)
	{
		Candidatevector p1 = (Candidatevector) o1;
		Candidatevector p2 = (Candidatevector) o2;

		if (p1.dist < p2.dist) return -1;
		if (p1.dist > p2.dist) return 1;
		return 0;
	}
}







