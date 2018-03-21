package spatialindex.rtree;

import java.util.Comparator;

public class CandidatepointComparator implements Comparator {

	public int compare(Object o1, Object o2)
	{
		Candidatepoint p1 = (Candidatepoint) o1;
		Candidatepoint p2 = (Candidatepoint) o2;


		if (p1.location_xcoordinate < p2.location_xcoordinate) return -1;
		if (p1.location_xcoordinate > p2.location_xcoordinate) return 1;
		return 0;


		
	}

}
