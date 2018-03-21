package spatialindex.rtree;

public class Candidatevector {
	
	protected double dist;
	protected double simi;
	protected double w_penalty = 0;
	protected int oid;
	protected int loc;
//	protected double _start = 0;
//	protected double _end = 0;
	
	
	Candidatevector(double dis, double sim){
		this.dist = dis;
		this.simi = sim;
	}
	
	Candidatevector(int oid, double dis, double sim) {
		this.oid = oid;
		this.dist = dis;
		this.simi = sim;
	}

	
	public boolean equals(Object obj){
		if(!(obj instanceof Candidatevector)) return false;
		
		return this.oid == ((Candidatevector) obj).oid;
		
	}
}
