package spatialindex.rtree;

public class Candidatepoint {

protected int in = 0;
protected int out = 0;
protected int loc = 0;
protected double location_xcoordinate = 0.0;
protected double location_ycoordinate = 0.0;


Candidatepoint(double l){

	location_xcoordinate = l; 
}

Candidatepoint(double l, int i){

	location_xcoordinate = l; 
	loc = i;
}

Candidatepoint(double x, double y){
	location_xcoordinate = x;
	location_ycoordinate = y;
	
}


public boolean equals(Object obj){
	if(!(obj instanceof Candidatepoint)) return false;
	
	return this.location_xcoordinate == ((Candidatepoint) obj).location_xcoordinate;
	
}


}
