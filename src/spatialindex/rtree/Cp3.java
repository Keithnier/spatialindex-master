package spatialindex.rtree;

public class Cp3 {

protected double x = 0.0;
protected double y = 0.0;
protected int num = 1;
protected int numFromindex0;
protected double penalty_w = 0.0;
protected double penalty_up = 0.0;
protected double penalty_low = 0.0;
protected int rank_up = 0;
protected int rank_low = 0;
//protected int hehe_up = 0;
//protected int hehe_low = 0;
protected int pruned = 0;

Cp3(double l){

	x = l; 
}

Cp3(double x, double y){
	this.x = x;
	this.y = y;
}


public boolean equals(Object obj){
	if(!(obj instanceof Cp3)) return false;
	
	return this.x == ((Cp3) obj).x;
}


}
