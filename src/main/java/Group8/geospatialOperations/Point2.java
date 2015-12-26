package Group8.geospatialOperations;

import java.io.Serializable;



public class Point2 implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double x1;
	private double id;
	private double y1;

	private double x2;

	private double y2;
	
	private double xmax;
	
	private double xmin;
	
	private double ymax;
	private double ymin;
	
	public Point2() {}

	

	public Point2(double id,double x1, double y1) {

		this.x1= x1;

		this.y1 = y1;
		
		this.id=id;
	}

	

	public double get_x1() {

		return this.x1;

	}

	
	public double get_id(){
		
		
		return this.id;
	}
	


	public double get_y1() {

		return this.y1;

	}

	public double get_x2() {

		return this.x2;

	}

	

	public double get_y2() {

		return this.y2;

	}


	
	public double get_xmax(){
		
		return this.xmax;
	}
	public double get_xmin(){
		
		return this.xmin;
	}
	public double get_ymin(){
		
		return this.ymin;
	}
	public double get_ymax(){
		
		return this.ymax;
	}

	public void set_x1(double x1) {

		this.x1 = x1;

	}

	

	public void set_y1(double y1) {

		this.y1 = y1;

	}

	public void set_x2(double x2) {

		this.x2 = x2;

	}

	

	public void set_y2(double y2) {

		this.y2 = y2;

	}
	
	
	public void set_id(double id)
	{
		this.id=id;
		
	}
	
	public void set_xmax(double xmax){
		
		this.xmax=xmax;
	}
public void set_xmin(double xmin){
		
		this.xmin=xmin;
	}
public void set_ymin(double ymin){
	
	this.ymin=ymin;
}
public void set_ymax(double ymax){
	
	this.ymax=ymax;
}

	

	public void print_x1() {

		System.out.println(Double.toString(this.x1));

	}

	public void print_y1() {

		System.out.println(Double.toString(this.y1));

	}

	public void print_xmax() {
		System.out.println(Double.toString(this.xmax));
	}

	public void print_ymax() {

		System.out.println(Double.toString(this.ymax));

	}
	
	public void print_ymin() {

		System.out.println(Double.toString(this.ymin));

	}
	
	public void print_xmin() {

		System.out.println(Double.toString(this.xmin));

	}
	


	@Override
	public String toString() {
		Integer idc= (int) this.id;
	return ""+idc+"";
	}

	public String toStringPrint() {
		return ""+x1+","+y1+","+x2+","+y2+"\n";
	}
	public int compareTo(Point2 other) {
		if (this.id < other.id) {
			return -1;
		} else if (this.id > other.id) {
			return 1;
		} else {
			return 0;
		}
	}
}
