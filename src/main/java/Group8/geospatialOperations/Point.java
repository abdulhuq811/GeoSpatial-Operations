package Group8.geospatialOperations;

import java.io.Serializable;


public class Point implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	double x,y;
	
	public Point()
	{}
	
	public double getX()
	{
		return x;
	}
	
	public double getY()
	{
		return y;
	}
	
	public void setX(double x)
	{
		this.x=x;
	}
		
	public void setY(double y)
	{
		this.y=y;
	}
	
	public Point(double x, double y)
	{
		this.x = x;
		this.y = y;
	}
	
	public int compareTo(Point other)
	{
		if(this.x > other.x)
			return 1;
		else if(this.x < other.x)
			return -1;
		else 
		{
			if(this.y > other.y)
				return 1;
			else if(this.y < other.y)
				return -1;
			else
				return 0;
		}
	}
	
	public String toString()
	{
		return x+","+y;
	}
}