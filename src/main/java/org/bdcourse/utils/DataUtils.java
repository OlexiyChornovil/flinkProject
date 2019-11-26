package org.bdcourse.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;

public class DataUtils {
	
	public static List<Tuple3<Long,Double,String>> getData() {
		ArrayList<Tuple3<Long,Double,String>> data = new ArrayList<Tuple3<Long,Double,String>>();
		data.add(new Tuple3<Long, Double, String>(0L, 0.0, "zero"));
		data.add(new Tuple3<Long, Double, String>(1L, 1.1, "one"));
		data.add(new Tuple3<Long, Double, String>(2L, 2.2, "two"));
		data.add(new Tuple3<Long, Double, String>(3L, 3.3, "three"));
		data.add(new Tuple3<Long, Double, String>(4L, 4.4, "four"));
		data.add(new Tuple3<Long, Double, String>(5L, 5.5, "five"));
		data.add(new Tuple3<Long, Double, String>(6L, 0.0, "zero zero"));
		data.add(new Tuple3<Long, Double, String>(7L, 1.1, "one one"));
		data.add(new Tuple3<Long, Double, String>(8L, 2.2, "two two"));
		data.add(new Tuple3<Long, Double, String>(9L, 3.3, "three three"));
		data.add(new Tuple3<Long, Double, String>(10L, 4.4, "four four"));
		data.add(new Tuple3<Long, Double, String>(11L, 5.5, "five five"));
		data.add(new Tuple3<Long, Double, String>(12L, 0.0, "zero zero"));
		data.add(new Tuple3<Long, Double, String>(13L, 1.1, "one x3"));
		data.add(new Tuple3<Long, Double, String>(14L, 2.2, "two x3"));
		data.add(new Tuple3<Long, Double, String>(15L, 3.3, "three x3"));
		data.add(new Tuple3<Long, Double, String>(16L, 4.4, "four x3"));
		data.add(new Tuple3<Long, Double, String>(17L, 5.5, "five x3"));
		data.add(new Tuple3<Long, Double, String>(18L, 100.0, "end 1"));
		data.add(new Tuple3<Long, Double, String>(19L, 200.0, "end 2"));
		data.add(new Tuple3<Long, Double, String>(20L, 300.0, "end 3"));
		return data;
	}

}
