package org.bdcourse;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = env.fromElements(
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,"
				);


		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// execute and print result
		counts.print();


		 /**

		DataSet<Tuple2<Integer, Integer>> counts2 =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter2())
						// group by the tuple field "0" and sum up tuple field "1"
						.groupBy(0)
						.sum(1);

		// execute and print result
		counts2.print();
		**/

        /**
		DataSet<Tuple2<String, Integer>> counts3 =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new LineSplitter3())
						// group by the tuple field "0" and sum up tuple field "1"
						.groupBy(0)
						.sum(1);

		// execute and print result
		counts3.print();
         **/
        /**
        DataSet<Tuple2<String, Integer>> counts21 =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter21())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // execute and print result
        counts21.print();
        **/

        /**
        DataSet<Tuple2<String, Integer>> counts22 =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter22())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // execute and print result
        counts22.print();


        DataSet<Tuple1<Double>> counts22_2 = text.flatMap(new LineSplitter22())
                // group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0)
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(Tuple2<String, Integer> input) throws Exception {
                        return Tuple3.of(input.getField(0), input.getField(1), "x");
                    }
                })
                .groupBy(2)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, Integer, String>, Tuple1<Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, Integer, String>> iterable, Collector<Tuple1<Double>> collector) throws Exception {
                        Integer c = null;
                        Integer v = null;
                        for (Tuple3<String, Integer, String> iter:iterable){
                            if (iter.getField(0).equals("conson")) c = iter.getField(1);
                            else if (iter.getField(0).equals("vovel")) v = iter.getField(1);
                            else System.out.println("something is not right");
                        }
                        double d = (double)v / (double) c;
                        System.out.println(v);
                        System.out.println(c);
                        collector.collect(new Tuple1 <Double>(d));
                    }
                });

        // execute and print result
        counts22_2.print();


        DataSet<Tuple3<Integer, Integer, Double>> ratio2_3 = text.flatMap(new FlatMapFunction<String, Tuple3<Integer, Integer, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split( "\\W+");

                for (String token : tokens){
                    collector.collect(new Tuple3<Integer, Integer, String>(token.length(), 1, "x") );
                }
            }
        })
                .groupBy(0)
                .sum(1)
                .groupBy(2)
                .reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<Integer, Integer, String>> iterable, Collector<Tuple3<Integer, Integer, Double>> collector) throws Exception {
                        Integer literalCount = 0;
                        Integer wordCount = 0;
                        for (Tuple3<Integer, Integer, String> iter: iterable){
                            literalCount += (Integer) iter.getField(0)*(Integer) iter.getField(1);
                            wordCount += (Integer) iter.getField(1);
                        }
                        Double d = (double) literalCount / (double) wordCount;
                        collector.collect(new Tuple3<Integer, Integer, Double>(literalCount, wordCount, d));
                    }
                });
        ratio2_3.print();

        DataSet<Tuple2<String, Integer>> most_used_word = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split("\\W+");

                for (String token: tokens) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        })
                .groupBy(0)
                .sum(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String most_used_word = null;
                        Integer frequency = 0;
                        for (Tuple2<String, Integer> iter: iterable){
                            if ( (Integer) iter.getField(1) > frequency) {
                                most_used_word = iter.getField(0);
                                frequency = iter.getField(1);
                            }
                        }
                        collector.collect(new Tuple2<String, Integer>(most_used_word, frequency));
                    }
                });
        most_used_word.print();
         **/

        /**

        String filename = "/home/alex/Big Data/lab/FlinkExample1/flinkexercises/src/main/resources/web.txt";
        File file = new File(filename);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String tmpline;
        ArrayList<String> lines = new ArrayList<>();
        while ((tmpline = bufferedReader.readLine()) != null){
            lines.add(tmpline);
        }

        String[] data = lines.toArray(new String[lines.size()]);


        final ExecutionEnvironment env_for_page_rank = ExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataSet<String> web_text = env_for_page_rank.fromElements(data);

        DataSet<Tuple2<String, Integer>> page_rank = web_text.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] tokens = s.split("\\{");
                tokens[1] = tokens[1].replace("}", "");

                String[] links = tokens[1].replace("URL:", "").split("; ");

                for (String link : links){
                    collector.collect(new Tuple2<String, String>(tokens[0], link));
                }

            }
        })
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Integer count = 0;
                        String name = "";
                        for(Tuple2<String, String> iter : iterable){
                            name = iter.getField(1);
                            count += 1;
                        }
                        collector.collect(new Tuple2<String, Integer>(name, count));
                    }
                });


        page_rank.print();
        **/
    }

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}

	}

	public static final class LineSplitter2 implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
			String[] tokens = value.toLowerCase().split( "\\W+");

			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<Integer, Integer>(token.length(), 1));
				}
			}
		}
	}


	public static final class LineSplitter3 implements FlatMapFunction<String, Tuple2<String, Integer>> {
		//My wrong solution on the task 7
		public static HashSet<String> removeExcessElements(String[] array, Integer length)
		{
			//return (String[]) Stream.of(array).filter(array_element -> array_element.equals(element)).toArray();
			//return IntStream.of(array).filter(array_element -> array_element != element).toArray();
			List<String> wordsOfLengthTwo = Arrays.stream(array).filter(s -> s.length() == length).collect(Collectors.toList());
			return new HashSet<String>(wordsOfLengthTwo);
		}


		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split( "\\W+");
			HashSet<String> wordsOfLengthTwo = removeExcessElements(tokens, 2);

			for(String token: tokens){
				for(String wordOfLengthTwo: wordsOfLengthTwo){
					if (token.contains(wordOfLengthTwo)){
						out.collect(new Tuple2<String, Integer>(wordOfLengthTwo, 1));
					}
				}
			}
		}
	}

    public static final class LineSplitter21 implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split( "\\W+");

            for(String token: tokens){
                if (token.length() < 6)  out.collect(new Tuple2<String, Integer>("short", 1));
                else out.collect(new Tuple2<String, Integer>("long", 1));
            }
        }
    }

    public static final class LineSplitter22 implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split( "\\W+");

            for(String token: tokens){
                if (token.matches("(a|e|i|o|u).*|(a|e|i|o|u)"))  out.collect(new Tuple2<String, Integer>("vovel", 1));
                else out.collect(new Tuple2<String, Integer>("conson", 1));
            }
        }
    }

    public static final class LineSplitter23_1 implements FlatMapFunction<String, Tuple2<Integer, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
            String[] tokens = value.toLowerCase().split( "\\W+");
            for(String token: tokens){
                out.collect(new Tuple2<Integer, Integer>(token.length(), 1));
            }
        }
    }


}
