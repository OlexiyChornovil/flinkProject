package org.bdcourse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bdcourse.utils.DataUtils;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the
 * SocketTextStreamWordCount.java file in the same package/directory or have a
 * look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink cluster.
 * Just type mvn clean package in the projects root directory. You will find the
 * jar in target/flinkexercises-1.0.jar From the CLI you can then run
 * ./bin/flink run -c org.bdcourse.StreamingJob target/flinkexercises-1.0.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// emulate a stream from a data set
		DataStream<Tuple3<Long, Double, String>> streamInput = env.fromCollection(DataUtils.getData());

		// print input
		streamInput.print();

		// add arrival timestamp
		// add prefix for string field
		// switch field 0 with field 1
		// add 1 to value of input field 1
		streamInput.map(new MapFunction<Tuple3<Long, Double, String>, Tuple4<Double, Long, String, Long>>() {
			@Override
			public Tuple4<Double, Long, String, Long> map(Tuple3<Long, Double, String> arg0) throws Exception {

				long plusOne = arg0.f0 + 1;
				return new Tuple4<>(arg0.f1, plusOne, "processed: " + arg0.f2, System.currentTimeMillis());
			}
		}).print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
