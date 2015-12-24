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

package com.cloudera.sparkstreaming;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;

public class StreamingLogInput {
  public static void main(String[] args) throws Exception {
		String master = args[0];
		JavaSparkContext sc = new JavaSparkContext(master, "StreamingLogInput");
    // Create a StreamingContext with a 1 second batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));
    // Create a DStream from all the input on port 7777
    JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
    // Filter our DStream for lines with "error"
    JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
        public Boolean call(String line) {
          return line.contains("error");
        }});
    // Print out the lines with errors, which causes this DStream to be evaluated
    errorLines.print();
    // start our streaming context and wait for it to "finish"
    jssc.start();
    // Wait for 10 seconds then exit. To run forever call without a timeout
    jssc.awaitTermination(10000);
    // Stop the streaming context
    jssc.stop();
	}
}