/*
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

package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {


	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// default operator parallelism
		env.setParallelism(1);
//		env.fromSequence(1, 10).map(number -> "Number: " + number).print();

		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("cigarette-prices")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
//				.setValueOnlyDeserializer(new SimpleStringSchema())
				.setDeserializer(new KafkaRecordToStringDeserializer())
				.build();

		// setting no watermarks means all messages go through
		// (1) a watermark in flink means if the message time is "above/newer" than the watermark it survives
		//     or else the message time is "below/older" than the watermark so it drowns
		// (2) if a kafka source parallelism is bigger than the number of kafka partitions
		//     the extra ones are not sitting idle, it holds back the min watermark as -Infinity
		//     need to set it with `WatermarkStrategy.withIdleness`. See https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/#idleness
		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
				.setParallelism(3) // kafka source operator parallelism
				.map(new SubtaskAnnotatingMap())
				.setParallelism(3) // our operator parallelism
				.print("Kafka Record") // stdout sink
				.setParallelism(1); // print sink operator parallelism

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
