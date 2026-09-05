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
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

		KafkaSource<CigarettePrice> cigaretteSource = KafkaSource.<CigarettePrice>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("cigarette-prices")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(new CigarettePriceDeserializer())
				.build();

		KafkaSource<AlcoholPrice> alcoholSource = KafkaSource.<AlcoholPrice>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics("alcohol-prices")
				.setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(new AlcoholPriceDeserializer())
				.build();

		DataStreamSource<CigarettePrice> cigaretteStream =
				env.fromSource(cigaretteSource, WatermarkStrategy.noWatermarks(), "Cigarette Kafka Source")
				.setParallelism(3); // kafka source operator parallelism

		DataStreamSource<AlcoholPrice> alcoholStream =
				env.fromSource(alcoholSource, WatermarkStrategy.noWatermarks(), "Alcohol Kafka Source")
				.setParallelism(4); // kafka source operator parallelism

		cigaretteStream.print("Cigarette");
		alcoholStream.print("Alcohol");

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
