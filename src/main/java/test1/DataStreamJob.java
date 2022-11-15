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

package test1;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.TaxiRide;
import utils.TaxiRideSchema;
import utils.TaxiRideSource;

import java.util.Properties;

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

	static final String TOPIC  = "rides";

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.


		ParameterTool params = ParameterTool.fromArgs(args);
		//final int servingSpeedFactor  = params.has("speed") ? params.getInt("speed") : 60;
		final String BROKER  = params.has("broker") ? params.get("broker") : "172.19.0.5:9092";
		final String pathToRides = params.has("path") ? params.get("path") :"/Rides-2days.gz";

		Properties Properties = new Properties();
		Properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
		Properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		Properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);



		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(pathToRides, 0, 30)).setParallelism(1).name("Source").uid("Source");
		rides.addSink(new FlinkKafkaProducer<TaxiRide>(BROKER, TOPIC, new TaxiRideSchema())).setParallelism(1).name("Sink1").uid("Sink1");
		rides.addSink(new FlinkKafkaProducer<TaxiRide>(BROKER, TOPIC, new TaxiRideSchema())).setParallelism(1).name("iSink2").uid("iSink2");
		rides.addSink(new FlinkKafkaProducer<TaxiRide>(BROKER, TOPIC, new TaxiRideSchema())).setParallelism(1).name("iSink3").uid("iSink3");

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		rides.print();
		// Execute program, beginning computation.
		env.execute("Flink-Kafka test");
	}
}
