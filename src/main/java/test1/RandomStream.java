package test1;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;


public class RandomStream {

    static Function<Long, Long> fib;

    static long Fibo = 24;
    public static long BOUND = 10000;


    public static void main(String[] args) throws Exception {


        ////////// Setup input arguments ::  it means using for example --StreamSize 100000  --Fibo 24  --Parallelism 8 as the input arguments
        final long StreamSize;
        final int Group;
        final int MapParallelism;
        final int ReduceParallelism;
        final int SourceParallelism;
        final int SinkParallelism;


////////// set up streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        //CheckpointConfig config = env.getCheckpointConfig();
        //config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


/////////Setup parameters
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        StreamSize = parameters.has("StreamSize") ? parameters.getInt("StreamSize") : 100000;
        Group = parameters.has("GroupNum") ? parameters.getInt("GroupNum") : 10000;
        //Fibo = parameters.has("Fibo") ? parameters.getInt("Fibo") : 24;
        MapParallelism = parameters.has("MapPara") ? parameters.getInt("MapPara") : env.getParallelism();
        // ReduceParallelism = parameters.has("ReducePara") ? parameters.getInt("ReducePara") : env.getParallelism();
        SourceParallelism = parameters.has("SourcePara") ? parameters.getInt("SourcePara") : 1;
        SinkParallelism = parameters.has("SinkPara") ? parameters.getInt("SinkPara") : 1;

        BOUND = StreamSize;

/////STREAM 1

//Source Random infinite sequence
        DataStream<Tuple2<Long, Long>> InputTuple = env.addSource(new RandomSource()).setParallelism(SourceParallelism).name("Source1").uid("Source1");

//Simple Map
        DataStream<Tuple2<Long, Long>> OutputTuple = InputTuple.map(new MyMap()).setParallelism(MapParallelism).name("map1").uid("map1").assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());//.keyBy(0).reduce(new MyReduce()).setParallelism(ReduceParallelism).name("Reduce");


/////STREAM 2

//Source Random infinite sequence
        DataStream<Tuple2<Long, Long>> InputTuple2 = env.addSource(new RandomSource()).setParallelism(SourceParallelism).name("Source2").uid("Source2");

//Simple Map
        DataStream<Tuple2<Long, Long>> OutputTuple2 = InputTuple2.map(new MyMap()).setParallelism(MapParallelism).name("map2").uid("map2").assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());//.keyBy(0).reduce(new MyReduce()).setParallelism(ReduceParallelism).name("Reduce");


//Join
        DataStream<Tuple4<Long, Long, Long, Long>> joinedDataStream = runWindowJoin(OutputTuple,OutputTuple2,2000);

//        OutputTuple.join(OutputTuple2).where(new MyKeySelector()).equalTo(new MyKeySelector()).window(TumblingEventTimeWindows.of(Time.milliseconds(2))).apply(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple4<Long,Long,Long,Long>>() {
//            @Override
//            public Tuple4<Long,Long,Long,Long> join(Tuple2<Long, Long> tuple1, Tuple2<Long, Long> tuple2) throws Exception {
//                return new Tuple4<>(tuple1.f0, tuple1.f1, tuple2.f0, tuple2.f1);
//            }}).assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

//Sink
        joinedDataStream.print().setParallelism(SinkParallelism).name("Sink").uid("Sink");

        env.execute("StreamingTest");
    }

    public static DataStream<Tuple4<Long,Long, Long,Long>> runWindowJoin(
            DataStream<Tuple2<Long, Long>> tuple1,
            DataStream<Tuple2<Long, Long>> tuple2,
            long windowSize) {

        return tuple1.join(tuple2)
                .where(new MyKeySelector())
                .equalTo(new MyKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(
                        new JoinFunction<
                                Tuple2<Long, Long>,
                                Tuple2<Long, Long>,
                                Tuple4<Long, Long, Long, Long>>() {

                            @Override
                            public Tuple4<Long, Long, Long, Long> join(
                                    Tuple2<Long, Long> first, Tuple2<Long, Long> second) {
                                return new Tuple4<Long, Long, Long, Long>(
                                        first.f0, first.f1, second.f0, second.f1);
                            }
                        }).assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());
    }

    public static class MyKeySelector implements KeySelector<Tuple2<Long,Long>, Long>{

        @Override
        public Long getKey(Tuple2<Long, Long> tuple2) throws Exception {
            return tuple2.f0;
        }
    }

    public static final class MyMap implements MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
            fib = (n) -> n > 1 ? fib.apply(n - 1) + fib.apply(n - 2) : n;
            Long fibresult = fib.apply(Fibo);
            value.f1 += 1;
            return value;
        }
    }

    /**
     * Generate BOUND number of random integer pairs from the range from 0 to BOUND.
     */
    private static class RandomSource implements SourceFunction<Tuple2<Long, Long>> {
        //private static final long serialVersionUID = 1L;

        private Random rnd = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

            while (isRunning /*&& counter < BOUND*/) {
                // Long first = rnd.nextLong() % BOUND;
                // Long second = rnd.nextLong() % BOUND;
                Long first = ThreadLocalRandom.current().nextLong(0, BOUND);
                Long second = ThreadLocalRandom.current().nextLong(0, BOUND);
                ctx.collect(new Tuple2<>(first, second));

                //counter++;
                //Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }

}
