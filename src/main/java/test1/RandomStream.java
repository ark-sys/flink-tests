package test1;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
        final int joinParallelism;
        final long generationInterval;

////////// set up streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.disableOperatorChaining();

        //CheckpointConfig config = env.getCheckpointConfig();
        //config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        SlotSharingGroup ssgSource1 = SlotSharingGroup.newBuilder("Source1")
                .setCpuCores(0.5)
                .setTaskHeapMemoryMB(100)
                .build();
        SlotSharingGroup ssgSource2 = SlotSharingGroup.newBuilder("Source2")
                .setCpuCores(0.5)
                .setTaskHeapMemoryMB(100)
                .build();
        SlotSharingGroup ssgB = SlotSharingGroup.newBuilder("PrintSRC-1")
                .setCpuCores(0.1)
                .setTaskHeapMemoryMB(100)
                .build();
        SlotSharingGroup ssg2 = SlotSharingGroup.newBuilder("PrintSRC-2")
                .setCpuCores(0.1)
                .setTaskHeapMemoryMB(100)
                .build();
        SlotSharingGroup ssgC = SlotSharingGroup.newBuilder("Joiners")
                .setCpuCores(0.5)
                .setTaskHeapMemoryMB(100)
                .build();

        SlotSharingGroup ssgD = SlotSharingGroup.newBuilder("Sink")
                .setCpuCores(0.1)
                .setTaskHeapMemoryMB(100)
                .build();

/////////Setup parameters
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        StreamSize = parameters.has("StreamSize") ? parameters.getInt("StreamSize") : 1;
        //Fibo = parameters.has("Fibo") ? parameters.getInt("Fibo") : 24;
        MapParallelism = parameters.has("MapPara") ? parameters.getInt("MapPara") : env.getParallelism();
        // ReduceParallelism = parameters.has("ReducePara") ? parameters.getInt("ReducePara") : env.getParallelism();
        SourceParallelism = parameters.has("SourcePara") ? parameters.getInt("SourcePara") : 1;
        SinkParallelism = parameters.has("SinkPara") ? parameters.getInt("SinkPara") : 1;

        joinParallelism = parameters.has("JoinPara") ? parameters.getInt("JoinPara") : 2;

        generationInterval = parameters.has("GenInterval") ? parameters.getLong("GenInterval") : 2000L;
        BOUND = StreamSize;

/////STREAM 1

//Source Random infinite sequence
        DataStream<Tuple3<String,Long, Long>> InputTuple = env.addSource(new RandomSource(generationInterval, "Source1")).setParallelism(SourceParallelism).name("Source1").uid("Source1").slotSharingGroup("Source1");

//Simple Map
        DataStream<Tuple3<String,Long, Long>> OutputTuple = InputTuple.map(new MyMap()).setParallelism(MapParallelism).name("map1").uid("map1").assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create()).slotSharingGroup("Source1");//.keyBy(0).reduce(new MyReduce()).setParallelism(ReduceParallelism).name("Reduce");

        KeyedStream<Tuple3<String,Long, Long>, Long> keyedStream= OutputTuple.keyBy(atuple -> atuple.f1);


        keyedStream.print().slotSharingGroup("PrintSRC-1");
/////STREAM 2

//Source Random infinite sequence
        DataStream<Tuple3<String,Long, Long>> InputTuple2 = env.addSource(new RandomSource(generationInterval,"Source2")).setParallelism(SourceParallelism).name("Source2").uid("Source2").slotSharingGroup("Source2");

//Simple Map
        DataStream<Tuple3<String,Long, Long>> OutputTuple2 = InputTuple2.map(new MyMap()).setParallelism(MapParallelism).name("map2").uid("map2").assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create()).slotSharingGroup("Source2");//.keyBy(0).reduce(new MyReduce()).setParallelism(ReduceParallelism).name("Reduce");

        KeyedStream<Tuple3<String,Long, Long>, Long> keyedStream2= OutputTuple2.keyBy(atuple2 -> atuple2.f1);

        keyedStream2.print().slotSharingGroup("PrintSRC-2");


//Join
        DataStream<Tuple6<String,Long, Long, String, Long, Long>> joinedDataStream = runWindowJoin(keyedStream,keyedStream2,5000, joinParallelism);

//Sink
        joinedDataStream.print().setParallelism(SinkParallelism).name("Sink").uid("Sink").slotSharingGroup("Sink");

        env.execute("StreamingTest");
    }

    public static DataStream<Tuple6<String,Long,Long, String, Long,Long>> runWindowJoin(
            DataStream<Tuple3<String,Long, Long>> tuple1,
            DataStream<Tuple3<String,Long, Long>> tuple2,
            long windowSize,
            int joinParallelism) {

        return tuple1.assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create()).join(tuple2)
                .where(new MyKeySelector())
                .equalTo(new MyValueSelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .with(new MyJoinFunction()).setParallelism(joinParallelism).slotSharingGroup("Joiners");
    }

    public static class MyJoinFunction implements JoinFunction<
            Tuple3<String,Long, Long>,
            Tuple3<String,Long, Long>,
            Tuple6<String, Long, Long, String, Long, Long>>{

        @Override
        public Tuple6<String, Long, Long, String, Long, Long> join(Tuple3<String, Long, Long> first, Tuple3<String, Long, Long> second) throws Exception {
            return new Tuple6<String, Long, Long, String, Long, Long>(
                    first.f0, first.f1, first.f2, second.f0, second.f1, second.f2);
        }
    }
    public static class MyKeySelector implements KeySelector<Tuple3<String,Long,Long>, Long>{

        @Override
        public Long getKey(Tuple3<String,Long, Long> atuple) throws Exception {
            return atuple.f1;
        }
    }

    public static class MyValueSelector implements KeySelector<Tuple3<String,Long,Long>, Long>{

        @Override
        public Long getKey(Tuple3<String,Long, Long> atuple) throws Exception {
            return atuple.f2;
        }
    }

    public static final class MyMap implements MapFunction<Tuple3<String,Long, Long>, Tuple3<String,Long, Long>> {
        @Override
        public Tuple3<String,Long, Long> map(Tuple3<String,Long, Long> value) throws Exception {
            fib = (n) -> n > 1 ? fib.apply(n - 1) + fib.apply(n - 2) : n;
            Long fibresult = fib.apply(Fibo);
            value.f2 += 1;
            return value;
        }
    }

    /**
     * Generate BOUND number of random integer pairs from the range from 0 to BOUND.
     */
    private static class RandomSource implements SourceFunction<Tuple3<String,Long, Long>> {
        //private static final long serialVersionUID = 1L;



        private volatile boolean isRunning = true;

        private final long generationInterval;
        private final String sourceID;

        public RandomSource(long generationInterval, String sourceID) {
            this.generationInterval = generationInterval;
            this.sourceID = sourceID;
        }
        @Override
        public void run(SourceContext<Tuple3<String,Long, Long>> ctx) throws Exception {
            long tupleID = 0;

            while (isRunning /*&& counter < BOUND*/) {
                Long randomLong = ThreadLocalRandom.current().nextLong(0, BOUND);

                tupleID++;
                ctx.collect(new Tuple3<>(this.sourceID,tupleID, randomLong));
                if (generationInterval > 0L){
                    Thread.sleep(generationInterval);
                }
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
