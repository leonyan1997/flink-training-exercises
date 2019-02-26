/**
 * 1.需要给event加timestamp和watermark
 * 2.需要确定用哪种窗口。sliding window或者global window。哪种开销小。
 * 3.需要用state管理结果。*/


package com.dataartisans.flinktraining.exercises.datastream_java;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;


public class DuplicateRemovalExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        // events are out of order by max 60 seconds
        final int maxEventDelay = 60;
        // events of 10 minutes are served in 1 second
        final int servingSpeedFactor = 600;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));
        DataStream<TaxiRide> result = rides
                .filter(new ValidateNum())
                .keyBy((TaxiRide ride) -> ride.driverId)
                .timeWindow(Time.hours(1))
                .process(new DuplicateRemovalFunction());

        // print the filtered stream
        printOrTest(result);

        // run the cleansing pipeline
        env.execute("Taxi Ride Duplicate Removal");
    }

    public static class ValidateNum implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            int num = 2;
            return taxiRide.passengerCnt <= num;
        }
    }

    public static class DuplicateRemovalFunction extends ProcessWindowFunction<TaxiRide, TaxiRide, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<TaxiRide> input, Collector<TaxiRide> out) throws Exception {
            DateTime time1 = new DateTime(2020, 12, 31, 23, 59, 59);
            DateTime time2 = new DateTime(1970, 1, 1, 0, 0, 0);
            for (TaxiRide in : input) {
                if (in.passengerCnt == 1 && in.startTime.isBefore(time1)) {
                    time1 = in.startTime;
                }
                if (in.passengerCnt == 2 && in.startTime.isAfter(time2)) {
                    time2 = in.startTime;
                }
                if (!time1.isEqual(new DateTime(2020, 12, 31, 23, 59, 59))) {
                    out.collect(in);
                }
                if (time1.isEqual(new DateTime(2020, 12, 31, 23, 59, 59))) {
                    out.collect(in);
                }
            }
        }
    }
}
