package com.lsk.bigdata.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest1_Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream =
                env.readTextFile("D:\\workspace\\bigdata-flink\\src\\main\\resources\\sensor.txt");

        // 1. map，把String转换成长度输出
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) value -> value.length());

        // 2. flatmap，按逗号分字段
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        // 3. filter, 筛选sensor_1开头的id对应的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        // 打印输出
        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
/*
map> 24
flatMap> sensor_1
flatMap> 1547718199
flatMap> 35.8
filter> sensor_1,1547718199,35.8
map> 24
flatMap> sensor_6
flatMap> 1547718201
flatMap> 15.4
map> 23
flatMap> sensor_7
flatMap> 1547718202
flatMap> 6.7
map> 25
flatMap> sensor_10
flatMap> 1547718205
flatMap> 38.1
filter> sensor_10,1547718205,38.1
map> 24
flatMap> sensor_1
flatMap> 1547718207
flatMap> 36.3
filter> sensor_1,1547718207,36.3
map> 24
flatMap> sensor_1
flatMap> 1547718209
flatMap> 32.8
filter> sensor_1,1547718209,32.8
map> 24
flatMap> sensor_1
flatMap> 1547718212
flatMap> 37.1
filter> sensor_1,1547718212,37.1
*/