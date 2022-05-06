package com.lsk.bigdata.apitest.transform;

import com.lsk.bigdata.apitest.beans.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream =
                env.readTextFile("D:\\workspace\\bigdata-flink\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId);

        DataStream<Long> dataStream1 = env.fromElements(1L, 34L, 4L, 657L, 23L);
        KeyedStream<Long, Integer> keyedStream2 = dataStream1.keyBy(new KeySelector<Long, Integer>() {
            @Override
            public Integer getKey(Long value) throws Exception {
                return value.intValue() % 2;
            }
        });

        // 滚动聚合，取当前最大的温度值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print("result");

        keyedStream1.print("key1");
        keyedStream2.sum(0).print("key2");
        env.execute();
    }
}
/*
key2:3> 1
key2:3> 34
key2:3> 38
key2:3> 658
key2:3> 681
key1:3> SensorReading{id='sensor_1', timestamp=1547718207, temperature=36.3}
key1:3> SensorReading{id='sensor_1', timestamp=1547718209, temperature=32.8}
result:3> SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
result:3> SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
result:3> SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
result:3> SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
result:3> SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
key1:4> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
result:2> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
result:4> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
key1:2> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
key1:3> SensorReading{id='sensor_1', timestamp=1547718212, temperature=37.1}
key1:3> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
key1:3> SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
*/