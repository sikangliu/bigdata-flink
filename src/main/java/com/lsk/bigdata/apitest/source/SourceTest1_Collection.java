package com.lsk.bigdata.apitest.source;

import com.lsk.bigdata.apitest.beans.SensorReading;
import java.util.Arrays;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 67, 189);

        // 打印输出
        dataStream.print("data");
        integerDataStream.print("int");

        // 执行
        env.execute();
    }
}
/*
int> 1
int> 2
data> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
int> 4
data> SensorReading{id='sensor_6', timestamp=1547718201, temperature=15.4}
int> 67
data> SensorReading{id='sensor_7', timestamp=1547718202, temperature=6.7}
int> 189
data> SensorReading{id='sensor_10', timestamp=1547718205, temperature=38.1}
*/