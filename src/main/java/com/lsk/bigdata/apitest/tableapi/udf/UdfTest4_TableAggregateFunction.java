package com.lsk.bigdata.apitest.tableapi.udf;

import com.lsk.bigdata.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class UdfTest4_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取数据
        DataStreamSource<String> inputStream =
                env.readTextFile("D:\\workspace\\bigdata-flink\\src\\main\\resources\\sensor.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3. 将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 4. 自定义聚合函数，求当前传感器的平均温度值
        // 4.1 table API
        // 创建一个表聚合函数实例
        Top2Temp top2Temp = new Top2Temp();
        tableEnv.registerFunction("top2Temp", top2Temp);

        Table resultTable = sensorTable
                .groupBy("id")
                .flatAggregate("top2Temp(temp) as (temp, rank)")
                .select("id, temp, rank");

        tableEnv.toRetractStream(resultTable, Row.class).print("result");

        // 4.2 SQL
//        tableEnv.createTemporaryView("sensor", sensorTable);
//        Table resultSqlTable = tableEnv.sqlQuery("select id, top2Temp(temp) " +
//                " from sensor group by id");

        // 打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
//        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 先定义一个 Accumulator
    public static class Top2TempAcc {
        double highestTemp = Double.MIN_VALUE;
        double secondHighestTemp = Double.MIN_VALUE;
    }

    // 自定义表聚合函数
    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Top2TempAcc> {
        @Override
        public Top2TempAcc createAccumulator() {
            return new Top2TempAcc();
        }

        // 实现计算聚合结果的函数 accumulate
        public void accumulate(Top2TempAcc acc, Double temp) {
            if (temp > acc.highestTemp) {
                acc.secondHighestTemp = acc.highestTemp;
                acc.highestTemp = temp;
            } else if (temp > acc.secondHighestTemp) {
                acc.secondHighestTemp = temp;
            }
        }

        // 实现一个输出结果的方法，最终处理完表中所有数据时调用
        public void emitValue(Top2TempAcc acc, Collector<Tuple2<Double, Integer>> out) {
            out.collect(new Tuple2<>(acc.highestTemp, 1));
            out.collect(new Tuple2<>(acc.secondHighestTemp, 2));
        }
    }
}
