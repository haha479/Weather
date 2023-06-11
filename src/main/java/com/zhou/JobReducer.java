package com.zhou;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JobReducer extends Reducer<Text,Weather,Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Weather> values, Context context) throws IOException, InterruptedException {
       // System.out.println("输入key=="+key+"---------");
        //求各个地址每天的最低温度
//        int max = 0;
//        for (Weather weather : values) {
//            max = Math.max(weather.getTemperature(),max);
//        }
        //结果： key 最低温度【min】
       // context.write(new Text(key+"最高温度【"+max+"】"),NullWritable.get());
        for (Weather weather : values) {
            context.write(new Text(weather.toString()),NullWritable.get());
        }

    }
}
