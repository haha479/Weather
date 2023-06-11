package com.zhou;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JobMapper extends Mapper<LongWritable,Text,Text,Weather> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // System.out.println("输入key=="+key+"---------"+value);
        //对value做数据清洗和预处理
        //选择子集，列名重命名，删除重复值，缺失值处理，一致化处理，数据排序处理，异常值处理。
        //90871,广东,南沙区,440115,多云,31,西南,≤3,63,4/10/2020 19:52:36,4/10/2020 20:00:06
        String[] lineValues = value.toString().split(",");
        StringBuffer error = new StringBuffer();
        if(lineValues.length<11){
            error.append("第几项有缺失值，请补全");
        }
        //封装java bean数据类型
        Weather weather = new Weather();
        weather.setId(Integer.parseInt(lineValues[0]));
        weather.setProvince(lineValues[1]);
        weather.setCity(lineValues[2]);
        weather.setAreaCode(Integer.parseInt(lineValues[3]));
        weather.setWea(lineValues[4]);
        weather.setTemperature(Integer.parseInt(lineValues[5]));
        weather.setWindDirection(lineValues[6]);
     //   weather.setWindPower(lineValues[7]);

        weather.setHumidity(Integer.parseInt(lineValues[8]));
        weather.setReportTime(LocalDateTime.parse(lineValues[9],
                DateTimeFormatter.ofPattern("d/M/yyyy HH:mm:ss")));
        weather.setYmd(weather.getReportTime().format(DateTimeFormatter.ofPattern("yyyyMMdd")));

        weather.setCreateTime(LocalDateTime.parse(lineValues[10],
                DateTimeFormatter.ofPattern("d/M/yyyy HH:mm:ss")));

        //System.out.println(weather);
        //将数据写入reduce
        context.write(new Text(weather.getAreaCode()+":"+weather.getYmd()),weather);
    }
}
