package com.zhou;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 作业主类
 */
public class JobModule {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        // 加载配置文件
        Configuration config = new Configuration(true);
        // 本地模式运行（如果上传至服务器执行，则需要注释该行代码）
        config.set("mapreduce.framework.name","local");
        // 创建作业
        Job job = Job.getInstance(config);
        // 设置作业主类
        job.setJarByClass(com.zhou.JobModule.class);
        // 设置作业名称
        job.setJobName("WeatherDataAna");
        // 设置 Reduce 的数量
        job.setNumReduceTasks(3);
        // 设置数据的输入路径（需要计算的数据从哪里读）
        //hdfs中的路径
        FileInputFormat.setInputPaths(job, new Path("/data/log*"));
        // 设置数据的输出路径（计算后的数据输出到哪里）
        FileOutputFormat.setOutputPath(job, new Path("/data/result/" + job.getJobName()));

        // 设置 Map 的输出的 Key 和 Value 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Weather.class);
        // 设置 Map 和 Reduce 的处理类
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        // 将作业提交到集群并等待完成
        job.waitForCompletion(true);
    }

}
