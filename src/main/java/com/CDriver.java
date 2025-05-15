package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: Driver <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mi Trabajo MapReduce");
        job.setJarByClass(CDriver.class);
        job.setMapperClass(CMapper.class);
        job.setReducerClass(CReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Especifica las rutas de tus tres datasets como entrada
        FileInputFormat.addInputPath(job, new Path(args[0])); // Ruta del primer dataset
        FileInputFormat.addInputPath(job, new Path(args[1])); // Ruta del segundo dataset
        FileInputFormat.addInputPath(job, new Path(args[2])); // Ruta del tercer dataset

        FileOutputFormat.setOutputPath(job, new Path(args[3])); // Ruta donde se guardarán los resultados

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}