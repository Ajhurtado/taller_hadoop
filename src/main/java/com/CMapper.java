package com;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Lógica para procesar cada línea de uno de tus datasets
        String line = value.toString();
        // Ejemplo: Tokenizar la línea por espacios
        for (String token : line.split("\\s+")) {
            word.set(token);
            context.write(word, one);
        }
        // Adapta esta lógica para procesar la información relevante de tus datasets
    }
}