package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        
        StringTokenizer tokenizer = new StringTokenizer(line);

        if (tokenizer.hasMoreTokens()) {
            String characterName = tokenizer.nextToken();

            while (tokenizer.hasMoreTokens()) {
                String currentWord = tokenizer.nextToken();

                characterWord.set(characterName + ":" + currentWord);
                context.write(characterWord, one);
            }
        }
    }

    }