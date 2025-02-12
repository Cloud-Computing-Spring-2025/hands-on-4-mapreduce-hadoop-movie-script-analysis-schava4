package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalWords = 0;

        // Sum up all word counts for the given character
        for (IntWritable count : values) {
            totalWords += count.get();
        }

        // Write the character name and total word count to the context
        context.write(key, new IntWritable(totalWords));
    }

    }