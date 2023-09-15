package edu.cs.utexas.HadoopEx;

import java.util.PriorityQueue;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class TopKMapper extends Mapper<Text, Text, Text, IntWritable> {

    private PriorityQueue<WordAndCount> pq;

    private Logger logger = Logger.getLogger(TopKMapper.class);

    public void setup(Context context){
        pq = new PriorityQueue<>();
    }

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        int count = Integer.parseInt(value.toString());
        pq.add(new WordAndCount(new Text(key), new IntWritable(count)));

        if(pq.size() > 10){
            pq.poll();
        }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        while(pq.size() > 0){
            WordAndCount wordAndCount = pq.poll();
            context.write(wordAndCount.getWord(), wordAndCount.getCount());
            logger.info("TopKMapper PQ Status: " + pq.toString());
        }

    }
}