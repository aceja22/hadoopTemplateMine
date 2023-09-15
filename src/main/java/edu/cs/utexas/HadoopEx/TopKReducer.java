package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.PriorityQueue;
import org.apache.log4j.Logger;

import java.io.IOException;

public class TopKReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private PriorityQueue<WordAndCount> pq = new PriorityQueue<>(10);
    
    private Logger logger = Logger.getLogger(TopKReducer.class);


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

       int counter = 0;

        for(IntWritable value : values){
            counter +=1;
            logger.info("Reducer text: counter is " + counter);
            logger.info("Reducer text add this item: " + new WordAndCount(key, value).toString());

            pq.add(new WordAndCount(new Text(key), new IntWritable(value.get())));

            logger.info("Reducer text: " + key.toString() + " , count : " + value.toString());
            logger.info("PQ status " + pq.toString());
        }

        while(pq.size() > 10){
            pq.poll();
        }

        while(!pq.isEmpty()){
            WordAndCount wordAndCount = pq.poll();
            context.write(wordAndCount.getWord(), wordAndCount.getCount());
        }
    }

}