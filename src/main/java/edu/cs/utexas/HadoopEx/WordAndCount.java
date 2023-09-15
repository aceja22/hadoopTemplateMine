package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;



public class WordAndCount implements Comparable<WordAndCount> {

    private final Text word;

    private final IntWritable count;

    public WordAndCount(Text word, IntWritable count){
        this.word = word;
        this.count = count;
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getCount() {
        return count;
    }

    public int compareTo(WordAndCount other){
        float diff = count.get() - other.count.get();
        if(diff > 0){
            return 1;
        }else if (diff < 0){
            return -1;
        }else{
            return 0;
        }
    }

    public String toString(){
        return word.toString() + " " + count.get();
    }

}