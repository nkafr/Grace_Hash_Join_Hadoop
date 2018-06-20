/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.proxwvaseis.gracehashjoin;



import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author nikos
 */
public class GroupBy {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    boolean firstparse=false;
    boolean secondparse=false;
    int count=0;
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text("count");


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       
       if(firstparse==false && secondparse==false){
           firstparse=true;
       }else if(firstparse==true && secondparse==false){
           secondparse=true;
       }else if(firstparse==true && secondparse==true){

           
           word.set(value);
           
           context.write(word, one);
       } 
     } 
  }
    
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
  }
    
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
       
       private IntWritable result = new IntWritable();
       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
         throws IOException, InterruptedException {
           int sum = 0;
          for (IntWritable val : values) {
               sum += val.get();
           }
            result.set(sum);
           context.write(key, result);
       }
   } 
  
}
