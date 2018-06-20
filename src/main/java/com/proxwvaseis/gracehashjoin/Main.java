/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.proxwvaseis.gracehashjoin;

import com.proxwvaseis.gracehashjoin.Bucketize.Map;
import com.proxwvaseis.gracehashjoin.Bucketize.Reduce;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author nikos
 */
public class Main {

    
    
    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    
    /*String leftpartitions="src/main/resources/Intermediate7/IntermediateR";
    String rightpartitions="src/main/resources/Intermediate7/IntermediateS";
    String pathToLeft="src/main/resources/FullInput/r.txt";
    String pathToRight="src/main/resources/FullInput/s.txt";
    String pathToIntermediate="src/main/resources/Intermediate7";
    String pathToOutput="src/main/resources/output6";
    String pathToGroupBy="src/main/resources/groupby4";
    String joinAttribute="foobar";*/
    
    String leftpartitions="/user/advdb01/Intermediate/IntermediateR";
    String rightpartitions="/user/advdb01/Intermediate/IntermediateS";
    
    String pathToLeft=args[0];
    String pathToRight=args[1];
    String pathToIntermediate="/user/advdb01/Intermediate";
    String pathToOutput="/user/advdb01/Final";
    String pathToGroupBy="/user/advdb01/GroupBy";
    String joinAttribute=args[2];
    conf.set("mapred.reduce.tasks", args[3]);
    
    
    String leftTableName = pathToLeft.substring(pathToLeft.lastIndexOf('/') + 1);
    String rightTableName = pathToRight.substring(pathToRight.lastIndexOf('/') + 1);
    
    conf.set("pathToLeft", pathToLeft);
    conf.set("pathToRight", pathToRight);
    conf.set("joinAttribite", joinAttribute);
    conf.set("leftTableName", leftTableName);
    conf.set("rightTableName", rightTableName);
    
    conf.set("rightpartitionpath", rightpartitions);
    
    //////////////////////////////////////////////////////////////////////////////////////
                            /****    FIRST STEP - CREATE BUCKETS *******/
    //////////////////////////////////////////////////////////////////////////////////////
    final long startA1 = System.nanoTime();
    
    FileSystem fs = FileSystem.get(conf);
    Path pt=new Path(args[0]);
    //Path pt=new Path("src/main/resources/FullInput/r.txt");
    String br1=new BufferedReader(new InputStreamReader(fs.open(pt))).readLine();
    
    Path pt2=new Path(args[1]);
    //Path pt2=new Path("src/main/resources/FullInput/s.txt");
    String br2=new BufferedReader(new InputStreamReader(fs.open(pt2))).readLine();

    validateJoin(br1,br2,joinAttribute);
    FindJoinColumns(br1,br2,conf);
    
    conf.set("rline", br1);
    conf.set("sline", br2);
    
    Job job = new Job(conf, "bucketize");
    
    job.setJarByClass(Main.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    MultipleInputs.addInputPath(job, new Path(pathToLeft) ,TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(pathToRight),TextInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(pathToIntermediate));
    
    MultipleOutputs.addNamedOutput(job, "ROutputName", TextOutputFormat.class, IntWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "SOutputName", TextOutputFormat.class, IntWritable.class, Text.class);
    
    conf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
    job.waitForCompletion(true);
   
    final long startA2 = System.nanoTime();
    
    System.out.println("\033[32;1mPartitioning took: " + ((startA2 - startA1) / 1000000) + "ms - "+ ((startA2 - startA1) / 1000000000)+"s\033[0m");
    
    

    
    //////////////////////////////////////////////////////////////////////////////////////
                            /****    SECOND STEP - GRACE HASH  JOIN *******/
    //////////////////////////////////////////////////////////////////////////////////////
    
     
    
        long startB1 = System.nanoTime();
            
        Job job2 = new Job(conf, "hashjoin");
         

        job2.setJarByClass(Main.class);

        LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setMapperClass(HashJoin.Map.class);


        FileInputFormat.addInputPath(job2, new Path(leftpartitions));
        FileOutputFormat.setOutputPath(job2, new Path(pathToOutput));
        job2.waitForCompletion(true);
        
        
        final long startB2 = System.nanoTime();
    
        System.out.println("\033[35;1mHash Join took: " + ((startB2 - startB1) / 1000000) + "ms - "+ ((startB2 - startB1) / 1000000000)+"s\033[0m");
        
        
    
    
    
//////////////////////////////////////////////////////////////////////////////////////
                            /****    THIRD STEP - GROUP BY *******/
    //////////////////////////////////////////////////////////////////////////////////////
    
    
    final long startC1 = System.nanoTime();
    
    Job job3 = new Job(conf, "groupby");
    job3.setJarByClass(Main.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);
    
    LazyOutputFormat.setOutputFormatClass(job3, TextOutputFormat.class);
    job3.setMapOutputKeyClass(Text.class); 
    job3.setCombinerClass(GroupBy.IntSumReducer.class);
    job3.setMapperClass(GroupBy.Map.class);
    job3.setReducerClass(GroupBy.Reduce.class); 
        
   
    
    FileInputFormat.setInputPaths(job3, new Path(pathToOutput));
    FileOutputFormat.setOutputPath(job3, new Path(pathToGroupBy));

    job3.waitForCompletion(true);
     
    
    
     final long startC2 = System.nanoTime();
    
    System.out.println("\033[34;1mGroup By took: " + ((startC2 - startC1) / 1000000) + "ms - "+ ((startC2 - startC1) / 1000000000)+"s\033[0m");
    }
  
    
    public static void validateJoin(String br1, String br2, String joinattribute){
        
        boolean ok1=false;
        boolean ok2=false;
        
        List temp1=Arrays.asList(br1.split(","));
        List temp2=Arrays.asList(br2.split(","));
        
        for(Object tmp:temp1){
            if(tmp.equals(joinattribute)){
                ok1=true;
            }   
        }
        
        for(Object tmp:temp2){
            if(tmp.equals(joinattribute)){
                ok2=true;
            }   
        }
        
        if(ok1==false || ok2==false){
            System.err.println("Join Attribute does not exist in both columns");
            System.exit(2);
        }
        
    }
    
    public static void FindJoinColumns(String br1, String br2, Configuration conf){
        
        List attributesR=Arrays.asList(br1.split(","));
        List attributesS=Arrays.asList(br2.split(","));
        int Rpos = 0;
        int Spos = 0;
        
        for( Object attribute:attributesR){
                                
                if(attribute.equals(conf.get("joinAttribite"))){
                    
                    Rpos=attributesR.indexOf(attribute);
                }
            }
        
        for( Object attribute:attributesS){
                                
                if(attribute.equals(conf.get("joinAttribite"))){
                    
                    Spos=attributesS.indexOf(attribute);
                }
            }
        
        conf.setInt("Rpos", Rpos);
        conf.setInt("Spos", Spos);
        
        
    }
    
}