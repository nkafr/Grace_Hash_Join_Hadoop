/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.proxwvaseis.gracehashjoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author nikos
 */
public class Bucketize {
    

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    boolean firstparse=false;
    InputSplit split;
    Configuration conf;
    FileSplit fileSplit = null;
    String filename;
    int hashcode=0;
    String joinedattribute;
    
    @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String line = value.toString();
       List<String> attributes;
       
       
        conf=context.getConfiguration();
        split = context.getInputSplit();
        Class<? extends InputSplit> splitClass = split.getClass();

        
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) split;
        } else if (splitClass.getName().equals(
                "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
           try {
               Method getInputSplitMethod = splitClass
                       .getDeclaredMethod("getInputSplit");
               getInputSplitMethod.setAccessible(true);
               fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
           } catch (NoSuchMethodException ex) {
               Logger.getLogger(Bucketize.class.getName()).log(Level.SEVERE, null, ex);
           } catch (SecurityException ex) {
               Logger.getLogger(Bucketize.class.getName()).log(Level.SEVERE, null, ex);
           } catch (IllegalAccessException ex) {
               Logger.getLogger(Bucketize.class.getName()).log(Level.SEVERE, null, ex);
           } catch (IllegalArgumentException ex) {
               Logger.getLogger(Bucketize.class.getName()).log(Level.SEVERE, null, ex);
           } catch (InvocationTargetException ex) {
               Logger.getLogger(Bucketize.class.getName()).log(Level.SEVERE, null, ex);
           }
            
        }
  
       filename = fileSplit.getPath().getName();
       if(!firstparse){
           firstparse=true;
        }else{
           String tuple="";
            attributes = Arrays.asList(line.split(","));
            
            if(filename.equals(conf.get("leftTableName")))
                 joinedattribute=attributes.get(conf.getInt("Rpos", 0));   
            else if(filename.equals(conf.get("rightTableName")))
                 joinedattribute=attributes.get(conf.getInt("Spos", 0));
            
            //diwxnoume to attribute pou tha ginei join apo to S .txt (theloume to join attribute na emganizetai mia fora)
             for (String attribute : attributes) {
               if(attributes.indexOf(attribute)==context.getConfiguration().getInt("Spos", 0) && filename.equals(context.getConfiguration().get("rightTableName"))){

                }else{
                   tuple=tuple+","+attribute;      
                   }  
                }
             tuple=tuple.substring(1);
             
             word.set(filename+","+tuple);
            context.write(new Text(joinedattribute), word);
        }
     }     
  }
    
      
    
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> multipleOutputs;  
    Configuration conf;
    
    @Override
    protected void setup(Context context)
    throws IOException, InterruptedException {
    multipleOutputs = new MultipleOutputs<Text, Text>(context);
    conf=context.getConfiguration();
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      
        for (Text val : values) {
            if(val.toString().substring(0, 1).equals(conf.get("leftTableName").substring(0,1)))
                
                //eksodos ston R folder
                multipleOutputs.write("ROutputName", key, val.toString().substring(val.toString().indexOf(",")+1, val.toString().length()) ,"IntermediateR/R");
            else if(val.toString().substring(0, 1).equals(conf.get("rightTableName").substring(0,1)))
                //eksodos ton S folder
                multipleOutputs.write("SOutputName", key, val.toString().substring(val.toString().indexOf(",")+1, val.toString().length()),"IntermediateS/S");
        } 
    }
    
   @Override
    protected void cleanup(Context context)
    throws IOException, InterruptedException {
    multipleOutputs.close();
    } 
 }  
  
  
    
    
    
}
