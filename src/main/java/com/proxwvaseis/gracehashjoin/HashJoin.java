/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.proxwvaseis.gracehashjoin;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 *
 * @author nikos
 */
public class HashJoin{

    public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
    
    
    HashMap hash=new HashMap<String, List<String>>(); 
    private Text joinedtuple=new Text();
    BufferedReader br;
    boolean marked=false;
    boolean firstparse;
    
    @Override
    protected void setup(Context context) throws IOException{
        FileSplit fileSplit = null;
        
        Configuration conf=context.getConfiguration();
        String rightpartitionpath=conf.get("rightpartitionpath");
        
        InputSplit split= context.getInputSplit();
        
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
        
        
        String leftpartitionname = fileSplit.getPath().getName();
        FileSystem fs=FileSystem.get(conf);
        
        String rightpartitionname="S"+leftpartitionname.substring(1);
        
        Path pt =new Path(rightpartitionpath+"/"+rightpartitionname);
        br=new BufferedReader(new InputStreamReader(fs.open(pt)),10485760);
        
    }
    
    String inputline;
    boolean nestedjoin=false;
    boolean onetomany=false;

    @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String line = value.toString();
       List<String> attributes=Arrays.asList(line.split(","));
       String tuple;
       
        if (!firstparse) {
            firstparse = initializeFile(context);
        }
       
        String joinedattribute=attributes.get(0).substring(0, attributes.get(0).indexOf("\t"));
        tuple=line.substring(line.indexOf("\t")+1, line.length());

        if (!marked) {
            br.mark(100000000);
            marked = true;
            inputline=br.readLine();
        }
        
        if(inputline!=null){   //an ta kleidia eine isa
        if(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)==0  ){
      
            while(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)==0 ){
                    context.write(NullWritable.get(), new Text(tuple+","+inputline.substring(inputline.indexOf("\t")+1, inputline.length())));

                    inputline=br.readLine();
                    
                    //den exoume periptwsh one to many join opote proxwrame parakatw
                    if(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)>0){
                        br.reset();//an teleiwsei to nested loop, ksemarkaroume to stream kai synexizoume
                        inputline=br.readLine();
                        onetomany=true;
                        break; 
                    }
                    
                    //kanoume ena nested loop join gia ena attribute ths R kai ola ths S pou exoun to idio kleidi
                    while(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)==0 && onetomany==false){
                       nestedjoin=true;
                       context.write(NullWritable.get(), new Text(tuple+","+inputline.substring(inputline.indexOf("\t")+1, inputline.length())));
                       inputline=br.readLine(); 
                       
                       if(inputline==null)
                               break; 
                    }
                    if(inputline==null)
                               break; 
                    
            }
            if(nestedjoin==true){
                br.reset();
                nestedjoin=false;
                marked=false;
            }
          }else if (inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)<0) { //an to S tuple einai mikrotero apo to R tuple

              //epanaferoume to stream twn S-tuples mexri kapoio S tuple na einai iso me to trexwn R-tuple
              while(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)<0){
                    inputline = br.readLine();
                    if(inputline==null)
                        break; 
                    }
                    
                    if(inputline!=null){    
                      if(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)==0  ){
                            
                        
                        while(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)==0 ){
                                context.write(NullWritable.get(), new Text(tuple+","+inputline.substring(inputline.indexOf("\t")+1, inputline.length())));
                                //koitazoume to epomeno kai an einai isa kanoume join kai auksanoume ton deikth sto stream 
                                inputline=br.readLine();

                                //an kai to epomeno einai iso me to R-tuple tote kanoume many-to-many-join, oso ta kleidia einai isa
                                while(inputline.substring(0, inputline.indexOf("\t")).compareTo(joinedattribute)==0){
                                   nestedjoin=true;
                                   context.write(NullWritable.get(), new Text(tuple+","+inputline.substring(inputline.indexOf("\t")+1, inputline.length())));
                                   inputline=br.readLine();

                                   if(inputline==null)
                                           break; 
                                }
                                if(inputline==null)
                                           break;
                            }
                            if(nestedjoin==true && marked!=false){
                                br.reset();
                                nestedjoin=false;
                                marked=false; //an teleiwsei to nested loop ksemarkaroume to stream kai synexizoume
                            }       
                      }
                      if(marked==true)
                          br.reset();
         }    
       
      }
     }
      
      
    }
      
    public static boolean initializeFile(Context context) throws IOException, InterruptedException {

       
          String firstline = "";
          List attributesR=Arrays.asList(context.getConfiguration().get("rline").split(","));
          List attributesS=Arrays.asList(context.getConfiguration().get("sline").split(","));
          
          for (Object attribute : attributesR) {
              firstline = firstline + attribute + ",";
          }
          
          for (Object attribute : attributesS) {
              if (attribute.equals(context.getConfiguration().get("joinAttribite"))) {
                  continue;
              }
              firstline = firstline + attribute + ",";
          }
          
          
          context.write(NullWritable.get(), new Text(firstline));
          context.write(NullWritable.get(), new Text(""));
          return true;
      }  
      
      
  
   }

    
    
}