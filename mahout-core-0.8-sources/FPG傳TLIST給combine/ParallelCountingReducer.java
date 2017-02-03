/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.fpm.pfpgrowth;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.mahout.common.Parameters;


import java.net.URI;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.lang.InterruptedException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.ArrayList;

/**
 *  sums up the item count and output the item and the count This can also be
 * used as a local Combiner. A simple summing reducer
 */
@Deprecated
public class ParallelCountingReducer extends Reducer<Text,Text,Text,LongWritable> {

  private int minSupport = 3;
  //private String uri = "hdfs://node1:9000/user/root/output";
  //private FileSystem fs;
  //private Path filePath;
  //private FSDataOutputStream outFile;
  
  public static ArrayList<int[]> table_list;
  public static int two_pattern_count = 0;
  public static int count = 0;
  
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                                 InterruptedException {
    int number_of_group = 1;	//number of group
	
	long sum = 0;
	int [] group = new int [number_of_group];
	
	//int number_of_group = 10000;
	//int [] group = new int [number_of_group];
	//String T_list = "";
	//int max = 0;
	//int int_value;
	String tmp = "";
    for (Text value : values) {
	String [] str_value = value.toString().split("#");
	/*
	String [] tmp = value.toString().split("#");
	for(int i=0;i<number_of_group;i++){
		group[i] += Integer.valueOf(tmp[i+1]);
	}
	*/
	
	//Pattern pattern = Pattern.compile("[0-9]+");
	//Matcher isNum = pattern.matcher(value.toString());
	//if(!isNum.matches())continue;
	
	//int_value = Integer.valueOf(value.toString());
	//max = max>int_value?max:int_value;
      context.setStatus("Parallel Counting Reducer :" + key);
	  //group[int_value%number_of_group]++;
	  //T_list += "#"+value.toString();
	  //T_list += value.toString();
	  

		//for(int i=0;i<number_of_group;i++){
//			tmp += value.toString().split(",")[1]+"#";
			//group[i] += Integer.valueOf(value.toString().split("#")[i+1]);
		//}
		
		/*
		String value_string = value.toString();
		for(int i=0;i<100;i++){
			 if(value_string.charAt(i)==','){
				sum += Integer.valueOf(value_string.substring(0,i));
				break;
				}
			 }
		*/	 
		if(str_value.length == 1)	 
			sum += Long.parseLong(str_value[0]);	/*************************************************/


    }
	//if(sum>=minSupport)
	//outFile.writeBytes(key.toString()+","+String.valueOf(sum)+"\n");
    context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
	
	

	
	//write to local

	/*
	if(sum>=minSupport){
	table_list.add(group);
	
	for(int i=0;i<count;i++){
	
	long Expectations = 0;	
		for(int j=0;j<number_of_group;j++)
			Expectations += (group[j]*(table_list.get(i)[j]));
			
		if(Expectations/1692000 >= minSupport){
			//outFile.writeBytes(String.valueOf(two_pattern_count++)+"\n");
		}
	}
	count++;
		//String group_list = String.valueOf(group[0]);
		//for(int i=1;i<number_of_group;i++)
		//	group_list += "#"+String.valueOf(group[i]);
			
		//==============================
        //outFile.writeBytes(group_list+"\n");
        //outFile.close();
		//==============================
		
		//==============================
		//System.out.println(group_list);
		//==============================
	}
	*/
	
	context.write(key, new LongWritable(sum));
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
	minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
	//fs = FileSystem.get(URI.create(uri), context.getConfiguration());
	//Path filePath = new Path(uri+"/result_"+context.getTaskAttemptID().getTaskID().getId());
	//outFile = fs.create(filePath);
	
	table_list = new ArrayList<int[]>();
  }


  
}
