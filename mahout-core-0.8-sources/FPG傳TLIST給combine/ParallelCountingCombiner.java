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
public class ParallelCountingCombiner extends Reducer<Text,Text,Text,Text> {
private int minSupport = 3;
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                                 InterruptedException {
 //   int number_of_group = 100000;	//number of group
//	int [] group = new int [number_of_group];
	
	long sum = 0;
	int int_value;
	StringBuilder tmp = new StringBuilder();
	int a=0;
	int b=0;
    for (Text value : values) {
	String [] str_value = value.toString().split("T");
	  //int_value = Integer.valueOf(value.toString());
	  //group[int_value]++;
      context.setStatus("Parallel Counting Reducer :" + key);
	  
//	Pattern pattern = Pattern.compile("[0-9]+");
//	Matcher isNum = pattern.matcher(value.toString().split(",")[0]);

	
	  if(str_value[0].length()==0){a++;	//from map
		tmp.append(value.toString());	//add T_ID
		//group[Integer.valueOf(value.toString().split("#")[1])]++;
//		group[Integer.valueOf(value.toString().split("T")[1])%number_of_group]++;
		sum += 1;
		}
	  else{b++;	//from combine

		//for(int i=0;i<number_of_group;i++){
//			tmp.append(value.toString().split(",")[1]+"#");
			//group[i] += Integer.valueOf(value.toString().split("#")[i+1]);
		//}
//		for(int i=1;i<str_value.length;i++)
//			group[Integer.valueOf(str_value[i])%number_of_group]++;
		sum += Long.parseLong(str_value[0]);	/*************************************************/
		}
    }
	String output_value;
	
//	if(b!=0){
	/*
		if(sum > minSupport/100){
			for (Text value : values) {
				String [] str = value.toString().split(",")[1].split("#");
				for(int i=0;i<str.length;i++){
					group[Integer.valueOf(str[i])]++;
				}
				
				//tmp.append(value.toString().split(",")[1]+"#");
			}
			for(int i=0;i<number_of_group;i++){
					tmp.append(String.valueOf(group[i])+"#");
				}
		}
		*/
//		output_value = String.valueOf(sum)+",";
		//System.out.println(key.toString()+","+String.valueOf(b)+","+String.valueOf(sum));
//		}
		
	if(a!=0){
		context.write(key,new Text(String.valueOf(sum)+tmp));
	}
	
	else {
//		for(int i=0;i<number_of_group;i++){
//			if(group[i]!=0){
//				context.write(key, new Text(String.valueOf(i)+"#"+String.valueOf(group[i])));	// for group to reduce
//			}
//		}
		context.write(key,new Text(String.valueOf(sum)));
//		output_value = String.valueOf(sum)+",";
		}

	
	/*
	for(int i=0;i<number_of_group;i++){
		output_value += "#";
		output_value += String.valueOf(group[i]);}
	*/
		//System.out.println(key.toString()+" : "+output_value);
		
    context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);

	//context.write(key, new Text(output_value));	// for wordcount to reduce
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
	minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
  }


  
}
