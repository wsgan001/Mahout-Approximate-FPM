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
  private String uri = "hdfs://node1:9000/user/root/output";
  private FileSystem fs;
  private Path filePath;
  private FSDataOutputStream outFile;
  
  public static ArrayList<long[]> B_table_list;	//for Big
  public static ArrayList<long[]> S_table_list;	//for small
  public static	ArrayList<boolean[]> B_sample_list ;
  public static	ArrayList<boolean[]> S_sample_list ;
  public static	ArrayList<Long> B_list ;
  public static ArrayList<Long> S_list ;

  public static int two_pattern_count = 0;
  public static int count = 0;
  
  public static long T_number = 1692000;
  public static long number_of_group = 1;
  public static long group_size = T_number/number_of_group;
  
  public static int sample_number = 10000;
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
																				InterruptedException {
	long sum = 0;
	int NOG = (int)number_of_group;
	long [] group = new long [NOG];
	
	boolean [] T_sample = new boolean [sample_number];
	String tmp = "";
    for (Text value : values) {
	String [] str_value = value.toString().split("#");
      context.setStatus("Parallel Counting Reducer :" + key);
		if(str_value.length == 1){	 
			sum += Long.parseLong(str_value[0]);	/*************************************************/
			}
		else{
			T_sample[Integer.valueOf(str_value[1])]=true;
		}

    }

    context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
	
//================================================================================================================
	
	double Bound = Math.sqrt(T_number*minSupport);	//determine B or S
	double LMS = (double)minSupport/(double)number_of_group;
	
	if(sum>=Bound){	//B
	
		two_pattern_count += B_list.size();	// for BB
		outFile.writeBytes(String.valueOf(two_pattern_count)+"\n");
		
		B_list.add(sum);
		B_sample_list.add(T_sample);
		
		for(int i=0;i<S_list.size();i++){	//for BS
		double relation_rate = relation(S_sample_list.get(i),T_sample,sample_number);
		double Ess = E(S_list.get(i),sum,T_number);
		if(relation_rate>0){	//real > ess
			if(Ess>= minSupport){
				outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
			else if(Ess<minSupport&&Ess+relation_rate*T_number>minSupport){
				outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
			}
		}
		else if(relation_rate == 0){	// just see ess
			if(Ess>= minSupport){
				outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
		}
		else{	//real < ess
			if(Ess>= minSupport && Ess+relation_rate*T_number>minSupport){
				outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
		}
		}//end of BS

	}//end of B
	
	if(sum >= minSupport && sum < Bound){	//S
		for(int i=0;i<S_list.size();i++){	//for SS
			double relation_rate = relation(S_sample_list.get(i),T_sample,sample_number);
			double Ess = E(S_list.get(i),sum,T_number);
			if(relation_rate>0){
				if(Ess+relation_rate*T_number>minSupport){
					outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
			}
		}
		S_list.add(sum);
		S_sample_list.add(T_sample);
		for(int i=0;i<B_list.size();i++){	//for SB
			double relation_rate = relation(B_sample_list.get(i),T_sample,sample_number);
			double Ess = E(B_list.get(i),sum,T_number);
			if(relation_rate>0){
			if(Ess>= minSupport){
				outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
			else if(Ess<minSupport&&Ess+relation_rate*T_number>minSupport){
				outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
			}
			}
			
			else if(relation_rate == 0){	// just see ess
				if(Ess>= minSupport){
					outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
			}
			
			else{	//real < ess
				if(Ess>= minSupport && Ess+relation_rate*T_number>minSupport){
					outFile.writeBytes(String.valueOf(++two_pattern_count)+"\n");
				}
			}
			
		}
	}

	context.write(key, new LongWritable(sum));
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
	minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
	fs = FileSystem.get(URI.create(uri), context.getConfiguration());
	Path filePath = new Path(uri+"/result_"+context.getTaskAttemptID().getTaskID().getId());
	outFile = fs.create(filePath);
	
	//table_list = new ArrayList<long[]>();
	B_table_list = new ArrayList<long[]>();
	S_table_list = new ArrayList<long[]>();
	B_sample_list = new ArrayList<boolean[]>();
	S_sample_list = new ArrayList<boolean[]>();
	B_list = new ArrayList<Long>();	
	S_list = new ArrayList<Long>();	
  }

  	 public static double S(long a,long b,long X){
			double M = E(a,b,X);
			double N = E(a-1,b-1,X-1);
			return Math.sqrt(M*N+M-M*M);
		}
	public static double V(long a,long b,long X){
			double M = E(a,b,X);
			double N = E(a-1,b-1,X-1);
			return (M*N+M-M*M);
		}
	 public static double E(long a,long b,long X){
		 return (double)a*(double)b/(double)X;
	 }
	 
	  public static boolean status(boolean [] first,boolean [] second,int X){
		 int a=0;
		 int b=0;
		 int c=0;
		 for(int i=0;i<X;i++){
			 if(first[i])a++;
			 if(second[i])b++;
			 if(first[i]&&second[i])c++;
		 }
		 if(a*b/X-S(a,b,X) <= c)
			 return true;
		 return false;
	 }
	 
	 public static double relation(boolean [] first,boolean [] second,long X){
		 long a=0;
		 long b=0;
		 long real=0;
		 for(int i=0;i<X;i++){
			 if(first[i])a++;
			 if(second[i])b++;
			 if(first[i]&&second[i])real++;
		 }
		 double ess = E(a,b,X);
		 double std = S(a,b,X);
		 double uper_bound = ess+3*std;
		 double lower_bound = ess-3*std;
		 if(real > uper_bound)	// + relation
			return ((double)real-uper_bound)/(double)X;	//	>0
		else if (real < lower_bound)	// - relation
			return ((double)real-lower_bound)/(double)X;	//	<0
		 return 0;	// =0
	 }

  
}
