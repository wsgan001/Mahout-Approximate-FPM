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

@Deprecated
public class ParallelNPatternReducer extends Reducer<Text,Text,Text,Text> {

  private int minSupport = 3;
  private String uri = "hdfs://node1:9000/user/root/output";
  private FileSystem fs;
  private Path filePath;
  private FSDataOutputStream outFile;
  
  public static long T_number = 1692000;
  public static int sample_number = 10000;  
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                                 InterruptedException {
	 	ArrayList<String> item = new ArrayList<String>(); 
     	ArrayList<Long> count = new ArrayList<Long>();
		ArrayList<Long> sample_count = new ArrayList<Long>();
	 	ArrayList<Double> Ess = new ArrayList<Double>();
	 	//ArrayList<Double> relation_rate = new ArrayList<Double>();
		
		ArrayList<boolean[]> Boolean_sample_list = new ArrayList<boolean[]>();
	
	  String Keyitem = key.toString().split("#")[0].split(":")[0];
	  long Keycount = Integer.valueOf(key.toString().split("#")[0].split(":")[1]);
	  long KeySampleCount = Integer.valueOf(key.toString().split("#")[0].split(":")[2]);

    for (Text value : values) {
	String [] str_value = value.toString().split("#");
	
	String tmp_item = str_value[0].split(":")[0];
	long tmp_count = Integer.valueOf(str_value[0].split(":")[1]);
	long tmp_sample_count = Integer.valueOf(str_value[0].split(":")[2]);
	double tmp_Ess = Double.parseDouble(str_value[1]);
	//double tmp_relation_rate = Double.parseDouble(str_value[2]);
	
	item.add(tmp_item);
	count.add(tmp_count);
	sample_count.add(tmp_sample_count);
	Ess.add(tmp_Ess);
	//relation_rate.add(tmp_relation_rate);
	Boolean_sample_list.add(SampleListToBoolean(str_value[2]));
	
    }
	
	for(int i=0;i<item.size();i++){
		for(int j=0;j<i;j++){
			ArrayList<Long> fp_list = frequent_pattern(Boolean_sample_list.get(i),Boolean_sample_list.get(j));
			int fp_size = fp_list.size();
			double relation_rate = get_relation_rate(sample_count.get(i),sample_count.get(j),fp_size-2,KeySampleCount);
			
			if(Ess.get(i)*Ess.get(j)*(1/(double)Keycount)+relation_rate*T_number >= minSupport){
				long tmp_a = Integer.valueOf(Keyitem);
				long tmp_b = Integer.valueOf(item.get(i));
				long tmp_c = Integer.valueOf(item.get(j));
				if(tmp_a>tmp_b&&tmp_a>tmp_c){
					if(tmp_b>tmp_c)//abc
						outFile.writeBytes(Keyitem+","+item.get(i)+","+item.get(j)+"\n");
					else//acb
						outFile.writeBytes(Keyitem+","+item.get(j)+","+item.get(i)+"\n");
				}
				if(tmp_b>tmp_a&&tmp_b>tmp_c){
					if(tmp_a>tmp_c)//bac
						outFile.writeBytes(item.get(i)+","+Keyitem+","+item.get(j)+"\n");
					else//bca
						outFile.writeBytes(item.get(i)+","+item.get(j)+","+Keyitem+"\n");
				}
				if(tmp_c>tmp_a&&tmp_c>tmp_b){
					if(tmp_a>tmp_b)//cab
						outFile.writeBytes(item.get(j)+","+Keyitem+","+item.get(i)+"\n");
					else//cba
						outFile.writeBytes(item.get(j)+","+item.get(i)+","+Keyitem+"\n");
				}
				//outFile.writeBytes(Keyitem+","+item.get(i)+","+item.get(j)+"\n");
			}
		}
	}
    //context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
    context.write(key, new Text("1"));
    
  }
  
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
	minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
	fs = FileSystem.get(URI.create(uri), context.getConfiguration());
	Path filePath = new Path(uri+"/result_1");
	outFile = fs.create(filePath);
  }
  
  public static double E(long a,long b,long X){
		 return (double)a*(double)b/(double)X;
	 }
	 
	 public static boolean [] SampleListToBoolean(String str){
		 String [] sample_str = str.split(", "); 
		 int sample_length = sample_str.length;
		 sample_str[0]=str.substring(1,sample_str[0].length());
		 sample_str[sample_length-1] = sample_str[sample_length-1].split("]")[0];
		 boolean [] tmp=new boolean [sample_number];
		 for(int i=0;i<sample_length;i++){
			 tmp[Integer.valueOf(sample_str[i])]=true;
		 }
		 return tmp;
	 }
	 
	 public static ArrayList<Long> frequent_pattern(boolean [] first,boolean [] second){
		 ArrayList<Long> tmp_frequent = new ArrayList<Long>();
		 long a=0;
		 long b=0;
		 //long real=0;
		 for(int i=0;i<sample_number;i++){
			 if(first[i])a++;
			 if(second[i])b++;
			 if(first[i]&&second[i]){Long li = new Long(i);tmp_frequent.add(li);}
		 }
		 tmp_frequent.add(a);
		 tmp_frequent.add(b);
		 return tmp_frequent;
	  }	 
	  
	  public static double get_relation_rate(long a,long b,long real,long key_sample_count){
		double ess = (double)a*(double)b*(double)key_sample_count*(1/((double)sample_number*(double)sample_number));
		return ((double)real-ess)/(double)sample_number;	
	  }
  
}
