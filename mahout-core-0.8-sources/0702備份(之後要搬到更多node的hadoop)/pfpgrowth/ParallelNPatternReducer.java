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
import java.util.*;

import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.common.Pair;


@Deprecated
public class ParallelNPatternReducer extends Reducer<Text,Text,Text,Text> {

  private int minSupport = 3;
  private String uri = "hdfs://node1:9000/user/root/output";
  private FileSystem fs;
  private Path filePath;
  private FSDataOutputStream outFile;
  
  public static long T_number = PFPGrowth.T_number;
  public static int sample_number = PFPGrowth.sample_number;  
  
  public int FrequentCount ;
  private OpenObjectIntHashMap<HashSet<String>> FrequentItemSet;
  private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                                 InterruptedException {
	ArrayList<String> ItemInformation = new ArrayList<String>(); 																		
	ArrayList<String> SampleInformation = new ArrayList<String>();	
	HashSet<String> KeyItemSet = new HashSet<String>();
	
	String [] KeyArray = key.toString().split("#");
	double KeyContinuallyMultiply = 1.0;
	double KeySampleContinuallyMultiply = 1.0;
	double KeySampleContinuallyMultiplyMinusOne = 1.0;
	for(int i=0;i<KeyArray.length;i++){
		String [] tmp = KeyArray[i].split(":");
		KeyContinuallyMultiply *= (Double.parseDouble(tmp[1])/(double)T_number);
		KeySampleContinuallyMultiply *= (Double.parseDouble(tmp[2])/(double)sample_number);
		KeySampleContinuallyMultiplyMinusOne *= ((Double.parseDouble(tmp[2])-1)/((double)sample_number-1));
		KeyItemSet.add(tmp[0]);
	}

    for (Text value : values) {
	String [] str_value = value.toString().split("_");
	ItemInformation.add(str_value[0]);
	SampleInformation.add(str_value[1]);
    }
	
	for(int i=0;i<ItemInformation.size();i++){
		String [] tmpI = ItemInformation.get(i).split(":");
		
		double TmpEss = KeyContinuallyMultiply*(Double.parseDouble(tmpI[1])/(double)T_number);
		double TmpSampleEss = KeySampleContinuallyMultiply*(Double.parseDouble(tmpI[2])/(double)sample_number);
		double TmpSampleEssMinusOne = KeySampleContinuallyMultiplyMinusOne*((Double.parseDouble(tmpI[2])-1)/((double)sample_number-1));
		HashSet<String> KeyItemSetAddI = new HashSet<String>(KeyItemSet);
		KeyItemSetAddI.add(tmpI[0]);
		
		long FPsizeI = Long.parseLong(SampleInformation.get(i).split("#")[0]);
		boolean [] FPListI = SampleListToBoolean(SampleInformation.get(i).split("#")[1]);
		for(int j=0;j<i;j++){
			String combination = combine(ItemInformation.get(i),ItemInformation.get(j));
			if(fMap.containsKey(combination)){	//need to compare
				String [] tmpJ = ItemInformation.get(j).split(":");
				HashSet<String> KeyItemSetAddIAddJ = new HashSet<String>(KeyItemSetAddI);
				KeyItemSetAddIAddJ.add(tmpJ[0]);
				
				if(FrequentItemSet.containsKey(KeyItemSetAddIAddJ))continue;//already see
				
				double Ess = TmpEss*Double.parseDouble(tmpJ[1]);
				double SampleEss = TmpSampleEss*Double.parseDouble(tmpJ[2]);
				double SampleEssMinusOne = TmpSampleEssMinusOne*(Double.parseDouble(tmpJ[2])-1);
				double SampleVar = SampleEss*SampleEssMinusOne + SampleEss - SampleEss*SampleEss;
				double RelationRate = SampleEss + PFPGrowth.HowManyStd*Math.sqrt(SampleVar);
				long FPsizeJ = Long.parseLong(SampleInformation.get(j).split("#")[0]);
				long MaxFPSize = FPsizeI<FPsizeJ?FPsizeI:FPsizeJ;
				
				if(Ess+((double)MaxFPSize-RelationRate)/(double)sample_number*(double)T_number < minSupport)continue;
				
				boolean [] FPListJ = SampleListToBoolean(SampleInformation.get(j).split("#")[1]);
				ArrayList<Long> FPIJ = frequent_pattern(FPListI,FPListJ);
				
				if(Ess+((double)FPIJ.size()-RelationRate)/(double)sample_number*(double)T_number > minSupport){
					FrequentItemSet.put(KeyItemSetAddIAddJ,FrequentCount++);
					outFile.writeBytes(key.toString()+ItemInformation.get(i)+"#"+ItemInformation.get(j)+"_"+FPIJ.size()+"#"+FPIJ+"\n");
				}
			}
		}
	}
    //context.write(key, new Text("1"));
   }
  
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
	minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
	int status = Integer.valueOf(params.get("status", "1"));
	int i = 0;
	
    for (Pair<String,Long> e : PFPGrowth.readFList(context.getConfiguration())) {
      fMap.put(e.getFirst(), i++);
    }
	
	FrequentItemSet = new OpenObjectIntHashMap<HashSet<String>>();
	FrequentCount = 0;
	
	fs = FileSystem.get(URI.create(uri), context.getConfiguration());
	Path filePath = new Path(uri+"/result_"+Integer.toString(status));
	outFile = fs.create(filePath);
  }
  
  public static boolean WhichSmallString(String a,String b){
		if(Long.parseLong(a)<Long.parseLong(b))return true;
		return false;
	}
	public static String combine(String a,String b){
		String first = a.split(":")[0];
		String second = b.split(":")[0];
		if(WhichSmallString(first,second))return first+"#"+second;
		return second+"#"+first;
	}
	 public static boolean [] SampleListToBoolean(String str){
		 String [] sample_str = str.split(", "); 
		 int sample_length = sample_str.length;
		 sample_str[0]=str.substring(1,sample_str[0].length());
		 sample_str[sample_length-1] = sample_str[sample_length-1].split("]")[0];
		 boolean [] tmp=new boolean [sample_number];
		 for(int i=0;i<sample_length;i++){
			if(Integer.valueOf(sample_str[i])<sample_number)
				tmp[Integer.valueOf(sample_str[i])]=true;
		 }
		 return tmp;
	 }

	 public static ArrayList<Long> frequent_pattern(boolean [] first,boolean [] second){
		 ArrayList<Long> tmp_frequent = new ArrayList<Long>();
		 for(int i=0;i<sample_number;i++){
			 if(first[i]&&second[i]){Long li = new Long(i);tmp_frequent.add(li);}
		 }
		 return tmp_frequent;
	  }	 
}
