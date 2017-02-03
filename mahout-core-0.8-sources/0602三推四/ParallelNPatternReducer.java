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
	
	ArrayList<ArrayList<Integer>> ID = new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Long>> C = new ArrayList<ArrayList<Long>>();
	ArrayList<ArrayList<Long>> SC = new ArrayList<ArrayList<Long>>();
	ArrayList<ArrayList<Long>> FP = new ArrayList<ArrayList<Long>>();
	
	for(int i=0;i<item.size();i++){//2 to 3
		for(int j=0;j<i;j++){
			ArrayList<Long> fp_list = frequent_pattern(Boolean_sample_list.get(i),Boolean_sample_list.get(j));
			int fp_size = fp_list.size();
			double relation_rate = get_relation_rate(sample_count.get(i),sample_count.get(j),fp_size,KeySampleCount);
			
			if(Ess.get(i)*Ess.get(j)*(1/(double)Keycount)+relation_rate*T_number >= minSupport){
				 ArrayList<Integer> IDList = new ArrayList<Integer>();
				 ArrayList<Long> CList = new ArrayList<Long>();
				 ArrayList<Long> SCList = new ArrayList<Long>();
				 
				 IDList.add(Integer.valueOf(item.get(i)));
				 IDList.add(Integer.valueOf(item.get(j)));
				 CList.add(count.get(i));
				 CList.add(count.get(j));
				 SCList.add(sample_count.get(i));
				 SCList.add(sample_count.get(j));
				 
				 ID.add(IDList);
				 C.add(CList);
				 SC.add(SCList);
				 FP.add(fp_list);
				 
				 //outFile.writeBytes(Keyitem+" ");
				 //IDList.add(Integer.valueOf(Keyitem));
				 Collections.sort(IDList);
				// outFile.writeBytes(String.valueOf(IDList)+"\n");
				//outFile.writeBytes(Keyitem+","+item.get(i)+","+item.get(j)+"\n");
			}
		}
	}
//===========================================================================================================================

	ArrayList<ArrayList<Integer>> new_ID = new ArrayList<ArrayList<Integer>>();
	ArrayList<ArrayList<Long>> new_C = new ArrayList<ArrayList<Long>>();
	ArrayList<ArrayList<Long>> new_SC = new ArrayList<ArrayList<Long>>();
	ArrayList<ArrayList<Long>> new_FP = new ArrayList<ArrayList<Long>>();
	
	//3 to 4...to n
	//ID , C , SC , FP
	//while(true){
	//outFile.writeBytes(ID.size()+"\n");
	//if(ID.size()==0)break;
	if(ID.size()!=0){
	int status = ID.get(0).size();//status to status+1
	
	new_ID.clear();
	new_C.clear();
	new_SC.clear();
	new_FP.clear();
	
	for(int i=0;i<ID.size();i++){
		Set<Integer> intSet = new HashSet<Integer>();
		for(int k=0;k<status;k++){
			intSet.add(ID.get(i).get(k));
			}
		for(int j=0;j<i;j++){
			Set<Integer> intSet2 = new HashSet<Integer>(intSet);
			int different_item=0;
			for(int k=0;k<status;k++){
				intSet2.add(ID.get(j).get(k));
				if(intSet2.size()==status+1){different_item=k;break;}//location for j
			}
			if(intSet2.size()!=status+1)continue;
			
			ArrayList<Integer> tmp_ID = new ArrayList<Integer>(ID.get(i));
			ArrayList<Long> tmp_C = new ArrayList<Long>(C.get(i));
			ArrayList<Long> tmp_SC = new ArrayList<Long>(SC.get(i));

			tmp_ID.add(ID.get(j).get(different_item));
			tmp_C.add(C.get(j).get(different_item));
			tmp_SC.add(SC.get(j).get(different_item));
			ArrayList<Long> tmp_FP = frequent_pattern(FP.get(i),FP.get(j));
			
			// function(tmp_C,tmp_SC,tmp_FP)
			//long multi_cl=1,multi_scl=1,ts=1,ss=1;
			double multi_cl=1.0,multi_scl=1.0;
			for(int k=0;k<tmp_C.size();k++)multi_cl*=((double)tmp_C.get(k)/(double)T_number);
			for(int k=0;k<tmp_SC.size();k++)multi_scl*=((double)tmp_SC.get(k)/(double)sample_number);
			//for(int k=0;k<status+1;k++){ts*=T_number;ss*=sample_number;}
			//ts = T_number*T_number*T_number;
			//ss = sample_number*sample_number*sample_number;
			multi_cl *= (double)Keycount;
			multi_scl *= (double)KeySampleCount;
			
			double essti = multi_cl;
			double rr = ((double)(tmp_FP.size())-multi_scl)/(double)sample_number;
			if(essti+rr*T_number >= minSupport){
				new_ID.add(tmp_ID);
				new_C.add(tmp_C);
				new_SC.add(tmp_SC);
				new_FP.add(tmp_FP);
				tmp_ID.add(Integer.valueOf(Keyitem));
				Collections.sort(tmp_ID);
				outFile.writeBytes(String.valueOf(tmp_ID)+"\n");
			}
		}
	}
			ID.clear();
			C.clear();
			SC.clear();
			FP.clear();
			
			ID.addAll(new_ID);
			C.addAll(new_C);
			SC.addAll(new_SC);
			FP.addAll(new_FP);
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
	 
	 public static boolean [] LongListToBoolean(ArrayList<Long> Llist){
		boolean [] tmp=new boolean [sample_number];
		for(int i=0;i<Llist.size();i++){
			int tmp_i = (int)(long)Llist.get(i);
			tmp[tmp_i]=true;}
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
		 return tmp_frequent;
	  }	 
	  
	  public static ArrayList<Long> frequent_pattern(ArrayList<Long> first,ArrayList<Long> second){
		ArrayList<Long> tmp_frequent = new ArrayList<Long>();
		boolean [] tmp1 = LongListToBoolean(first);
		boolean [] tmp2 = LongListToBoolean(second);
		return frequent_pattern(tmp1,tmp2);
	  }
	  
	  public static double get_relation_rate(long a,long b,long real,long key_sample_count){
		double ess = (double)a*(double)b*(double)key_sample_count*(1/((double)sample_number*(double)sample_number));
		return ((double)real-ess)/(double)sample_number;	
	  }
	  

  
}
