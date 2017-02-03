package org.apache.mahout.fpm.pfpgrowth;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@Deprecated
public class ParallelNPatternReducer extends Reducer<Text,Text,Text,Text> {

  private int minSupport = 3;
  private String uri = "hdfs://node1:9000/user/root/output";
  private FileSystem fs;
  private Path filePath;
  private FSDataOutputStream outFile;
  
  public static long T_number = 1692000;
  
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                                                                                 InterruptedException {
	public static	ArrayList<String> item ;
    public static	ArrayList<Long> count ;
	public static	ArrayList<Double> Ess ;
	public static	ArrayList<Double> relation_rate ;
	
	public static String Keyitem = key.toString().split("#")[0];
	public static double Keycount = Double.parseDouble(key.toString().split("#")[1]);

    for (Text value : values) {
	String [] str_value = value.toString().split(",");
	
	String tmp_item = str_value[0].split(":")[0];
	long tmp_count = Integer.valueOf(str_value[0].split(":")[1]);
	double tmp_Ess = Double.parseDouble(str_value[1]);
	double tmp_relation_rate = Double.parseDouble(str_value[2]);
	
	item.add(tmp_item);
	count.add(tmp_count);
	Ess.add(tmp_Ess);
	relation_rate.add(tmp_relation_rate);
	
    }
	
	for(int i=0;i<item.size();i++){
		for(int j=0;j<i;j++){
			if(Ess.get(i)*Ess.get(j)*(1/Keycount)+(relation_rate.get(i)+relation_rate.get(j))*T_number >= minSupport){
				outFile.writeBytes(Keyitem+","+item.get(i)+","+item.get(j)+"\n");
			}
		}
	}
    context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
    context.write(key, new Text(sum));
    
  }
  
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
	minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
	fs = FileSystem.get(URI.create(uri), context.getConfiguration());
	Path filePath = new Path(uri+"/result_1");
	outFile = fs.create(filePath);

	
  }
  
}
