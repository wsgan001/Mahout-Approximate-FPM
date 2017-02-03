package org.apache.mahout.fpm.pfpgrowth;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Parameters;


@Deprecated
public class ParallelNPatternMapper extends Mapper<LongWritable,Text,Text,Text> {
  
  private Pattern splitter;
  
  @Override
  protected void map(LongWritable offset, Text input, Context context) throws IOException,
                                                                      InterruptedException {
//item1:count1#item2:count2 Ess,relation_rate
	
	
    String[] line = splitter.split(input.toString());/**/
	String[] frequent_items = line[0].split("#");
	//String Ess = line[1].split(",")[0];
	//String relation_rate = line[1].split(",")[1];
	
	for(int i=0;i<frequent_items.length;i++){
		StringBuilder tmp = new StringBuilder();
		for(int j=0;i<frequent_items.length;j++){
			if(i!=j){
				tmp.append(frequent_items[j]+"#");
			}
		}
		context.write(new Text(tmp), new Text(frequent_items[i]+","+line[1]));
//	Key : item1:count1#		Value : item2:count2,Ess,relation_rate
	}
    
  }
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Parameters params = new Parameters(context.getConfiguration().get(PFPGrowth.PFP_PARAMETERS, ""));
    splitter = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN, PFPGrowth.SPLITTER.toString()));
  }
}
