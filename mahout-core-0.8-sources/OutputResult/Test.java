import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;


public class Test {

	public static long TNumber = 0;
	  public static int SampleNumber = 0;
	  public static long MinSup = 0;
	
	 public static void main(String[] args) throws IOException { 
		 long begin = System.currentTimeMillis();
		 
		 TNumber = Long.valueOf(args[0]);
		 SampleNumber = Integer.valueOf(args[1]);
		 MinSup = Long.valueOf(args[2]);

		 ArrayList<String> InputLine = new ArrayList<String>();
		 ArrayList <HashSet<String>> FrequentItemSet = new  ArrayList <HashSet<String>>();

		 FileReader fr = new FileReader(args[3]);
		 BufferedReader br = new BufferedReader(fr);
		 
		 int LineNumber = 0;
         while (br.ready()) {
        	 
         InputLine.add(br.readLine()); //先存每一行再說
         String [] InputTmpString = (InputLine.get(LineNumber++).split("_")[0]).split("#");	//item1:count1:sample_count1#item2:count2:sample_count2#...
         
         HashSet<String> TmpSet = new HashSet<String>(Arrays.asList(InputTmpString));
         FrequentItemSet.add(TmpSet); //計綠input裡面的frequent itemset
         

         }
         fr.close();
		 
         ArrayList <HashSet<String>> Fail = new  ArrayList <HashSet<String>>();
         ArrayList <HashSet<String>> Success = new  ArrayList <HashSet<String>>();
         
         for(int i=0;i<FrequentItemSet.size();i++){
        	 int status = FrequentItemSet.get(i).size();
			 boolean [] first_boolean_list = SampleListToBoolean((InputLine.get(i).split("_")[1]).split("#")[1]);
        	 for(int j=0;j<i;j++){
        		 HashSet<String> intersection = new HashSet<String>(FrequentItemSet.get(i));//兩frequent itemset 交集
        		 HashSet<String> union = new HashSet<String>(FrequentItemSet.get(i));//兩frequent itemset 連集
        		 
        		 intersection.retainAll(FrequentItemSet.get(j));
        		 union.addAll(FrequentItemSet.get(j));
        		 
        		 if(Success.contains(union)||Fail.contains(union))continue;
        		 
        		 HashSet<String> difference = new HashSet<String>(union);//交集-連集
        		 difference.removeAll(intersection);
        		 
        		 if(intersection.size() == status-1 && FrequentItemSet.contains(difference)){
        			 ArrayList<Double> EssandRR = GetEssAndRR(union);//0:Ess 1:ess+2s
        			 long MaxFPSize = GetMinRR(InputLine.get(i),InputLine.get(j));
        			 if(EssandRR.get(0)+((double)MaxFPSize-EssandRR.get(1))/(double)SampleNumber*(double)TNumber<MinSup)continue;
        			 
        			 ArrayList<Long> FPList = frequent_pattern(first_boolean_list,SampleListToBoolean((InputLine.get(j).split("_")[1]).split("#")[1]));
        			 
        			 
        			 //ArrayList<Long> FPList = frequent_pattern((InputLine.get(i).split("_")[1]).split("#")[1],(InputLine.get(j).split("_")[1]).split("#")[1]);
        			 if(EssandRR.get(0)+((double)FPList.size()-EssandRR.get(1))/(double)SampleNumber*(double)TNumber>MinSup){
        				 Success.add(union);
        			 }
        			 else Fail.add(union);
        			 //System.out.println(union);
        		 	}
        		 }
        	 }
         Iterator it = Success.iterator();
         while (it.hasNext())
         System.out.println(it.next());
		 long end = System.currentTimeMillis();
		 System.out.println("It takes "+String.valueOf((end-begin)*0.001)+" seconds");
}
	 
	 
	 public static long GetMinRR(String InputStr1,String InputStr2){
		 long a = Long.parseLong(InputStr1.split("_")[1].split("#")[0]);
		 long b = Long.parseLong(InputStr2.split("_")[1].split("#")[0]);
		 if(a<b)return a;
		 return b;
	 }
	 
	 
	 public static ArrayList<Double> GetEssAndRR(HashSet<String> InputSet){
		 ArrayList<Double> Answer = new ArrayList<Double>();
		 Iterator it = InputSet.iterator();
		 double Esstimation = 1.0;
		 double EsstiSample = 1.0;
		 double EsstiSampleMinusOne = 1.0;
		 
		 while (it.hasNext()){
			 String [] EachItem = it.next().toString().split(":");
			 Esstimation *= ((double)Long.valueOf(EachItem[1]) / (double)TNumber);
			 EsstiSample *= ((double)Long.valueOf(EachItem[2]) / (double)SampleNumber);
			 EsstiSampleMinusOne *= ((double)(Long.valueOf(EachItem[2])-1) / ((double)SampleNumber-1));
			 //System.out.print(it.next()+" ");
		 }
		 Esstimation *= (double)TNumber;
		 EsstiSample *= (double)SampleNumber;
		 EsstiSampleMinusOne *= ((double)SampleNumber-1);
		 double RelationRate = EsstiSample+2*Math.sqrt(EsstiSample*EsstiSampleMinusOne+EsstiSample-EsstiSample*EsstiSample);
		 Answer.add(Esstimation);
		 Answer.add(RelationRate);
		 return Answer;
	 }
	 
	 public static boolean [] SampleListToBoolean(String str){
		 String [] sample_str = str.split(", "); 
		 int sample_length = sample_str.length;
		 sample_str[0]=str.substring(1,sample_str[0].length());
		 sample_str[sample_length-1] = sample_str[sample_length-1].split("]")[0];
		 boolean [] tmp=new boolean [SampleNumber+2];
		 for(int i=0;i<sample_length;i++){
			 tmp[Integer.valueOf(sample_str[i])]=true;
		 }
		 return tmp;
	 }
	 
	 public static String [] SampleListToString(String str){
		 String [] sample_str = str.split(", "); 
		 int sample_length = sample_str.length;
		 sample_str[0]=str.substring(1,sample_str[0].length());
		 sample_str[sample_length-1] = sample_str[sample_length-1].split("]")[0];

		 return sample_str;
	 }
	 
	 public static ArrayList<Long> frequent_pattern(boolean [] first,boolean [] second){
		 ArrayList<Long> tmp_frequent = new ArrayList<Long>();
		 for(int i=0;i<SampleNumber;i++){
			 if(first[i]&&second[i]){Long li = new Long(i);tmp_frequent.add(li);}
		 }
		 return tmp_frequent;
	  }	 
	  

	  
	 public static ArrayList<Long> frequent_pattern(String first,String second){
		  ArrayList<Long> tmp_frequent = new ArrayList<Long>();
		  String [] f = SampleListToString(first);
		  String [] s = SampleListToString(second);
		  int i=0,j=0;

		  while(true){
			  
			if(i==f.length || j==s.length)break;
			if(f[i]==f[j]){tmp_frequent.add(Long.parseLong(f[i]));i++;j++;}
			else if	(Long.parseLong(f[i])<Long.parseLong(s[j])){i++;}
			else{j++;}
		  }
		  return tmp_frequent;
		  }
	 
	 
}







