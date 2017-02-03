public static void startNPattern(Parameters params, Configuration conf)
    throws IOException, InterruptedException, ClassNotFoundException {
    conf.set(PFP_PARAMETERS, params.toString());
    
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    
    String input = "/user/root/output/result_0";
    Job job = new Job(conf, "Parallel Counting Driver running over input: " + input);
    job.setJarByClass(PFPGrowth.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(input));
    Path outPath = new Path("/user/root/output/result_1");
    FileOutputFormat.setOutputPath(job, outPath);
    
    HadoopUtil.delete(conf, outPath);
    
    job.setInputFormatClass(TextInputFormat.class);
    //job.setMapperClass(ParallelCountingMapper.class);
    //job.setCombinerClass(ParallelCountingReducer.class);
    //job.setReducerClass(ParallelCountingReducer.class);
    //job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new IllegalStateException("Job failed!");
    }
    
  }