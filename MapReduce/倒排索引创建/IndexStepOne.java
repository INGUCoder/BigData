/**
第一步  先根据文件名统计出单词数量
*/
public class IndexStepOne{

	public static class IndexStepOneMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	
	
		@override
		public void map(LongWritable key,Text,value,Context context) throws IOException, InterruptedException{
		
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			//获取文件名
			String fileName = inputSplit.getPath().getName();
			//每一行单词以空格划分
			String[] words = value.toString().split(" ");
			for(String word:words){
			//统计格式  word(单词)+"-"+"文件名"
				context.write(new Text(word+"-"+fileName)，new IntWritable(1));
			
			}
		
		}
	
	
	}
	public static class IndexStepOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	
		@override
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
				
			int count = 0;
			for(IntWritable value:values){
			
				count += value.get();
			}
			
		context.write(key,new IntWritable(count));
		
		}
		
	
	
	
	}


	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException{
	
		Configuration configuration = new configuration();
		Job job = Job.getInstance(configuration);
		
		
		Job.setJarByClass(IndexStepOne.class);
		
		Job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		//设置reduceTask 数量
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
        FileInputFormat.setInputPaths(job,new Path("D://data//input"));
        FileOutputFormat.setOutputPath(job,new Path("D://data//out1"));

		
		job.waitForCompletion(true);
	
	
	}











}