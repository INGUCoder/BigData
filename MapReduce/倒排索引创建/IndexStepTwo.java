/**
第二步  根据第一步的结果进行分类
*/
public class IndexStepTwo{

	public static class IndexStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	
	
		@override
		public void map(LongWritable key,Text,value,Context context) throws IOException, InterruptedException{
		
			String[] split = value.toString().split("-");
			context.write(new Text(split[0]),new Text(split[1].replaceAll("\t","-->")));
			
			}
		
		}
	
	
	}
	public static class IndexStepTwoReducer extends Reducer<Text,Text,Text,Text>{
	
	
		@override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuilder stringBuilder = new StringBuilder();
			for(Text value:values){
				stringBuilder.append(value.toString()).append("\t");
			}
			context.write(key,new Text((stringBuilder.toString())));
		}
		
	
	
	
	}


	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException{
	
		Configuration configuration = new configuration();
		Job job = Job.getInstance(configuration);
		
		
		Job.setJarByClass(IndexStepTwo.class);
		
		Job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);
		
		//设置reduceTask 数量
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
        FileInputFormat.setInputPaths(job,new Path("D://data//out1"));
        FileOutputFormat.setOutputPath(job,new Path("D://data//out2"));

		
		job.waitForCompletion(true);
	
	
	}











}