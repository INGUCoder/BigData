package order.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class OrderTopn{


	public static class OrderTopnMapper extends Mapper<LongWritable,Text,Text,OrderBean>{
		//防止 orderBean 被实例化多次    在Mapper 前创建一次就行了 因为orderBean 会被序列化  所以不需要担心对象被覆盖问题
		OrderBean orderBean = new OrderBean();
		
		Text k = new Text();
		
		@Override 
		protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		
			String[] fields = value.toString().split(",");
			k.set(fields[0]);
			//从这里交给maptask的 k -v 对象  会被maptask序列化分组  所以不用担心被覆盖问题
			context.write(k,orderBean);
		
		}
		
		public static class OrderTopnReducer extends Reducer<Text,OrderBean,OrderBean,NullWritable>{
		
		@Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
			//获取topn参数
			int topn = context.getConfiguration().getInt("order.top.n",3);;
			ArrayList<OrderBean> beanArrayList = new ArrayList<>();
			//reduceTask 提供的values 迭代器 每次迭代返回给我们的都是同一个对象  只是设置了不同值
			for(OrderBean orderBean:values){
				//构造一个新对象 来存储本次的迭代出来的值
				OrderBean newBean = new OrderBean();
				newBean.set(orderBean.getOrderId(),newBean.getUserId(),newBean.getProductName(),newBean.getPrice(),newBean.getNum());
				beanArrayList.add(orderBean);
			}
			Collections.sort(orderBean);
			for(int i=0;i<topn;i++){
				context.write(beanArrayList.get[i],NullWritable.get());
			}
		}
	
	}
	public static void main(String[] args){
		onfiguration conf = new Configuration(); // 默认只加载core-default.xml core-site.xml
        conf.setInt("order.top.n", 2);

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopn.class);

        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D://data//input//topn"));
        FileOutputFormat.setOutputPath(job, new Path("D://data//out3"));

        job.waitForCompletion(true);
	
	}




}