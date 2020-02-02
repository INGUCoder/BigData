package order.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {

    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        //按照订单中的orderid来分发数据
        return (orderBean.getOrderId().hashCode()&Integer.MAX_VALUE)%i;
    }
}
