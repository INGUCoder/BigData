package order.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean>{

	private String orderBean;
	private String userId;
	private String productName;
	private float price;
	private int num;
	private float amountPrice;
	public void set(String orderId, String userId, String productName, float price, int num) {
        this.orderId = orderId;
        this.userId = userId;
        this.productName = productName;
        this.price = price;
        this.num = num;
        this.amountPrice = price * num;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public float getAmountPrice() {
        return amountPrice;
    }

    public void setAmountPrice(float amountPrice) {
        this.amountPrice = amountPrice;
    }

    @Override
    public String toString() {
        return this.orderId + "," + this.userId + "," + this.productName + "," + this.price + "," + this.num + ","
                + this.amountPrice;
    }
	
	//比较规则 先比较总金额 在比商品名称 最后比较总价格
	public int CompareTo(OrderBean o){
			
		return FloatBean.compare(o.getAmountPrice,this.amountPrice)==0?this.productName.compareTo(o.getProductName()):Float.compare(o.
		getAmountPrice,this.getAmountPrice);
	
	}
	
	public void write(DataOutput dataOutput) throws IOException{
	
		dataOutput.writeUTF(this.orderId);
		dataOutput.writeUTF(this.userId);
		dataOutput.writeUTF(this.productName);
		dataOutput.writeFloat(this.price);
		dataOutput.writeInt(this.num);
	
	}
	
	public void readFields(DataInput dataOutput){
	
		this.orderId = dataInput.readUTF();
		this.userId = dataInput.readUTF();
		this.productName = dataInput.readUTF();
		this.price = dataInput.readFloat();
		this.num = dataInput.readInt();
		this.amountPrice = this.price * this.num;
	
	}
	
}