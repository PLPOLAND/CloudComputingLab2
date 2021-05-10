import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable{

	Text tag;
	IntWritable howMany;

	public StringAndInt() {
		tag = new Text();
		howMany = new IntWritable(0);
	}

	public StringAndInt(String tag, Integer howMany) {
		this.tag = new Text(tag);
		this.howMany = new IntWritable(howMany);
	}



	public StringAndInt(StringAndInt str) {
		this();
		this.setTag(str.getTag());
		this.setHowMany(str.getHowMany());
    }

    public String getTag() {
		return this.tag.toString();
	}

	public void setTag(String tag) {
		this.tag = new Text(tag);
	}

	public Integer getHowMany() {
		return this.howMany.get();
	}

	public void setHowMany(Integer howMany) {
		this.howMany = new IntWritable(howMany);
	}

	

	@Override
	public int compareTo(StringAndInt o) {
		return o.howMany.compareTo(this.howMany);
	}

	@Override
	public boolean equals(Object obj) {
		return ((StringAndInt) obj).getTag().equals(this.getTag());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tag.write(out);
		howMany.write(out);;
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		howMany.readFields(in);
	}
	
}