import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
		public NaturalKeyGroupingComparator() {
			super(StringAndInt2.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			StringAndInt2 a = (StringAndInt2) w1;
			StringAndInt2 b = (StringAndInt2) w2;
			return a.getTag().compareTo(b.getTag());
		}
	}