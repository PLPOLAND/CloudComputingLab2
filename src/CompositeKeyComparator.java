import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
		
		public CompositeKeyComparator() {
			super(StringAndInt2.class, true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringAndInt2 aa = (StringAndInt2) a;
			StringAndInt2 bb = (StringAndInt2) b;
			int result = aa.compareTo(bb);
			if(result == 0) {
				result = -1*aa.getHowMany().compareTo(bb.getHowMany());
			}
			return result;
		}
//		public int compare(StringAndInt2 a, StringAndInt2 b) {
//			int result = a.compareTo(b);
//			if(result == 0) {
//				result = -1*a.getHowMany().compareTo(b.getHowMany());
//			}
//			return result;
//		}
	}