import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<StringAndInt2, String>{
		@Override
		public int getPartition(StringAndInt2 key, String val, int numPartitions) {
			int hash = key.getTag().hashCode();
			int partition = hash%numPartitions;
			return partition;
		}
	}