public class StringAndInt implements Comparable<StringAndInt> {

	String tag;
	Integer howMany;

	StringAndInt() {
		tag = "";
		howMany = 0;
	}

	public StringAndInt(String tag, Integer howMany) {
		this.tag = tag;
		this.howMany = howMany;
	}


	public String getTag() {
		return this.tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public Integer getHowMany() {
		return this.howMany;
	}

	public void setHowMany(Integer howMany) {
		this.howMany = howMany;
	}

	

	@Override
	public int compareTo(StringAndInt o) {
		return o.howMany.compareTo(this.howMany);
	}
	
}