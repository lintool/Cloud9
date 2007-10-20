package edu.umd.cloud9.tuple;

public class SchemaTest {

	public static final Schema MY_SCHEMA = new Schema();
	static {
		MY_SCHEMA.addColumn("p1", String.class, "default");
		MY_SCHEMA.addColumn("p2", Integer.class, new Integer(1));
	}
	
	public static void main(String[] args) {
		Tuple tuple = MY_SCHEMA.instantiate();
		
		System.out.println(tuple.get(0) + ", " + tuple.get(1));
		System.out.println(tuple.get("p1") + ", " + tuple.get("p2"));
	}

}
