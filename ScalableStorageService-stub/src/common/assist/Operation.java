package common.assist;

public class Operation{
	public String key;
	public String value;
	public Timestamp prev;
	public String sequence;
	
	@Override
	public String toString(){
		String s = "";
		s += ("key:" + key + ",value:" + value);
		return s;
	}
	
	public static Operation deserialize(String s){
		Operation operation = new Operation();
		String [] pairs = s.split(",");
		for(String pair : pairs){
			String [] element = pair.split(":");
			switch(element[0]){
			case "key":
				operation.key = element[1];
			case "value":
				operation.value = element[1];
			}
		}
		return operation;
	}	
}
