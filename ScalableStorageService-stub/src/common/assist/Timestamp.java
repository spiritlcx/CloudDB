package common.assist;

public class Timestamp{
	private int [] vector;
	public Timestamp(int n){
		vector = new int[n];
	}
	
	public Timestamp(){
		
	}
	
	public Timestamp(Timestamp timestamp){
		vector = new int[timestamp.length()];
		int [] othervector = timestamp.getVector();
		for(int i = 0; i < vector.length; i++){
			vector[i] = othervector[i];
		}
	}
	public void setVector(int [] vector){
		this.vector = vector;
	}
	public void merge(Timestamp timestamp){
		if(vector.length != timestamp.length())
			return;
		int [] othervector = timestamp.getVector();
		for(int i = 0; i < timestamp.length(); i++){
			if(vector[i] < othervector[i])
				vector[i] = othervector[i];
		}
	}
	
	public void update(int i){
		if(i >= vector.length)
			return;
		vector[i]++;
	}
	
	public void update(Timestamp timestamp, int i){
		if(vector.length != timestamp.length())
			return;
		int [] othervector = timestamp.getVector();
		vector[i] = othervector[i];
	}
	
	public int compare(Timestamp timestamp){
		int [] othervector = timestamp.getVector();
		if(vector.length != othervector.length)
			return 0;
		boolean smaller = false;
		boolean bigger = false;
		boolean equal = false;
		for(int i = 0; i < vector.length; i++){
			if(vector[i] < othervector[i]){
				smaller = true;
			}else if(vector[i] > othervector[i]){
				bigger = true;
			}else{
				equal = true;
			}
		}

		if((smaller || equal) && !bigger){
			return -1;
		}else if(bigger && !smaller){
			return 1;
		}else{
			return 0;
		}
	}
	
	public int length(){
		return vector.length;
	}
	public int [] getVector(){
		return vector;
	}
	
	@Override
	public String toString(){
		String s = "";
		for(int v : vector){
			s += (v + " ");
		}
		return s;
	}
	
	public static Timestamp deserialize(String s){
		String [] elements = s.split(" ");
		int [] newvector = new int[elements.length];
		for(int i = 0; i < elements.length; i++){
			newvector[i] = Integer.parseInt(elements[i]);
		}
		Timestamp timestamp = new Timestamp();
		timestamp.setVector(newvector);
		return timestamp;
	}
}