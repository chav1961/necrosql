package chav1961.necrosql.interfaces;

public interface RowsCollection {
	int size();	
	int position();
	void position(int position);
	Object[] getRow();
	void updateRow(Object[] row);
	void deleteRow();	
	int insertRow();
}
