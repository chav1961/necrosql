package chav1961.necrosql;

import chav1961.necrosql.interfaces.RowsCollection;

class PredefinedRowsCollection implements RowsCollection {
	private final boolean	readOnly;
	private Object[][]		content;
	private int				size, length;
	private int				position;
	
	PredefinedRowsCollection(final boolean readOnly) {
		this.content = new Object[0][0];
		this.readOnly = readOnly;
		this.size = 0;
		this.length = 0;
		this.position = -1;
	}
	
	PredefinedRowsCollection(final Object[][] content, final boolean readOnly) {
		if (content == null || content.length == 0) {
			throw new IllegalArgumentException("Content can't be null or empty!");
		}
		else {
			this.content = content;
			this.readOnly = readOnly;
			this.size = content.length;
			this.length = content[0].length;
			this.position = -1;
			
			for (int index = 0; index < size; index++) {
				if (content[index] == null) {
					throw new IllegalArgumentException("Content contains null row at line ["+index+"]! Null rows are not supported");
				}
				else if (content[index].length != length) {
					throw new IllegalArgumentException("Content row at line ["+index+"] has different size ["+content[index].length+"] than awaited ["+length+"]! All rows in the content need have identical size");
				}
				else {
					for (int cell = 0; cell < length; cell++) {
						if (content[index][cell] == null) {
							throw new IllegalArgumentException("Content row at line ["+index+"] has null cell at location ["+cell+"]! Null values for the content are not supported");
						}
					}
				}
			}
		}
	}
	
	@Override
	public int size() {
		return size;
	}
	
	@Override
	public int position() {
		return position;
	}
	
	@Override
	public void position(int position) {
		if (position < 0 || position >= size) {
			if (size == 0) {
				throw new IllegalArgumentException("Can't set position for empty collection!");
			}
			else {
				throw new IllegalArgumentException("Position ["+position+"] outside the range 0.."+size());
			}
		}
		else {
			this.position = position;
		}
	}
	
	@Override
	public Object[] getRow() {
		if (position < 0 || position >= size) {
			throw new IllegalStateException("Position ["+position+"] outside the range 0.."+size()+". Call position(int) before");
		}
		else {
			return content[position].clone();
		}
	}
	
	@Override
	public void updateRow(final Object[] row) {
		if (row == null) {
			throw new IllegalArgumentException("Row can't be null!");
		}
		else if (readOnly) {
			throw new IllegalStateException("This set is marked read-only. Operation rejected");
		}
		else if (position < 0 || position >= size) {
			throw new IllegalStateException("Position ["+position+"] outside the range 0.."+size()+". Call position(int) before");
		}
		else if (row.length != length) {
			throw new IllegalArgumentException("Row length ["+row.length+"] differ than content length ["+length+"]!");
		}
		else {
			for (int index = 0; index < row.length; index++) {
				if (row[index] == null) {
					throw new IllegalArgumentException("Row has nulls at location ["+index+"]! Null values are not supported");
				}
			}
			System.arraycopy(row,0,content[position],0,row.length);
		}
	}
	
	@Override
	public void deleteRow() {
		if (position < 0 || position >= size) {
			throw new IllegalStateException("Position ["+position+"] outside the range 0.."+size()+". Call position(int) before");
		}
		else if (readOnly) {
			throw new IllegalStateException("This set is marked read-only. Operation rejected");
		}
		else if (size == 1) {
			content = new Object[0][0];
			size = 0;
			position = -1;
		}
		else {
			final Object[][]	newContent = new Object[content.length-1][length];

			System.arraycopy(content,0,newContent,0,position);
			System.arraycopy(content,position+1,newContent,position,size-position-1);
			content = newContent;
			if (position >= --size) {
				position = size-1;
			}
		}
	}
	
	@Override
	public int insertRow() {
		if (position < 0 || position >= size) {
			throw new IllegalStateException("Position ["+position+"] outside the range 0.."+size()+". Call position(int) before");
		}
		else if (readOnly) {
			throw new IllegalStateException("This set is marked read-only. Operation rejected");
		}
		else {
			final Object[][]	newContent = new Object[content.length+1][length];

			System.arraycopy(content,0,newContent,0,size);
			for (int index = 0; index < length; index++) {
				newContent[size][index] = new Object();
			}
			content = newContent;
			return size++;
		}
	}
}
