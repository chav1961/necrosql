package chav1961.necrosql.sqlprocessing;

import java.math.BigDecimal;
import java.util.Arrays;

import chav1961.necrosql.exceptions.NecroSQLCalculationException;
import chav1961.necrosql.sqlprocessing.Parser.DataType;

class Calculation {
	static final char			CMD_NOP = 0x00;
	static final char			CMD_LDC = 0x01;
	static final char			CMD_LDI = 0x02;
	static final char			CMD_LDR = 0x03;
	static final char			CMD_LDL = 0x04;
	static final char			CMD_LDNAME = 0x05;
	static final char			CMD_CALL_F = 0x06;
	static final char			CMD_CALL_GF = 0x07;
	static final char			CMD_RET = 0x08;
	static final char			CMD_RETT = 0x09;
	static final char			CMD_RETF = 0x0A;
	static final char			CMD_RETX = 0x0B;
	static final char			CMD_RETTX = 0x0C;
	static final char			CMD_RETFX = 0x0D;
	static final char			CMD_JMP = 0x0E;
	static final char			CMD_JMPT = 0x0F;
	static final char			CMD_JMPF = 0x10;
	static final char			CMD_ADD = 0x11;
	static final char			CMD_SUB = 0x12;
	static final char			CMD_MUL = 0x13;
	static final char			CMD_DIV = 0x14;
	static final char			CMD_MOD = 0x15;
	static final char			CMD_CAT = 0x16;
	static final char			CMD_EQ = 0x17;
	static final char			CMD_NE = 0x18;
	static final char			CMD_GT = 0x19;
	static final char			CMD_GE = 0x1A;
	static final char			CMD_LT = 0x1B;
	static final char			CMD_LE = 0x1C;
	static final char			CMD_INLIST = 0x1D;
	static final char			CMD_INRANGE = 0x1E;
	static final char			CMD_LIKE = 0x1F;
	static final char			CMD_INSET = 0x20;
	static final char			CMD_EXISTS = 0x21;
	static final char			CMD_LDINDIRECT = 0x22;
	static final char			CMD_LDINDIRECTC = 0x23;
	static final char			CMD_LDINDIRECTI = 0x24;
	static final char			CMD_LDINDIRECTR = 0x25;
	static final char			CMD_LDINDIRECTD = 0x26;
	static final char			CMD_LDINDIRECTL = 0x27;
	static final char			CMD_LDINDIRECTM = 0x28;
	static final char			CMD_CONVERT = 0x29;
	static final char			CMD_NEG = 0x2A;
	static final char			CMD_NOT = 0x2B;
	static final char			CMD_AND = 0x2C;
	static final char			CMD_OR = 0x2D;
	
	static final Calculation	ALWAYS_TRUE = new Calculation();
	
	private static final int	CMD_FRAME = 64;
	private static final long	BEFORE_OVERFLOW = Long.MAX_VALUE/10; 
	
	static {
		ALWAYS_TRUE.add(CMD_LDL,'T');
		ALWAYS_TRUE.add(CMD_RET);
	}
	
	
	private StackRepo			stack = new StackRepo();

	private char[]				cmdList = new char[CMD_FRAME];
	private int					cmdIndex = 0;

	void add(final char cmd) {
		if (cmdIndex >= cmdList.length) {
			expandCmdList(1);
		}
		cmdList[cmdIndex++] = cmd;
	}
	

	void add(char cmd, char... parameters) {
		if (cmdIndex + parameters.length >= cmdList.length) {
			expandCmdList(1 + parameters.length);
		}
		cmdList[cmdIndex++] = cmd;
		System.arraycopy(parameters,0,cmdList,cmdIndex,parameters.length);
		cmdIndex += parameters.length; 
	}

	void add(char cmd, char[] data, int from, int to) {
		if (cmdIndex + (to - from) + 2 >= cmdList.length) {
			expandCmdList(to - from + 2);
		}
		cmdList[cmdIndex++] = cmd;
		cmdList[cmdIndex++] = (char)(to - from);
		System.arraycopy(data,0,cmdList,cmdIndex,to-from);
		cmdIndex += to - from; 
	}
	
	StackItem calculate(final char[] code) throws NecroSQLCalculationException {
		return calculate(code,0,stack);
	}

	private void expandCmdList(final int delta) {
		final int	add = Math.max(delta,CMD_FRAME);
		
		cmdList = Arrays.copyOf(cmdList,cmdList.length+add);
	}
	
	private static StackItem calculate(final char[] code, int from, final StackRepo stack) throws NecroSQLCalculationException {
		StackItem		item;
		DataType		preferred;
		
		for (;;) {
			switch (code[from]) {
				case CMD_NOP 		:	// NOP 
					from++; 
					break;
				case CMD_LDC		:	// LDC <length><constant_chars>
					if (stack.itemIndex == 0) {
						stack.expand();
					}
					item = stack.items[--stack.itemIndex];
					item.dataType = DataType.Char;
					item.charValue = code;
					item.fromChar = 0;
					from += (item.toChar = code[from+1]) + 2;
					break;
				case CMD_LDI		:	// LDI <length><constant_chars>
					long	result = 0;
					
					for (int index = 0, maxIndex = code[from+1]; index < maxIndex; index++) {
						if (result >= BEFORE_OVERFLOW) {
							throw new IllegalArgumentException();
						}
						else {
							result = 10 * result + code[from+index+2] - '0';
						}
					}
					if (stack.itemIndex == 0) {
						stack.expand();
					}
					item = stack.items[--stack.itemIndex];
					item.dataType = DataType.Int;
					item.intValue = result;
					from += code[from+1] + 2;
					break;
				case CMD_LDR		:	// LDR <length><constant_chars>
					if (stack.itemIndex == 0) {
						stack.expand();
					}
					item = stack.items[--stack.itemIndex];
					item.dataType = DataType.Real;
					item.realValue = new BigDecimal(code,0,code[from+1]);
					from += code[from+1] + 2;
					break;
				case CMD_LDL		:	// LDL {'T'|'F'}
					if (stack.itemIndex == 0) {
						stack.expand();
					}
					item = stack.items[--stack.itemIndex];
					item.dataType = DataType.Boolean;
					item.boolValue = code[from+1] == 'T';
					from += 2;
					break;
				case CMD_LDNAME		:	// LDNAME <length><name>	
					if (stack.itemIndex == 0) {
						stack.expand();
					}
					item = stack.items[--stack.itemIndex];
					item.dataType = DataType.Char;
					item.charValue = code;
					item.fromChar = 0;
					from += (item.toChar = code[from+1]) + 2;
					break;
				case CMD_RET		:	// RET
					return stack.items[stack.itemIndex];
				case CMD_ADD		:	// ADD
				case CMD_SUB		:	// SUB
				case CMD_MUL		:	// MUL
				case CMD_DIV		:	// DIV
				case CMD_MOD		:	// MOD
					preferred = preferredType(stack.items[stack.itemIndex].dataType,stack.items[stack.itemIndex+1].dataType,DataType.Int,DataType.Real);
					if (stack.items[stack.itemIndex].dataType != preferred) {
						convert(stack.items[stack.itemIndex],preferred);
					}
					if (stack.items[stack.itemIndex+1].dataType != preferred) {
						convert(stack.items[stack.itemIndex+1],preferred);
					}
					switch (preferred) {
						case Int :
							switch (code[from]) {
								case CMD_ADD	:
									stack.items[stack.itemIndex+1].intValue += stack.items[stack.itemIndex+1].intValue;
									break;
								case CMD_SUB	:
									stack.items[stack.itemIndex+1].intValue -= stack.items[stack.itemIndex].intValue;
									break;
								case CMD_MUL	:
									stack.items[stack.itemIndex+1].intValue *= stack.items[stack.itemIndex].intValue;
									break;
								case CMD_DIV	:
									stack.items[stack.itemIndex+1].intValue /= stack.items[stack.itemIndex].intValue;
									break;
								case CMD_MOD	:
									stack.items[stack.itemIndex+1].intValue %= stack.items[stack.itemIndex].intValue;
									break;
							}
							stack.itemIndex++;
							break;
						case Real :
							switch (code[from]) {
								case CMD_ADD	:
									stack.items[stack.itemIndex+1].realValue.add(stack.items[stack.itemIndex+1].realValue);
									break;
								case CMD_SUB	:
									stack.items[stack.itemIndex+1].realValue.subtract(stack.items[stack.itemIndex+1].realValue);
									break;
								case CMD_MUL	:
									stack.items[stack.itemIndex+1].realValue.multiply(stack.items[stack.itemIndex+1].realValue);
									break;
								case CMD_DIV	:
									stack.items[stack.itemIndex+1].realValue.divide(stack.items[stack.itemIndex+1].realValue);
									break;
								case CMD_MOD	:
									stack.items[stack.itemIndex+1].realValue.remainder(stack.items[stack.itemIndex+1].realValue);
									break;
							}
							stack.items[stack.itemIndex].realValue = null;
							stack.itemIndex++;
							break;
						default :
							throw new NecroSQLCalculationException("Conversion exception: arithmetic operations need integer or real data type");
					}
					from++;
					break;
				case CMD_CAT		:	// CAT <number_catenations>
					int			size = 0, concatCount = code[from+1];
					
					for (int index = 0; index < concatCount; index++) {
						item = stack.items[stack.itemIndex+index];
						
						if (item.dataType != DataType.Char) {
							convert(item,DataType.Char);
						}
						size += item.toChar - item.fromChar;
					}
					final char[]	concatenated = new char[size];
					int				displ = 0;
					
					for (int index = 0; index < concatCount; index++) {
						item = stack.items[stack.itemIndex+index];
						
						System.arraycopy(item.charValue,0,concatenated,displ,item.toChar-item.fromChar);
						item.charValue = null;
						displ += item.toChar - item.fromChar;
					}
					item = stack.items[stack.itemIndex+concatCount-1];
					item.charValue = concatenated;
					item.fromChar = 0;
					item.toChar = size;
					from += 2;
					break;
				case CMD_EQ			:	// EQ
				case CMD_NE			:	// NE
				case CMD_GT			:	// GT
				case CMD_GE			:	// GE
				case CMD_LT			:	// LT
				case CMD_LE			:	// LE
					preferred = preferredType(stack.items[stack.itemIndex+1].dataType,stack.items[stack.itemIndex+1].dataType,DataType.Int,DataType.Real,DataType.Char,DataType.Date,DataType.Boolean);
					if (stack.items[stack.itemIndex].dataType != preferred) {
						convert(stack.items[stack.itemIndex],preferred);
					}
					stack.items[stack.itemIndex+1].dataType = DataType.Boolean;
					switch (preferred) {
						case Int 		:
							switch (code[from]) {
								case CMD_EQ		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue == stack.items[stack.itemIndex].intValue;
									break;
								case CMD_NE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue != stack.items[stack.itemIndex].intValue;
									break;
								case CMD_GT		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue > stack.items[stack.itemIndex].intValue;
									break;
								case CMD_GE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue >= stack.items[stack.itemIndex].intValue;
									break;
								case CMD_LT		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue < stack.items[stack.itemIndex].intValue;
									break;
								case CMD_LE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue <= stack.items[stack.itemIndex].intValue;
									break;
								default :
									throw new NecroSQLCalculationException("Conversion exception: arithmetic operations need integer or real data type");
							}
							break;
						case Real 		:
							switch (code[from]) {
								case CMD_EQ		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].realValue.compareTo(stack.items[stack.itemIndex].realValue) == 0;
									break;
								case CMD_NE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].realValue.compareTo(stack.items[stack.itemIndex].realValue) != 0;
									break;
								case CMD_GT		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].realValue.compareTo(stack.items[stack.itemIndex].realValue) > 0;
									break;
								case CMD_GE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].realValue.compareTo(stack.items[stack.itemIndex].realValue) >= 0;
									break;
								case CMD_LT		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].realValue.compareTo(stack.items[stack.itemIndex].realValue) < 0;
									break;
								case CMD_LE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].realValue.compareTo(stack.items[stack.itemIndex].realValue) <= 0;
									break;
								default :
									throw new NecroSQLCalculationException("Conversion exception: arithmetic operations need integer or real data type");
							}
							break;
						case Char 		:
							break;
						case Date 		:
							switch (code[from]) {
								case CMD_EQ		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue == stack.items[stack.itemIndex].intValue;
									break;
								case CMD_NE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue != stack.items[stack.itemIndex].intValue;
									break;
								case CMD_GT		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue > stack.items[stack.itemIndex].intValue;
									break;
								case CMD_GE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue >= stack.items[stack.itemIndex].intValue;
									break;
								case CMD_LT		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue < stack.items[stack.itemIndex].intValue;
									break;
								case CMD_LE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].intValue <= stack.items[stack.itemIndex].intValue;
									break;
								default :
									throw new NecroSQLCalculationException("Conversion exception: arithmetic operations need integer or real data type");
							}
							break;
						case Boolean	:
							switch (code[from]) {
								case CMD_EQ		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].boolValue == stack.items[stack.itemIndex].boolValue;
									break;
								case CMD_NE		:
									stack.items[stack.itemIndex+1].boolValue = stack.items[stack.itemIndex+1].boolValue != stack.items[stack.itemIndex].boolValue;
									break;
								case CMD_GT		:
								case CMD_GE		:
								case CMD_LT		:
								case CMD_LE		:
									throw new NecroSQLCalculationException("Comparison operator for booleans can be '=' and '<>' only!");
								default :
									throw new NecroSQLCalculationException("Conversion exception: arithmetic operations need integer or real data type");
							}
							break;
						default :
							throw new NecroSQLCalculationException("Conversion exception: comparison operations can't be executed for ["+preferred+"] data type");
					}
					stack.itemIndex++;
					from++;
					break;
				case CMD_INLIST		:	// INLIST <list_element_count>
					int			listCount = code[from+1];
					boolean		resultFound = false;
					
					item = stack.items[stack.itemIndex+listCount]; 
					preferred = preferredType(item.dataType,item.dataType,DataType.Int,DataType.Real,DataType.Char,DataType.Date);
					item.dataType = DataType.Boolean;
					item.boolValue = false;
					
					for (int index = 0; index < listCount; index++) {
						if (!resultFound) {
							if (stack.items[stack.itemIndex+index].dataType != preferred) {
								convert(stack.items[stack.itemIndex+index],preferred);
							}
							switch (preferred) {
								case Int 		:
									if (item.intValue == stack.items[stack.itemIndex+index].intValue) {
										item.boolValue = true;
										resultFound = true;
									}
									break;
								case Real 		:
									if (item.realValue.equals(stack.items[stack.itemIndex+index].realValue)) {
										item.boolValue = true;
										resultFound = true;
									}
									break;
								case Char 		:
									break;
								case Date 		:
									if (item.intValue == stack.items[stack.itemIndex+index].intValue) {
										item.boolValue = true;
										resultFound = true;
									}
									break;
								default :
									throw new NecroSQLCalculationException("Conversion exception: comparison operations can't be executed for ["+preferred+"] data type");
							}
						}
						stack.items[stack.itemIndex+index].realValue = null;
						stack.items[stack.itemIndex+index].charValue = null;
					}
					stack.itemIndex += listCount;
					item.realValue = null;
					item.charValue = null;
					from += 2;
					break;
				case CMD_INRANGE	:	// INRANGE
					item = stack.items[stack.itemIndex+2]; 
					preferred = preferredType(item.dataType,item.dataType,DataType.Int,DataType.Real,DataType.Char,DataType.Date);
					item.dataType = DataType.Boolean;
					
					if (stack.items[stack.itemIndex].dataType != preferred) {
						convert(stack.items[stack.itemIndex],preferred);
					}
					if (stack.items[stack.itemIndex+1].dataType != preferred) {
						convert(stack.items[stack.itemIndex+1],preferred);
					}
					switch (preferred) {
						case Int 		:
							item.boolValue = item.intValue >= stack.items[stack.itemIndex+1].intValue && item.intValue <= stack.items[stack.itemIndex].intValue; 
							break;
						case Real 		:
							item.boolValue = item.realValue.compareTo(stack.items[stack.itemIndex+1].realValue) >= 0 && item.realValue.compareTo(stack.items[stack.itemIndex].realValue) <= 0; 
							break;
						case Char 		:
							break;
						case Date 		:
							item.boolValue = item.intValue >= stack.items[stack.itemIndex+1].intValue && item.intValue <= stack.items[stack.itemIndex+1].intValue; 
							break;
						default :
							throw new NecroSQLCalculationException("Conversion exception: comparison operations can't be executed for ["+preferred+"] data type");
					}
					stack.items[stack.itemIndex].realValue = null;
					stack.items[stack.itemIndex].charValue = null;
					stack.items[stack.itemIndex+1].realValue = null;
					stack.items[stack.itemIndex+1].charValue = null;
					item.realValue = null;
					item.charValue = null;
					stack.itemIndex += 2;
					from++;
					break;
				case CMD_LIKE		:	// LIKE
					item = stack.items[stack.itemIndex+1]; 
					if (stack.items[stack.itemIndex].dataType != DataType.Char) {
						convert(stack.items[stack.itemIndex],DataType.Char);
					}
					if (stack.items[stack.itemIndex+1].dataType != DataType.Char) {
						convert(stack.items[stack.itemIndex+1],DataType.Char);
					}
					item.dataType = DataType.Boolean;
					
					stack.items[stack.itemIndex].realValue = null;
					stack.items[stack.itemIndex].charValue = null;
					item.realValue = null;
					item.charValue = null;
					stack.itemIndex++;
					from++;
					break;
				case CMD_NEG		:	// NEG
					item = stack.items[stack.itemIndex+1];
					switch (item.dataType) {
						case Int 	:
							item.intValue = -item.intValue;
							break;
						case Real	:
							item.realValue.negate();
							break;
						default :
							throw new NecroSQLCalculationException("Negation exception: operations can't be executed for ["+item.dataType+"] data type");
					}
					from++;
					break;
				case CMD_NOT		:	// NOT
					item = stack.items[stack.itemIndex+1];
					switch (item.dataType) {
						case Boolean 	:
							item.boolValue = !item.boolValue;
							break;
						default :
							throw new NecroSQLCalculationException("Not exception: operations can't be executed for ["+item.dataType+"] data type");
					}
					from++;
					break;
				case CMD_AND		:	// AND
					if (stack.items[stack.itemIndex].dataType != DataType.Char) {
						convert(stack.items[stack.itemIndex],DataType.Char);
					}
					if (stack.items[stack.itemIndex+1].dataType != DataType.Char) {
						convert(stack.items[stack.itemIndex+1],DataType.Char);
					}
					stack.items[stack.itemIndex+1].boolValue &= stack.items[stack.itemIndex].boolValue; 
					stack.itemIndex++;
					from++;
					break;
				case CMD_OR			:
					if (stack.items[stack.itemIndex].dataType != DataType.Char) {
						convert(stack.items[stack.itemIndex],DataType.Char);
					}
					if (stack.items[stack.itemIndex+1].dataType != DataType.Char) {
						convert(stack.items[stack.itemIndex+1],DataType.Char);
					}
					stack.items[stack.itemIndex+1].boolValue |= stack.items[stack.itemIndex].boolValue; 
					stack.itemIndex++;
					from++;
					break;
				case CMD_CALL_F		:
				case CMD_CALL_GF	:
				case CMD_RETT		:
				case CMD_RETF		:
				case CMD_RETX		:
				case CMD_RETTX		:
				case CMD_RETFX		:
				case CMD_JMP		:
				case CMD_JMPT		:
				case CMD_JMPF		:
				case CMD_EXISTS		:
				case CMD_LDINDIRECT	:
				case CMD_LDINDIRECTC:
				case CMD_LDINDIRECTI:
				case CMD_LDINDIRECTR:
				case CMD_LDINDIRECTD:
				case CMD_LDINDIRECTL:
				case CMD_LDINDIRECTM:
				case CMD_CONVERT	:
				case CMD_INSET		:
				default : throw new UnsupportedOperationException("P-Code ["+code[from]+"] is ot supported yet");
			}
		}
	}
	
	private static DataType preferredType(final DataType type1, final DataType type2, final DataType... alllowed) {
		return DataType.Char;
	}

	private static void convert(final StackItem item, final DataType awaitedType) {
		
	}
	
	
	class StackRepo {
		private static final int	INITIAL_FRAME = 16; 
		
		volatile boolean		inUse;
		volatile long 			lastUsed;
		StackItem[]				items = new StackItem[0];
		int						itemIndex = 0;
		
		void expand() {
			final StackItem[]	newItems = new StackItem[items.length+INITIAL_FRAME];
			
			System.arraycopy(items,0,newItems,INITIAL_FRAME,items.length);
			for (int index = 0; index < INITIAL_FRAME; index++){
				newItems[index] = new StackItem();
			}
			itemIndex = INITIAL_FRAME;
		}
	}
	
	class StackItem {
		DataType			dataType;
		long				intValue;
		boolean				boolValue;
		char[]				charValue;
		int					fromChar, toChar;
		BigDecimal			realValue;
	}
}
