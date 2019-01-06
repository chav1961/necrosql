package chav1961.necrosql.sqlprocessing;


import java.util.Arrays;

import chav1961.necrosql.exceptions.NecroSQLSyntaxException;

public class Parser {
	static final LexDesc	LEX_OPEN = new LexDesc(LexType.Open);
	static final LexDesc	LEX_CLOSE = new LexDesc(LexType.Close);
	static final LexDesc	LEX_DOT = new LexDesc(LexType.Dot);
	static final LexDesc	LEX_LIST = new LexDesc(LexType.List);
	static final LexDesc	LEX_NAME = new LexDesc(LexType.Name);
	static final LexDesc	LEX_EOF = new LexDesc(LexType.EOF);
	static final LexDesc	LEX_OPER_ADD = new OperDesc(OperPrty.Add,Operation.Add);
	static final LexDesc	LEX_OPER_SUB = new OperDesc(OperPrty.Add,Operation.Sub);
	static final LexDesc	LEX_OPER_MUL = new OperDesc(OperPrty.Mul,Operation.Mul);
	static final LexDesc	LEX_OPER_DIV = new OperDesc(OperPrty.Mul,Operation.Div);
	static final LexDesc	LEX_OPER_MOD = new OperDesc(OperPrty.Mul,Operation.Mod);
	static final LexDesc	LEX_OPER_CAT = new OperDesc(OperPrty.Cat,Operation.Cat);
	static final LexDesc	LEX_OPER_EQ = new OperDesc(OperPrty.Cmp,Operation.EQ);
	static final LexDesc	LEX_OPER_NE = new OperDesc(OperPrty.Cmp,Operation.NE);
	static final LexDesc	LEX_OPER_GT = new OperDesc(OperPrty.Cmp,Operation.GT);
	static final LexDesc	LEX_OPER_GE = new OperDesc(OperPrty.Cmp,Operation.GE);
	static final LexDesc	LEX_OPER_LT = new OperDesc(OperPrty.Cmp,Operation.LT);
	static final LexDesc	LEX_OPER_LE = new OperDesc(OperPrty.Cmp,Operation.LE);
	static final LexDesc	LEX_CONST_INT = new ConstDesc(ConstType.Int);
	static final LexDesc	LEX_CONST_REAL = new ConstDesc(ConstType.Real);
	static final LexDesc	LEX_CONST_CHAR = new ConstDesc(ConstType.String);

	private static final Decode[]	WORDS = new Decode[]{
										 new Decode("ADD",new LexDesc(LexType.Add))
										,new Decode("ALL",new LexDesc(LexType.All))
										,new Decode("ALTER",new LexDesc(LexType.Alter))
										,new Decode("AND",new OperDesc(OperPrty.And,Operation.And))
										,new Decode("AS",new LexDesc(LexType.As))
										,new Decode("AVG",new FuncDesc(FunctionId.Avg,true))
										,new Decode("BETWEEN",new OperDesc(OperPrty.Cmp,Operation.Range))
										,new Decode("BLOB",new TypeDesc(DataType.Blob,false,false,0,0))
										,new Decode("BOOLEAN",new TypeDesc(DataType.Boolean,false,false,0,0))
										,new Decode("BY",new LexDesc(LexType.By))
										,new Decode("CHAR",new TypeDesc(DataType.Char,true,false,256,0))
										,new Decode("CHARACTER",new TypeDesc(DataType.Char,true,false,256,0))
										,new Decode("COUNT",new FuncDesc(FunctionId.Count,true))
										,new Decode("CREATE",new LexDesc(LexType.Create))
										,new Decode("DATE",new TypeDesc(DataType.Date,false,false,0,0))
										,new Decode("DELETE",new LexDesc(LexType.Delete))
										,new Decode("DISTINCT",new LexDesc(LexType.Distinct))
										,new Decode("DROP",new LexDesc(LexType.Drop))
										,new Decode("EXISTS",new FuncDesc(FunctionId.Exists,false))
										,new Decode("FALSE",new ConstDesc(ConstType.Boolean,false))
										,new Decode("FROM",new LexDesc(LexType.From))
										,new Decode("GROUP",new LexDesc(LexType.Group))
										,new Decode("HAVING",new LexDesc(LexType.Having))
										,new Decode("IN",new OperDesc(OperPrty.Cmp,Operation.In))
										,new Decode("INSERT",new LexDesc(LexType.Insert))
										,new Decode("INTEGER",new TypeDesc(DataType.Int,true,false,15,0))
										,new Decode("INTO",new LexDesc(LexType.Into))
										,new Decode("LIKE",new OperDesc(OperPrty.Cmp,Operation.Like))
										,new Decode("LIMIT",new LexDesc(LexType.Limit))
										,new Decode("MAX",new FuncDesc(FunctionId.Max,true))
										,new Decode("MIN",new FuncDesc(FunctionId.Min,true))
										,new Decode("MODIFY",new LexDesc(LexType.Modify))
										,new Decode("NOT",new OperDesc(OperPrty.Not,Operation.Not))
										,new Decode("NUMERIC",new TypeDesc(DataType.Real,true,true,40,10))
										,new Decode("OFFSET",new LexDesc(LexType.Offset))
										,new Decode("OR",new OperDesc(OperPrty.Or,Operation.Or))
										,new Decode("ORDER",new LexDesc(LexType.Order))
										,new Decode("SELECT",new LexDesc(LexType.Select))
										,new Decode("SET",new LexDesc(LexType.Set))
										,new Decode("SUM",new FuncDesc(FunctionId.Sum,true))
										,new Decode("TABLE",new LexDesc(LexType.Table))
										,new Decode("TOP",new LexDesc(LexType.Top))
										,new Decode("TRUE",new ConstDesc(ConstType.Boolean,true))
										,new Decode("UNION",new LexDesc(LexType.Union))
										,new Decode("UPDATE",new LexDesc(LexType.Update))
										,new Decode("VALUES",new LexDesc(LexType.Values))
										,new Decode("WHERE",new LexDesc(LexType.Where))
									};
	
	private static final Action[]	CREATE_TABLE = new Action[]{
										 new AsIsAction(LexType.Create)
										,new AsIsAction(LexType.Table)
										,new ExtractAction(LexType.Name, CreateTableRepo.TABLE_NAME)
										,new AsIsAction(LexType.Open)
										,new LoopAction(LexType.List,
												 new ExtractAction(LexType.Name, CreateTableRepo.FIELD_NAME)
												,new ChoiseAction(
														new OptionalAction(LexType.DataType,
																 new ExtractAction(LexType.DataType,CreateTableRepo.FIELD_TYPE)
																,new OptionalAction(LexType.Open,
																		 new AsIsAction(LexType.Open)
																		,new ExtractAction(LexType.Const,CreateTableRepo.FIELD_SIZE)
																		,new OptionalAction(LexType.List,
																				  new AsIsAction(LexType.List)
																				 ,new ExtractAction(LexType.Const,CreateTableRepo.FIELD_PRECISION)
																		)
																		,new AsIsAction(LexType.Close)
																)
														)
												)
										) 
									    ,new AsIsAction(LexType.Close)
									};
	private static final Action[]	DROP_TABLE = new Action[]{
										 new AsIsAction(LexType.Drop)
										,new AsIsAction(LexType.Table)
										,new ExtractAction(LexType.Name, DropTableRepo.TABLE_NAME)
									};	
	private static final Action[]	DELETE_TABLE = new Action[]{
										 new AsIsAction(LexType.Delete)
										,new AsIsAction(LexType.Table)
										,new ExtractAction(LexType.Name, DeleteTableRepo.TABLE_NAME)
										,new OptionalAction(LexType.Where,
												 new AsIsAction(LexType.Where)
												,new ExpressionAction(OperPrty.Or,DeleteTableRepo.WHERE)
										)
									};	
	private static final Action[]	UPDATE_TABLE = new Action[]{
										 new AsIsAction(LexType.Update)
										,new ExtractAction(LexType.Name, UpdateTableRepo.TABLE_NAME)
										,new AsIsAction(LexType.Set)
										,new LoopAction(LexType.List,
												 new ExtractAction(LexType.Name, UpdateTableRepo.FIELD_NAME)
										 		,new AsIsAction(LexType.Oper)
												,new ExpressionAction(OperPrty.Or,UpdateTableRepo.FIELD_VALUE)
										)
										,new OptionalAction(LexType.Where,
												 new AsIsAction(LexType.Where)
												,new ExpressionAction(OperPrty.Or,UpdateTableRepo.WHERE)
										)
									};	
	private static final Action[]	INSERT_TABLE = new Action[]{
										 new AsIsAction(LexType.Insert)
										,new AsIsAction(LexType.Into)
										,new ExtractAction(LexType.Name, InsertTableRepo.TABLE_NAME)
										,new AsIsAction(LexType.Open)
										,new LoopAction(LexType.List,
												 new ExtractAction(LexType.Name, InsertTableRepo.FIELD_NAME)
										)
										,new AsIsAction(LexType.Close)
										,new AsIsAction(LexType.Values)
										,new AsIsAction(LexType.Open)
										,new LoopAction(LexType.List,
												 new ExpressionAction(OperPrty.Term,InsertTableRepo.FIELD_VALUE)
										)
										,new AsIsAction(LexType.Close)
									};	
	
	enum LexType {
		Name, Const, Function, DataType,
		Oper, Open, Close, Dot, List,
		Add, All, Alter, As, By, Create, Delete, Distinct, Drop,
		From, Group, Having, Insert, Into, Limit, Modify,
		Offset, Order, Select, Set, Table, Top, Union, Update,
		Values, Where,
		EOF
	}

	enum DataType {
		Char, Int, Real, Date, Boolean, Blob
	}
	
	private enum ConstType {
		String, Int, Real, Boolean
	}
	
	private enum OperPrty {
		Term, Unary, Mul, Add, Cat, Cmp, Not, And, Or
	}

	private enum Operation {
		Add, Sub, Mul, Div, Mod, Cat, EQ, NE, LT, LE, GT, GE, In, Range, Like, Not, And, Or
	}

	private enum FunctionId {
		Avg, Count, Exists, Min, Max, Sum
	}

	private enum ActionType {
		asIs, extract, option, choise, loop, expression
	}
	
	
	private LexDesc		lex;	
	private long		longValue;
	private int[]		charValue = new int[2];
	private Calculation calc;
	
	public Parser(){
		
	}

	LexDesc getLexema() {
		return lex; 
	}
	
	int next(final char[] source, int from) throws NecroSQLSyntaxException {
		final int	maxLen = source.length;
		
		while (from < maxLen && source[from] <= ' ') {
			from++;
		}
		if (from < maxLen) {
			switch (source[from]) {
				case '(' : lex = LEX_OPEN; return from + 1;
				case ')' : lex = LEX_CLOSE; return from + 1;
				case '.' : lex = LEX_DOT; return from + 1;
				case ',' : lex = LEX_LIST; return from + 1;
				case '+' : lex = LEX_OPER_ADD; return from + 1;  
				case '-' : lex = LEX_OPER_SUB; return from + 1;
				case '*' : lex = LEX_OPER_MUL; return from + 1;
				case '/' : lex = LEX_OPER_DIV; return from + 1;
				case '%' : lex = LEX_OPER_MOD; return from + 1;
				case '=' : lex = LEX_OPER_EQ; return from + 1;
				case '>' :
					if (from < maxLen - 1 && source[from+1] == '=') {
						lex = LEX_OPER_GE; return from + 2;
					}
					else {
						lex = LEX_OPER_GT; return from + 1;
					}
				case '<' :
					if (from < maxLen - 1 && source[from+1] == '=') {
						lex = LEX_OPER_LE; return from + 2;
					}
					else if (from < maxLen - 1 && source[from+1] == '>') {
						lex = LEX_OPER_NE; return from + 2;
					}
					else {
						lex = LEX_OPER_LT; return from + 1;
					}
				case '|' :
					if (from < maxLen - 1 && source[from+1] == '|') {
						lex = LEX_OPER_CAT; return from + 2;
					}
					else {
						throw new NecroSQLSyntaxException("Position ["+from+"]: unknonw operation (possibly || ?)");
					}
				case '0' :	case '1' :	case '2' :	case '3' :	case '4' :
				case '5' :	case '6' :	case '7' :	case '8' :	case '9' :
					final int	start = from;
					long		result = 0;
					
					while (from < maxLen && source[from] >= '0' && source[from] <= '9') {
						result = 10 * result + source[from] - '0';
						from++;
					}
					if (from < maxLen && source[from] == '.') {
						from++;
						while (from < maxLen && source[from] >= '0' && source[from] <= '9') {
							from++;
						}
						charValue[0] = start;	charValue[1] = from; 
						lex = LEX_CONST_REAL; return from;
					}
					else {
						longValue = result; 
						charValue[0] = start;	charValue[1] = from; 
						lex = LEX_CONST_INT; return from;
					}
				case '\'':
					final int	startConst = ++from;
					
					while (from < maxLen && source[from] != '\'') {
						from++;
					}
					if (from < maxLen) {
						charValue[0] = startConst;	charValue[1] = from++; 
						lex = LEX_CONST_CHAR; return from;
					}
					else {
						throw new NecroSQLSyntaxException("Position ["+from+"]: unclosed char constant");
					}
				case '\"':
					final int	startQuotedName = ++from;
					
					while (from < maxLen && source[from] != '\"') {
						from++;
					}
					if (from < maxLen) {
						charValue[0] = startQuotedName;	charValue[1] = from++; 
						lex = LEX_NAME; return from;
					}
					else {
						throw new NecroSQLSyntaxException("Position ["+from+"]: unclosed quoted name");
					}
				default :
					final int	startName = from, endName;
					
					while (from < maxLen && (source[from] >= '0' && source[from] <= '9' || source[from] >= 'a' && source[from] <= 'z' || source[from] >= 'A' && source[from] <= 'Z' || source[from] == '_')) {
						from++;
					}
					if (from == startName) {
						throw new NecroSQLSyntaxException("Position ["+from+"]: unknown symbols. Name awaited");
					}
					charValue[0] = startName;	charValue[1] = endName = from; 
			        
					int low = 0, high = WORDS.length - 1, mid, delta, diffLen, index, maxIndex;
	
			        while (low <= high) {
			            mid = (low + high) >>> 1;
						diffLen = (endName-startName) - WORDS[mid].template.length;
						delta = 0;
					
						for (index = 0, maxIndex = WORDS[mid].template.length + (diffLen < 0 ? diffLen : 0); index < maxIndex && delta == 0; index++) {
							delta = Character.toUpperCase(source[startName+index])-WORDS[mid].template[index];
						}
						if (delta == 0) {
							delta = diffLen;
						}
			            if (delta > 0) {
			                low = mid + 1;
			            }
			            else if (delta < 0) {
			                high = mid - 1;
			            }
			            else {
			            	lex = WORDS[mid].cargo; return from;
			            }
			        }
					lex = LEX_NAME; return from;
			}
		}
		else {
			lex = LEX_EOF; return from;
		}
	}

	Repo parse(final char[] source, int from) throws NecroSQLSyntaxException {
		Repo	result = null;
		
		switch (lex.lexType) {
			case Create	:
				from = parse(source,from,CREATE_TABLE,result = new CreateTableRepo());
				break;
			case Alter	:
				break;
			case Drop	:
				from = parse(source,from,DROP_TABLE,result = new DropTableRepo());
				break;
			case Insert	:
				from = parse(source,from,INSERT_TABLE,result = new InsertTableRepo());
				break;
			case Update	:
				from = parse(source,from,UPDATE_TABLE,result = new UpdateTableRepo());
				break;
			case Delete	:
				from = parse(source,from,DELETE_TABLE,result = new DeleteTableRepo());
				break;
			case Select	:
				break;
			default : throw new NecroSQLSyntaxException("Position ["+from+"]: unknown keyword (SELECT/INSERT/UPDATE/DELETE/CREATE/ALTER/DROP are available only)");
		}
		if (lex.lexType != LexType.EOF) {
			throw new NecroSQLSyntaxException("Position ["+from+"]: unparsed tail in the SQL!");
		}
		return result;
	}
	
	private int parse(final char[] source, int from, final Action[] actions, final Repo target) throws NecroSQLSyntaxException {
		final int	maxActions = actions.length;
		
next:	for (int index = 0; index < maxActions; index++) {
			switch (actions[index].action) {
				case asIs		:
					if (lex.lexType == ((AsIsAction)actions[index]).awaited) {
						from = next(source,from);
					}
					else {
						throw new NecroSQLSyntaxException("Position ["+from+"]: unwaited lexema ("+((AsIsAction)actions[index]).awaited+" awaiting)");
					}
					break;
				case extract	:
					if (lex.lexType == ((ExtractAction)actions[index]).awaited) {
						target.put(((ExtractAction)actions[index]).extractId,lex,source);
						from = next(source,from);
					}
					else {
						throw new NecroSQLSyntaxException("Position ["+from+"]: unwaited lexema ("+((ExtractAction)actions[index]).awaited+" awaiting)");
					}
					break;
				case option		:
					if (lex.lexType == ((OptionalAction)actions[index]).awaited) {
						from = parse(source,from,((OptionalAction)actions[index]).actions,target);
					}
					break;
				case choise		:
					for (OptionalAction item : ((ChoiseAction)actions[index]).actions) {
						if (lex.lexType == item.awaited) {
							from = parse(source,from,item.actions,target);
							continue next;
						}
					}
					final StringBuilder	sbDiagChoise = new StringBuilder();
					
					for (OptionalAction item : ((ChoiseAction)actions[index]).actions) {
						sbDiagChoise.append(',').append(item.awaited);
					}
					throw new NecroSQLSyntaxException("Position ["+from+"]: unwaited lexema. Need be one of ("+sbDiagChoise.toString().substring(1)+")");
				case loop		:
					for(;;) {
						from = parse(source,from,((LoopAction)actions[index]).actions,target);
						if (lex.lexType == ((LoopAction)actions[index]).continued) {
							from = next(source,from);
						}
						else {
							break;
						}
					}
					break;
				case expression	:
					from = parseExpression(source,from,((ExpressionAction)actions[index]).prty,calc = new Calculation());
					calc.add(Calculation.CMD_RET);
					target.put(((ExtractAction)actions[index]).extractId,lex,source);
					break;
			}
		}
		return from;
	}

	private int parseExpression(final char[] source, int from, final OperPrty prty, final Calculation calc) throws NecroSQLSyntaxException {
		switch (prty) {
			case Term 	:
				switch (lex.lexType) {
					case Name 		:
						calc.add(Calculation.CMD_LDNAME,source,charValue[0],charValue[1]);
						from = next(source,from);
						break;
					case Const		:
						switch (((ConstDesc)lex).constType) {
							case Int 	:
								calc.add(Calculation.CMD_LDI,source,charValue[0],charValue[1]);
								from = next(source,from);
								break;
							case Real	:
								calc.add(Calculation.CMD_LDR,source,charValue[0],charValue[1]);
								from = next(source,from);
								break;
							case String	:
								calc.add(Calculation.CMD_LDC,source,charValue[0],charValue[1]);
								from = next(source,from);
								break;
							case Boolean:
								calc.add(Calculation.CMD_LDL,((ConstDesc)lex).constValue ? 'T' : 'F');
								break;
							default :
								throw new NecroSQLSyntaxException("Position ["+from+"]: constant awaited (name, constant, fuction or open bracket)");
						}
						break;
					case Function	:
						break;
					case Open 		:
						from = parseExpression(source,next(source,from),OperPrty.Or,calc);
						if (lex.lexType == LexType.Close) {
							from = next(source,from);
						}
						else {
							throw new NecroSQLSyntaxException("Position ["+from+"]: close bracket is missing!");
						}
						break;
					default :
						throw new NecroSQLSyntaxException("Position ["+from+"]: term awaited (name, constant, fuction or open bracket)");
				}
			case Unary	:
				if (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					calc.add(Calculation.CMD_NEG);
				}
				else {
					from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				}
				break;
			case Mul	:
				from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				while (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					final Operation	code = ((OperDesc)lex).code;
					
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					switch (code) {
						case Mul : calc.add(Calculation.CMD_MUL); break;
						case Div : calc.add(Calculation.CMD_DIV); break;
						case Mod : calc.add(Calculation.CMD_MOD); break;
						default :
					}
				}
				break;
			case Add	:
				from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				while (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					final Operation	code = ((OperDesc)lex).code;
					
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					switch (code) {
						case Add : calc.add(Calculation.CMD_ADD); break;
						case Sub : calc.add(Calculation.CMD_SUB); break;
						default :
					}
				}
				break;
			case Cat	:
				char	catCount = 0;
				
				from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				while (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					catCount++;
				}
				if (catCount > 0) {
					calc.add(Calculation.CMD_CAT,catCount);	// Optimization to make bulk concatenation
				}
				break;
			case Cmp	:
				from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				if (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					switch (((OperDesc)lex).code) {
						case EQ		:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_EQ);
							break;
						case NE		:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_NE);
							break;
						case LT		:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_LT);
							break;
						case LE		:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_LE);
							break;
						case GT		:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_GT);
							break;
						case GE		:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_GE);
							break;
						case In		:
							from = next(source,from);
							if (lex.lexType == LexType.Open) {
								char	count = 0;
								
								do {from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
									count++;
								} while (lex.lexType == LexType.List);
								
								if (lex.lexType == LexType.Close) {
									from = next(source,from);
									calc.add(Calculation.CMD_INLIST,count);
								}
								else {
									throw new NecroSQLSyntaxException("Position ["+from+"]: close bracket is missing!");
								}
							}
							else {
								throw new NecroSQLSyntaxException("Position ["+from+"]: open bracket is missing!");
							}
							break;
						case Range	:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							if (lex.lexType == LexType.Oper && ((OperDesc)lex).code == Operation.And) {
								from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
								calc.add(Calculation.CMD_INRANGE);
							}
							else {
								throw new NecroSQLSyntaxException("Position ["+from+"]: 'AND' is missing!");
							}
							break;
						case Like	:
							from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
							calc.add(Calculation.CMD_LIKE);
							break;
						case Not	:
							from = next(source,from);
							if (lex.lexType == LexType.Oper) {
								switch (((OperDesc)lex).code) {
									case Like 	:
										from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
										calc.add(Calculation.CMD_LIKE);
										calc.add(Calculation.CMD_NOT);
										break;
									case In 	:
										from = next(source,from);
										if (lex.lexType == LexType.Open) {
											char	count = 0;
											
											do {from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
												count++;
											} while (lex.lexType == LexType.List);
											
											if (lex.lexType == LexType.Close) {
												from = next(source,from);
												calc.add(Calculation.CMD_INLIST,count);
												calc.add(Calculation.CMD_NOT,count);
											}
											else {
												throw new NecroSQLSyntaxException("Position ["+from+"]: close bracket is missing!");
											}
										}
										else {
											throw new NecroSQLSyntaxException("Position ["+from+"]: open bracket is missing!");
										}
										break;
									default :
										throw new NecroSQLSyntaxException("Position ["+from+"]: 'LIKE' or 'IN' awaited!");
								}
							}
							else {
								throw new NecroSQLSyntaxException("Position ["+from+"]: operation is missing!");
							}
						default :
							throw new NecroSQLSyntaxException("Position ["+from+"]: unwaited comparison operator!");
					}
				}
				break;
			case Not	:
				if (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					calc.add(Calculation.CMD_NOT);
				}
				else {
					from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				}
				break;
			case And	:
				from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				while (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					calc.add(Calculation.CMD_AND);
				}
				break;
			case Or		:
				from = parseExpression(source,from,OperPrty.values()[prty.ordinal()-1],calc);
				while (lex.lexType == LexType.Oper && ((OperDesc)lex).prty == prty) {
					from = parseExpression(source,next(source,from),OperPrty.values()[prty.ordinal()-1],calc);
					calc.add(Calculation.CMD_OR);
				}
				break;
		}
		return from;
	}

	static class LexDesc {
		final LexType	lexType;

		LexDesc(final LexType lexType) {
			this.lexType = lexType;
		}

		@Override
		public String toString() {
			return "LexDesc [lexType=" + lexType + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((lexType == null) ? 0 : lexType.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			LexDesc other = (LexDesc) obj;
			if (lexType != other.lexType) return false;
			return true;
		}
	}

	static class OperDesc extends LexDesc {
		final OperPrty	prty;
		final Operation	code;

		OperDesc(final OperPrty prty, final Operation code) {
			super(LexType.Oper);
			this.prty = prty;
			this.code = code;
		}

		@Override
		public String toString() {
			return "OperDesc [prty=" + prty + ", code=" + code + "]";
		}
	}

	static class ConstDesc extends LexDesc {
		final ConstType	constType;
		final boolean	constValue;

		ConstDesc(final ConstType constType) {
			this(constType,false);
		}

		ConstDesc(final ConstType constType, final boolean constValue) {
			super(LexType.Const);
			this.constType = constType;
			this.constValue = constValue;
		}
		
		@Override
		public String toString() {
			return "ConstDesc [constType=" + constType + ", constValue=" + constValue + "]";
		}
	}
	
	static class FuncDesc extends LexDesc {
		final FunctionId	func;
		final boolean		isGroup;

		FuncDesc(final FunctionId func, final boolean isGroup) {
			super(LexType.Function);
			this.func = func;
			this.isGroup = isGroup;
		}

		@Override
		public String toString() {
			return "FuncDesc [func=" + func + ", isGroup=" + isGroup + "]";
		}
	}

	static class TypeDesc extends LexDesc {
		final DataType	dataType;
		final boolean	canBeLength;
		final boolean	canBeprecision;
		final int		maxLength;
		final int 		maxPrecision;

		TypeDesc(final DataType dataType, final boolean canBeLength, final boolean canBeprecision, final int maxLength, final int maxPrecision) {
			super(LexType.DataType);
			this.dataType = dataType;
			this.canBeLength = canBeLength;
			this.canBeprecision = canBeprecision;
			this.maxLength = maxLength;
			this.maxPrecision = maxPrecision;
		}

		@Override
		public String toString() {
			return "TypeDesc [dataType=" + dataType + ", canBeLength=" + canBeLength + ", canBeprecision=" + canBeprecision + ", maxLength=" + maxLength + ", maxPrecision=" + maxPrecision + "]";
		}
	}
		
	static class Decode {
		final char[]	template;
		final LexDesc	cargo;
		
		Decode(final String template, final LexDesc cargo) {
			this.template = template.toUpperCase().toCharArray();
			this.cargo = cargo;
		}

		@Override
		public String toString() {
			return "Decode [template=" + Arrays.toString(template) + ", cargo=" + cargo + "]";
		}
	}

	static abstract class Action {
		final ActionType	action;

		Action(final ActionType action) {
			this.action = action;
		}
	}
	
	static class AsIsAction extends Action {
		final LexType	awaited;

		public AsIsAction(final LexType awaited) {
			super(ActionType.asIs);
			this.awaited = awaited;
		}
	}

	static class ExtractAction extends Action {
		final LexType	awaited;
		final int		extractId;

		public ExtractAction(final LexType awaited, int extractId) {
			super(ActionType.extract);
			this.awaited = awaited;
			this.extractId = extractId;
		}
	}
	
	static class OptionalAction extends Action {
		final LexType	awaited;
		final Action[]	actions;

		public OptionalAction(final LexType awaited, final Action... actions) {
			super(ActionType.option);
			this.awaited = awaited;
			this.actions = actions;
		}
	}
	
	static class ChoiseAction extends Action {
		final OptionalAction[]	actions;

		public ChoiseAction(final OptionalAction... actions) {
			super(ActionType.choise);
			this.actions = actions; 
		}
	}

	static class LoopAction extends Action {
		final LexType	continued;
		final Action[]	actions;

		public LoopAction(final LexType continued, final Action... actions) {
			super(ActionType.loop);
			this.continued = continued;
			this.actions = actions; 
		}
	}

	static class ExpressionAction extends Action {
		final OperPrty	prty;
		final int		extractId;

		public ExpressionAction(OperPrty prty, final int extractId) {
			super(ActionType.expression);
			this.prty = prty;
			this.extractId = extractId;
		}
	}
	
	abstract static class Repo {
		abstract void put(int id, LexDesc content, char[] source); 
		abstract Object get(int id); 
		abstract Object get(int id, int sequential); 
		abstract int size(int id); 
	}
	
	class CreateTableRepo extends Repo{
		static final int 		TABLE_NAME = 0;
		static final int 		FIELD_NAME = 1;
		static final int 		FIELD_TYPE = 2;
		static final int 		FIELD_SIZE = 3;
		static final int 		FIELD_PRECISION = 4;

		private String			tableName = "";
		private String[]		fieldName = new String[0];
		private DataType[]		fieldType = new DataType[0];
		private int[]			size = new int[0];
		private int[]			precision = new int[0];
		
		@Override
		void put(final int id, final LexDesc content, final char[] source) {
			switch (id) {
				case TABLE_NAME		: 
					tableName = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case FIELD_NAME		:
					fieldName = Arrays.copyOf(fieldName,fieldName.length+1);
					fieldType = Arrays.copyOf(fieldType,fieldType.length+1);
					size = Arrays.copyOf(size,size.length+1);
					precision = Arrays.copyOf(precision,precision.length+1);
					fieldName[fieldName.length-1] = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case FIELD_TYPE		:
					fieldType[fieldName.length-1] = ((TypeDesc)content).dataType;
					break;
				case FIELD_SIZE		:
					size[fieldName.length-1] = (int)longValue;
					break;
				case FIELD_PRECISION:
					precision[fieldName.length-1] = (int)longValue;
					break;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to store data");
			}
		}

		@Override
		Object get(int id) {
			switch (id) {
				case TABLE_NAME		: return tableName;
				case FIELD_NAME		:	case FIELD_TYPE		:
				case FIELD_SIZE		:	case FIELD_PRECISION:
					throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int,int) method");
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		Object get(int id, int sequential) {
			switch (id) {
				case TABLE_NAME		: 
					throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int) method");
				case FIELD_NAME		: return fieldName[sequential];
				case FIELD_TYPE		: return fieldType[sequential];
				case FIELD_SIZE		: return size[sequential];
				case FIELD_PRECISION:  return precision[sequential];
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		int size(int id) {
			switch (id) {
				case TABLE_NAME		: return 0;
				case FIELD_NAME		: return fieldName.length;
				case FIELD_TYPE		: return fieldType.length;
				case FIELD_SIZE		: return size.length;
				case FIELD_PRECISION:  return precision.length;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		public String toString() {
			return "CreateTableRepo [tableName=" + tableName + ", fieldName=" 
					+ Arrays.toString(fieldName) + ", fieldType="
					+ Arrays.toString(fieldType) + ", size="
					+ Arrays.toString(size) + ", precision="
					+ Arrays.toString(precision) + "]";
		}
	}
	
	class DropTableRepo extends Repo{
		static final int 		TABLE_NAME = 0;

		private String			tableName = "";
		
		@Override
		void put(int id, LexDesc content, char[] source) {
			switch (id) {
				case TABLE_NAME		: 
					tableName = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to store data");
			}
		}

		@Override
		Object get(int id) {
			switch (id) {
				case TABLE_NAME		: return tableName;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		Object get(int id, int sequential) {
			throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int) method");
		}

		@Override
		int size(int id) {
			switch (id) {
				case TABLE_NAME		: return 0;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		public String toString() {
			return "DropTableRepo [tableName=" + tableName + "]";
		}
	}

	class DeleteTableRepo extends Repo{
		static final int 		TABLE_NAME = 0;
		static final int 		WHERE = 1;

		private String			tableName = "";
		private Calculation		where = Calculation.ALWAYS_TRUE;
		
		@Override
		void put(int id, LexDesc content, char[] source) {
			switch (id) {
				case TABLE_NAME		: 
					tableName = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case WHERE			: 
					where = calc;
					break;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to store data");
			}
		}

		@Override
		Object get(int id) {
			switch (id) {
				case TABLE_NAME		: return tableName;
				case WHERE			: return where;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		Object get(int id, int sequential) {
			throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int) method");
		}

		@Override
		int size(int id) {
			switch (id) {
				case TABLE_NAME		: return 0;
				case WHERE			: return 0;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		public String toString() {
			return "DeleteTableRepo [tableName=" + tableName + ", where=" + where + "]";
		}
	}
	
	class UpdateTableRepo extends Repo{
		static final int 		TABLE_NAME = 0;
		static final int 		FIELD_NAME = 1;
		static final int 		FIELD_VALUE = 2;
		static final int 		WHERE = 4;

		private String			tableName = "";
		private Calculation		where = Calculation.ALWAYS_TRUE;
		private String[]		fieldName = new String[0];
		private Calculation[]	fieldValue = new Calculation[0];
		
		@Override
		void put(final int id, final LexDesc content, final char[] source) {
			switch (id) {
				case TABLE_NAME		: 
					tableName = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case FIELD_NAME		:
					fieldName = Arrays.copyOf(fieldName,fieldName.length+1);
					fieldValue = Arrays.copyOf(fieldValue,fieldValue.length+1);
					fieldName[fieldName.length-1] = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case FIELD_VALUE	:
					fieldValue[fieldName.length-1] = calc;
					break;
				case WHERE			: 
					where = calc;
					break;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to store data");
			}
		}

		@Override
		Object get(int id) {
			switch (id) {
				case TABLE_NAME		: return tableName;
				case WHERE			: return where;
				case FIELD_NAME		:
				case FIELD_VALUE	:
					throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int,int) method");
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		Object get(int id, int sequential) {
			switch (id) {
				case TABLE_NAME		: 
				case WHERE			: 
					throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int) method");
				case FIELD_NAME		: return fieldName[sequential];
				case FIELD_VALUE	: return fieldValue[sequential];
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		int size(int id) {
			switch (id) {
				case TABLE_NAME		: return 0;
				case WHERE			: return 0;
				case FIELD_NAME		: return fieldName.length;
				case FIELD_VALUE	: return fieldValue.length;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		public String toString() {
			return "UpdateTableRepo [tableName=" + tableName + ", where="
					+ where + ", fieldName=" + Arrays.toString(fieldName)
					+ ", fieldValue=" + Arrays.toString(fieldValue) + "]";
		}
	}

	class InsertTableRepo extends Repo{
		static final int 		TABLE_NAME = 0;
		static final int 		FIELD_NAME = 1;
		static final int 		FIELD_VALUE = 2;

		private String			tableName = "";
		private String[]		fieldName = new String[0];
		private Calculation[]	fieldValue = new Calculation[0];
		
		@Override
		void put(final int id, final LexDesc content, final char[] source) {
			switch (id) {
				case TABLE_NAME		: 
					tableName = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case FIELD_NAME		:
					fieldName = Arrays.copyOf(fieldName,fieldName.length+1);
					fieldValue = Arrays.copyOf(fieldValue,fieldValue.length+1);
					fieldName[fieldName.length-1] = new String(source,charValue[0],charValue[1]-charValue[0]);
					break;
				case FIELD_VALUE	:
					fieldValue[fieldName.length-1] = calc;
					break;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to store data");
			}
		}

		@Override
		Object get(int id) {
			switch (id) {
				case TABLE_NAME		: return tableName;
				case FIELD_NAME		:
				case FIELD_VALUE	:
					throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int,int) method");
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		Object get(int id, int sequential) {
			switch (id) {
				case TABLE_NAME		: 
					throw new IllegalArgumentException("Fields with id ["+id+"] need be selected by get(int) method");
				case FIELD_NAME		: return fieldName[sequential];
				case FIELD_VALUE	: return fieldValue[sequential];
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		int size(int id) {
			switch (id) {
				case TABLE_NAME		: return 0;
				case FIELD_NAME		: return fieldName.length;
				case FIELD_VALUE	: return fieldValue.length;
				default :
					throw new IllegalArgumentException("Unsupported id ["+id+"] to get data");
			}
		}

		@Override
		public String toString() {
			return "InsertTableRepo [tableName=" + tableName + ", fieldName="
					+ Arrays.toString(fieldName) + ", fieldValue="
					+ Arrays.toString(fieldValue) + "]";
		}
	}
}
