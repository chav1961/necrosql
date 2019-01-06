package chav1961.necrosql.sqlprocessing;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import chav1961.necrosql.sqlprocessing.Parser.DropTableRepo;
import chav1961.necrosql.sqlprocessing.Parser.LexDesc;
import chav1961.necrosql.sqlprocessing.Parser.LexType;
import chav1961.necrosql.sqlprocessing.Parser.CreateTableRepo;

public class ParserTest {
	private static final char[]		TEST_PARSE = " \n().,+-*/%=>>=<<=<>||123 123.456\'test\'\"field\"select mzinana".toCharArray();
	private static final LexDesc[]	TEST_RESULT = new LexDesc[]{
										Parser.LEX_OPEN,
										Parser.LEX_CLOSE,
										Parser.LEX_DOT,
										Parser.LEX_LIST,
										Parser.LEX_OPER_ADD,
										Parser.LEX_OPER_SUB,
										Parser.LEX_OPER_MUL,
										Parser.LEX_OPER_DIV,
										Parser.LEX_OPER_MOD,
										Parser.LEX_OPER_EQ,
										Parser.LEX_OPER_GT,
										Parser.LEX_OPER_GE,
										Parser.LEX_OPER_LT,
										Parser.LEX_OPER_LE,
										Parser.LEX_OPER_NE,
										Parser.LEX_OPER_CAT,
										Parser.LEX_CONST_INT,
										Parser.LEX_CONST_REAL,
										Parser.LEX_CONST_CHAR,
										Parser.LEX_NAME,
										new LexDesc(LexType.Select),
										Parser.LEX_NAME,
										Parser.LEX_EOF
									};
	private static final char[][]	ILLEGAL_PARSE = new char[][] {
										 "?".toCharArray()
										,"|".toCharArray()
										,"\'asd".toCharArray()
										,"\"asd".toCharArray()
									};
	
	
	private static final char[]		CREATE_TABLE_TEST = "create table MyTable (field1 char(100), field2 numeric(10,2), fields3 date, field4 boolean, field5 blob)".toCharArray();
	private static final char[]		DROP_TABLE_TEST = "drop table MyTable".toCharArray();
	

	@Test
	public void lexicalAnalyzeTest() throws SQLException {
		final Parser	p = new Parser();
		int				from = 0;
		
		for (int index = 0, maxIndex = TEST_RESULT.length; index < maxIndex; index++) {
			from = p.next(TEST_PARSE,from);
			Assert.assertEquals(p.getLexema(),TEST_RESULT[index]);
		}
		for (char[] item : ILLEGAL_PARSE) {
			try{p.parse(item,0);
				Assert.fail("Mandatory exception was not detected (illegal lexema)");
			} catch (SQLException exc) {
			}
			
		}
	}

	@Test
	public void createAndDropTest() throws SQLException {
		final Parser	p = new Parser();
		
		Object			result = p.parse(CREATE_TABLE_TEST,p.next(CREATE_TABLE_TEST,0));
		
		Assert.assertTrue(result instanceof CreateTableRepo);
		final CreateTableRepo	create = (CreateTableRepo)result;
		
		Assert.assertEquals(create.size(CreateTableRepo.FIELD_NAME),5);
		Assert.assertEquals(create.get(CreateTableRepo.TABLE_NAME),"MyTable");
		Assert.assertEquals(create.get(CreateTableRepo.FIELD_NAME,0),"field1");

		result = p.parse(DROP_TABLE_TEST,p.next(DROP_TABLE_TEST,0));

		Assert.assertTrue(result instanceof DropTableRepo);
		final DropTableRepo		drop = (DropTableRepo)result;
		
		Assert.assertEquals(drop.size(DropTableRepo.TABLE_NAME),0);
		Assert.assertEquals(create.get(DropTableRepo.TABLE_NAME),"MyTable");
	}
}
