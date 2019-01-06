package chav1961.necrosql;

import org.junit.Assert;
import org.junit.Test;

public class NecroUtilsTest {

	@Test
	public void testLike() {
		Assert.assertFalse(NecroUtils.like(null,"?"));
		Assert.assertFalse(NecroUtils.like("?",null));
		
		Assert.assertTrue(NecroUtils.like("",""));
		Assert.assertFalse(NecroUtils.like("1",""));
		Assert.assertFalse(NecroUtils.like("","1"));

		Assert.assertFalse(NecroUtils.like("123","12"));
		Assert.assertFalse(NecroUtils.like("12","123"));
		Assert.assertTrue(NecroUtils.like("123","123"));

		Assert.assertTrue(NecroUtils.like("123","12_"));
		Assert.assertTrue(NecroUtils.like("123","_23"));
		Assert.assertTrue(NecroUtils.like("123","1_3"));
		Assert.assertFalse(NecroUtils.like("1234","1_3"));
		
		Assert.assertTrue(NecroUtils.like("123","12%"));
		Assert.assertTrue(NecroUtils.like("12345","12%"));
		Assert.assertTrue(NecroUtils.like("123","%"));

		Assert.assertTrue(NecroUtils.like("123","1%3"));
		Assert.assertTrue(NecroUtils.like("123","%3"));
		Assert.assertFalse(NecroUtils.like("124","%3"));
		
		Assert.assertTrue(NecroUtils.like("12345","1%3%5"));
		
	}
}
