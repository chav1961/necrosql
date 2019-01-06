package chav1961.necrosql;

class NecroUtils {
	static boolean like(final String source, final String template) {
		if (source == null || template == null) {
			return false;
		}
		else {
			return like(source.toCharArray(),0,template.toCharArray(),0);
		}
	}
	
	private static boolean like(final char[] source, int fromSrc, final char[] template, int fromTempl) {
		while (fromSrc < source.length && fromTempl < template.length) {
			switch (template[fromTempl]) {
				case '%' :
					while (fromSrc < source.length && !like(source,fromSrc,template,fromTempl+1)) {
						fromSrc++;
					}
					return fromSrc < source.length;
				case '_' :
					fromTempl++;
					fromSrc++;
					break;
				default :
					if (source[fromSrc] != template[fromTempl]) {
						return false;
					}
					else {
						fromTempl++;
						fromSrc++;
					}
					break;
			}
		}
		return fromSrc == source.length && fromTempl == template.length 
			|| template.length > 0 && template[template.length-1] == '%';
	}
}
