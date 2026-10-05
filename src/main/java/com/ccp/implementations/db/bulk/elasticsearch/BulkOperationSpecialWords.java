package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;

/**
 * System property keys used by {@code BulkOperation} whose real value contains a dot
 * and therefore cannot be written as a Java identifier. Same pattern as
 * {@code ElasticSerchDbBulkExecutorSpecialWords}: legal name in the constant, real value
 * in the constructor, exposed by {@code getValue()}.
 */
enum BulkOperationSpecialWords implements CcpJsonFieldName {
	/** The {@code line.separator} system property. */
	line_separator("line.separator"),
;
	/** The real key, which has a dot. */
	private final String value;

	/**
	 * Associates the constant with its real key.
	 * @param value the real key
	 */
	private BulkOperationSpecialWords(String value) {
		this.value = value;
	}

	/**
	 * Returns the real key.
	 * @return the real key
	 */
	public String getValue() {
		return this.value;
	}
}
