package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;

/**
 * System property keys used by {@code BulkOperation} whose real value contains a dot
 * and therefore cannot be written as a Java identifier. Same pattern as
 * {@code ElasticSerchDbBulkExecutorSpecialWords}: legal name in the constant, real value
 * in the constructor, exposed by {@code getValue()}.
 */
enum BulkOperationSpecialWords implements CcpJsonFieldName {
	line_separator("line.separator"),
;
	private final String value;

	private BulkOperationSpecialWords(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}
}
