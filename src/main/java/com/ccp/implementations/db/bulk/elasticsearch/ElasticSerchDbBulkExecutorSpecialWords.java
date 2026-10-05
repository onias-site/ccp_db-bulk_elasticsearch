package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;

/**
 * HTTP header names used by the bulk executor whose real value contains a character that a Java identifier cannot
 * have: legal name in the constant, real value in the constructor, exposed by {@code getValue()}.
 */
enum ElasticSerchDbBulkExecutorSpecialWords implements CcpJsonFieldName{
	/** The {@code Content-Type} header. */
	Content_Type("Content-Type"),
;
	/** Fields of the bulk response. */
	static enum JsonFieldNames implements CcpJsonFieldName{
		/** The results of the items. */
		items
	}
	/** The real header name. */
	private final String value;
	
	/**
	 * Associates the constant with its real name.
	 * @param value the real name
	 */
	private ElasticSerchDbBulkExecutorSpecialWords(String value) {
		this.value = value;
	}

	/**
	 * Returns the real header name.
	 * @return the real name
	 */
	public String getValue() {
		return this.value;
	}

}
