package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;

enum ElasticSerchDbBulkExecutorSpecialWords implements CcpJsonFieldName{
	Content_Type("Content-Type"),
;
	static enum JsonFieldNames implements CcpJsonFieldName{
		items
	}
	private final String value;
	
	private ElasticSerchDbBulkExecutorSpecialWords(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}

}
