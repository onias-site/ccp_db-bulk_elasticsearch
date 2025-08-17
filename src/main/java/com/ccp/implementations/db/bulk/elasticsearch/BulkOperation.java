
package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.especifications.db.bulk.CcpBulkItem;


enum BulkOperation implements CcpJsonFieldName{
	delete {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return "";
		}
	}, update {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.doc, json).asUgglyJson();
		}
	}, create {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return json.asUgglyJson();
		}
	}
	;
	static final String NEW_LINE = System.getProperty("line.separator");

	public String getContent(CcpBulkItem item) {

		String firstLine = this.getFirstLine(item);
		
		String secondLine = this.getSecondLine(item.json);
		
		String content = firstLine + NEW_LINE + secondLine + NEW_LINE;
	
		return content;
	}

	private String getFirstLine(CcpBulkItem item) {
		String entityName = item.entity.getEntityName();
		String firstLine = CcpOtherConstants.EMPTY_JSON
				.addToItem(this, JsonFieldNames._index, entityName)
				.addToItem(this, JsonFieldNames._id, item.id)
				.asUgglyJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpJsonRepresentation json);
	
	enum JsonFieldNames implements CcpJsonFieldName{
		doc, _id, _index
	}
}
