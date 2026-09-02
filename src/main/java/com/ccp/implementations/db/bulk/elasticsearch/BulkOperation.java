
package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;


/**
 * Enum que representa as operações bulk do Elasticsearch ({@code delete}, {@code update},
 * {@code create}). Cada constante gera a segunda linha do par NDJSON correspondente à
 * operação via {@code getContent(CcpBulkItem)}.
 */
enum BulkOperation implements CcpJsonFieldName{
	delete {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return "";
		}
	}, update {
		
		String getSecondLine(CcpJsonRepresentation json) {
			CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.doc, json);
			String asUgglyJson = put.asUgglyJson();
			return asUgglyJson;
		}
	}, create {
		
		String getSecondLine(CcpJsonRepresentation json) {
			String asUgglyJson2 = json.asUgglyJson();
			return asUgglyJson2;
		}
	}
	;
	static final String NEW_LINE = System.getProperty("line.separator");

	public String getContent(CcpBulkItem item) {

		String firstLine = this.getFirstLine(item);
		
		String secondLine = this.getSecondLine(item.json);
		String firstLineMais = firstLine + NEW_LINE;
		String firstLineMaisMais = firstLineMais + secondLine;

		String content = firstLineMaisMais + NEW_LINE;
	
		return content;
	}

	private String getFirstLine(CcpBulkItem item) {
		CcpEntityMetaData entityDetails = item.entity.getEntityMetaData();
		String entityName = entityDetails.entityName;
		CcpJsonRepresentation addToItem = CcpOtherConstants.EMPTY_JSON
				.addToItem(this, JsonFieldNames._index, entityName);
				CcpJsonRepresentation addToItem2 = addToItem
				.addToItem(this, JsonFieldNames._id, item.id);
				String firstLine = addToItem2
				.asUgglyJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpJsonRepresentation json);
	
	enum JsonFieldNames implements CcpJsonFieldName{
		doc, _id, _index
	}
}
