
package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;


import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * Enum representing the Elasticsearch bulk operations ({@code delete}, {@code update},
 * {@code create}). Each constant produces the second line of the NDJSON pair for its
 * operation via {@code getContent(CcpBulkItem)}.
 */
enum BulkOperation implements CcpJsonFieldName{
	delete {
		
		String getSecondLine(CcpJsonRepresentation json) {
			return "";
		}
	}, update {
		
		String getSecondLine(CcpJsonRepresentation json) {
			CcpJsonRepresentation jsonWithDoc = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.doc, json);
			String asUgglyJson = jsonWithDoc.asUgglyJson();
			return asUgglyJson;
		}
	}, create {

		String getSecondLine(CcpJsonRepresentation json) {
			String asUgglyJson = json.asUgglyJson();
			return asUgglyJson;
		}
	}
	;
	static final String NEW_LINE = System.getProperty(BulkOperationSpecialWords.line_separator.getValue());

	public String getContent(CcpBulkItem item) {

		String firstLine = this.getFirstLine(item);
		
		String secondLine = this.getSecondLine(item.json);
		String firstLineWithNewLine = firstLine + NEW_LINE;
		String bothLines = firstLineWithNewLine + secondLine;

		String content = bothLines + NEW_LINE;

		return content;
	}

	private String getFirstLine(CcpBulkItem item) {
		CcpEntityMetaData entityDetails = item.entity.getEntityMetaData();
		String entityName = entityDetails.entityName;
		CcpJsonRepresentation actionWithIndex = CcpOtherConstants.EMPTY_JSON
				.addToItem(this, CcpJsonCommonsFields._index, entityName);
				CcpJsonRepresentation actionWithIndexAndId = actionWithIndex
				.addToItem(this, CcpJsonCommonsFields._id, item.id);
				String firstLine = actionWithIndexAndId
				.asUgglyJson();
		return firstLine;
	}
	
	abstract String getSecondLine(CcpJsonRepresentation json);
	
	enum JsonFieldNames implements CcpJsonFieldName{
		doc
	}
}
