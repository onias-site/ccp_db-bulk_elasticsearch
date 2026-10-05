
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
	/** {@code delete} action: no document line. */
	delete {
		
		/**
		 * No document line for a delete.
		 * @param json the record
		 * @return an empty text
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			return "";
		}
	},
	/** {@code update} action: the document line is {@code {"doc": record}} (partial update). */
	update {
		
		/**
		 * Wraps the record in {@code doc}.
		 * @param json the record
		 * @return the compact JSON line
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			CcpJsonRepresentation jsonWithDoc = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.doc, json);
			String asUgglyJson = jsonWithDoc.asUgglyJson();
			return asUgglyJson;
		}
	},
	/** {@code create} action: the document line is the record itself. */
	create {

		/**
		 * Returns the record as a compact JSON line.
		 * @param json the record
		 * @return the compact JSON line
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			String asUgglyJson = json.asUgglyJson();
			return asUgglyJson;
		}
	}
	;
	/** The platform line separator, used to end the NDJSON lines. */
	static final String NEW_LINE = System.getProperty(BulkOperationSpecialWords.line_separator.getValue());

	/**
	 * Builds the two NDJSON lines of the item: the action line and the document line, each ended by {@link #NEW_LINE}.
	 * @param item the bulk item
	 * @return the NDJSON content
	 */
	public String getContent(CcpBulkItem item) {

		String firstLine = this.getFirstLine(item);
		
		String secondLine = this.getSecondLine(item.json);
		String firstLineWithNewLine = firstLine + NEW_LINE;
		String bothLines = firstLineWithNewLine + secondLine;

		String content = bothLines + NEW_LINE;

		return content;
	}

	/**
	 * Builds the action line: {@code {"<operation>": {"_index": ..., "_id": ...}}}.
	 * @param item the bulk item
	 * @return the compact JSON line
	 */
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
	
	/**
	 * Builds the document line of the operation.
	 * @param json the record
	 * @return the document line
	 */
	abstract String getSecondLine(CcpJsonRepresentation json);
	
	/** Fields of the document line. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The partial document of an update. */
		doc
	}
}
