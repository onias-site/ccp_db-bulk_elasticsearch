package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;

/**
 * Represents a single bulk operation item for Elasticsearch. Converts an (abstract) CcpBulkItem
 * into its text representation in the NDJSON (Newline Delimited JSON) format required by the
 * Elasticsearch _bulk API.
 */
class BulkItem {
	final String id;
	final String entity;
	final String content;

	public BulkItem(CcpBulkItem item) {

		String name = item.operation.name();
		BulkOperation bulkOperation = BulkOperation.valueOf(name);
		String content = bulkOperation.getContent(item);
		CcpEntityMetaData entityDetails = item.entity.getEntityMetaData();
		this.entity = entityDetails.entityName;
		this.content = content;
		this.id = item.id;
	}
	
	
	
	public String toString() {
		String textWithId = "BulkItem [id=" + id;
		String textWithEntityLabel = textWithId + ", entity=";
		String textWithEntity = textWithEntityLabel + entity;
		String textWithContentLabel = textWithEntity + ", content=";
		String textWithContent = textWithContentLabel + content;
		String bulkItemAsText = textWithContent + "]";
		return bulkItemAsText;
	}


	public int hashCode() {
		String entityAndId = this.entity + this.id;
		int hashCode = (entityAndId).hashCode();
		return hashCode;
	}
	
	
	public boolean equals(Object obj) {
		try {
			BulkItem other = (BulkItem)obj;
			boolean entityEquals = other.entity.equals(this.entity);

			boolean differentEntity = false == entityEquals;
			
			if(differentEntity) {
				return false;
			}
			boolean idEquals = other.id.equals(this.id);

			boolean differentId = false == idEquals;
			
			if(differentId) {
				return false;
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
