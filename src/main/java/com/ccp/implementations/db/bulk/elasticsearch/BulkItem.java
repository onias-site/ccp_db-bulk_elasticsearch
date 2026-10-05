package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;

/**
 * Represents a single bulk operation item for Elasticsearch. Converts an (abstract) CcpBulkItem
 * into its text representation in the NDJSON (Newline Delimited JSON) format required by the
 * Elasticsearch _bulk API.
 */
class BulkItem {
	/** The document id. */
	final String id;
	/** The index name. */
	final String entity;
	/** The NDJSON lines of the item (action line and, except for delete, document line), each ended by the line separator. */
	final String content;

	/**
	 * Builds the NDJSON content of the item from its operation.
	 * @param item the abstract bulk item
	 */
	public BulkItem(CcpBulkItem item) {

		String name = item.operation.name();
		BulkOperation bulkOperation = BulkOperation.valueOf(name);
		String content = bulkOperation.getContent(item);
		CcpEntityMetaData entityDetails = item.entity.getEntityMetaData();
		this.entity = entityDetails.entityName;
		this.content = content;
		this.id = item.id;
	}
	
	
	
	/**
	 * Describes id, index and content.
	 * @return the description
	 */
	public String toString() {
		String textWithId = "BulkItem [id=" + id;
		String textWithEntityLabel = textWithId + ", entity=";
		String textWithEntity = textWithEntityLabel + entity;
		String textWithContentLabel = textWithEntity + ", content=";
		String textWithContent = textWithContentLabel + content;
		String bulkItemAsText = textWithContent + "]";
		return bulkItemAsText;
	}


	/**
	 * Consistent with {@link #equals(Object)}: hash of index name plus id.
	 * @return the hash code
	 */
	public int hashCode() {
		String entityAndId = this.entity + this.id;
		int hashCode = (entityAndId).hashCode();
		return hashCode;
	}
	
	
	/**
	 * Two items are equal when they have the same index and id.
	 * @param obj the other object
	 * @return {@code true} for the same index and id
	 */
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
