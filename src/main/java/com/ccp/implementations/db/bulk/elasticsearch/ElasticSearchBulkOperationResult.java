package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpFieldName;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;

import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * Represents the result of a single operation inside an Elasticsearch bulk response.
 * Finds the matching item in the result list by id and entity name, exposing the
 * HTTP status, error details and the original {@code CcpBulkItem}.
 */
class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	/** Fields of the result and of its description. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The index name. */
		entity,
		/** The document id. */
		id,
		/** The record. */
		json,
		/** Unused. */
		filteredRecords,
		/** The HTTP status of the item. */
		status,
		/** The bulk item. */
		bulkItem
	}
	
	/** The {@code error} of the item in the response (empty on success). */
	private final CcpJsonRepresentation errorDetails;

	/** The original bulk item. */
	private final CcpBulkItem bulkItem;
	
	/** The HTTP status of the item. */
	private final Integer status;
	
	/**
	 * Takes, among the items of the {@code _bulk} response, the one at the same position as the bulk item in the request
	 * (Elasticsearch answers the actions in the order they were sent) and checks that it has the same id and index.
	 * Until 2026-11-03 every bulk item scanned the whole response, which made a bulk of n items cost n² and kept the
	 * recreation of the vis entities (more than 250 thousand seeds) running for hours after the records were written.
	 * @param bulkItem the original bulk item
	 * @param result the items of the bulk response
	 * @param position the position of the bulk item in the request
	 * @throws CcpErrorBulkItemNotFound when the response has no item at that position or it belongs to another record
	 */
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result, int position) {

		CcpEntityMetaData entityDetails = bulkItem.entity.getEntityMetaData();
		String entityName = entityDetails.entityName;
		CcpDbRequester dbRequester = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String fieldNameToEntity = dbRequester.getFieldNameToEntity();
		String fieldNameToId = dbRequester.getFieldNameToId();
		int resultSize = result.size();
		boolean noItemAtThePosition = position >= resultSize;

		if(noItemAtThePosition) {
			throw new CcpErrorBulkItemNotFound(bulkItem, result);
		}
		// the result comes under the Elasticsearch action, which is not always the name of the operation (scriptedUpdate is an update)
		String operationName = bulkItem.operation.name();
		BulkOperation bulkOperation = BulkOperation.valueOf(operationName);
		CcpJsonFieldName action = bulkOperation.getAction();
		CcpJsonRepresentation responseItem = result.get(position);
		CcpJsonRepresentation details = responseItem.getInnerJson(action);
		CcpFieldName idField = new CcpFieldName(fieldNameToId);
		String responseId = details.getAsString(idField);
		CcpFieldName entityField = new CcpFieldName(fieldNameToEntity);
		String responseEntity = details.getAsString(entityField);
		boolean anotherRecord = false == responseId.equals(bulkItem.id) || false == responseEntity.equals(entityName);

		if(anotherRecord) {
			throw new CcpErrorBulkItemNotFound(bulkItem, result);
		}

		this.status = details.getAsIntegerNumber(JsonFieldNames.status); 
		this.errorDetails = details.getInnerJson(CcpJsonCommonsFields.error);
		this.bulkItem = bulkItem;
	}
	
	/**
	 * Returns the error of the item.
	 * @return the error details, empty on success
	 */
	public CcpJsonRepresentation getErrorDetails() {
		return this.errorDetails;
	}

	/**
	 * Returns the original bulk item.
	 * @return the bulk item
	 */
	public CcpBulkItem getBulkItem() {
		return this.bulkItem;
	}

	/**
	 * Tells whether the item failed.
	 * @return {@code true} when the response has an error for the item
	 */
	public boolean hasError() {
		boolean empty = this.errorDetails.isEmpty();
		boolean hasErrorDetails = false == empty;
		return hasErrorDetails;
	}

	/**
	 * Returns the HTTP status of the item.
	 * @return the status
	 */
	public int status() {
		return this.status;
	}


	/**
	 * Describes the bulk item, the status and the error details.
	 * @return the description
	 */
	public String toString() {
		CcpJsonRepresentation asMap = this.bulkItem.asMap();
		CcpJsonRepresentation jsonWithBulkItem = CcpOtherConstants.EMPTY_JSON
				.put(JsonFieldNames.bulkItem, asMap);
				CcpJsonRepresentation jsonWithStatus = jsonWithBulkItem
				.put(JsonFieldNames.status, this.status);
				CcpJsonRepresentation jsonWithErrorDetails = jsonWithStatus
				.put(CcpJsonCommonsFields.errorDetails, this.errorDetails)
				;
		String string = jsonWithErrorDetails.toString();
		return string;
	}

	/** Raised when the bulk response has no item for a bulk item that was sent. */
	@SuppressWarnings("serial")
	public static class CcpErrorBulkItemNotFound extends RuntimeException {
		/**
		 * Builds the error naming the id, the entity and the whole response.
		 * @param bulkItem the bulk item
		 * @param result the items of the bulk response
		 */
		private CcpErrorBulkItemNotFound(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {
			super( String.format("Id '%s' from entity '%s' not found. Complete list: " + result, bulkItem.id, bulkItem.entity.getEntityMetaData().entityName));
		}
	}
}
