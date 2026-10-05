package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
import java.util.stream.Stream;

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
	 * Finds, among the items of the {@code _bulk} response, the one of the same operation, id and index as the bulk item.
	 * @param bulkItem the original bulk item
	 * @param result the items of the bulk response
	 * @throws CcpErrorBulkItemNotFound when the response has no item for it
	 */
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {

		CcpEntityMetaData entityDetails = bulkItem.entity.getEntityMetaData();
		String entityName = entityDetails.entityName;
		CcpDbRequester dbRequester = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String fieldNameToEntity = dbRequester.getFieldNameToEntity();
		String fieldNameToId = dbRequester.getFieldNameToId();
		Stream<CcpJsonRepresentation> resultStream = result.stream();
		var operationResultsStream = resultStream.map(x -> x.getInnerJson(bulkItem.operation));
		List<CcpJsonRepresentation> operationResults = operationResultsStream.collect(Collectors.toList());
		Stream<CcpJsonRepresentation> operationResultsToFilter = operationResults.stream();
		var resultsWithSameId = operationResultsToFilter.filter(x -> x.getAsString(new CcpFieldName(fieldNameToId)).equals(bulkItem.id));

		List<CcpJsonRepresentation> filteredById = resultsWithSameId.collect(Collectors.toList());
		boolean filteredByIdEmpty = filteredById.isEmpty();

		if(filteredByIdEmpty) {
			CcpErrorBulkItemNotFound ccpErrorBulkItemNotFound = new CcpErrorBulkItemNotFound(bulkItem, result);

			throw ccpErrorBulkItemNotFound;
		}
		Stream<CcpJsonRepresentation> filteredByIdStream = filteredById.stream();
		var resultsWithSameEntity = filteredByIdStream
		.filter(x -> x.getAsString(new CcpFieldName(fieldNameToEntity)).equals(entityName));
		Optional<CcpJsonRepresentation> matchingResult = resultsWithSameEntity
		.findFirst();
		boolean matchingResultPresent = matchingResult.isPresent();

		boolean idNotFoundInTheEntity = false == matchingResultPresent;

		if(idNotFoundInTheEntity) {
			CcpErrorBulkItemNotFound idNotFoundInEntityError = new CcpErrorBulkItemNotFound(bulkItem, result);
			throw idNotFoundInEntityError;
		}

		CcpJsonRepresentation details = matchingResult.get();

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
