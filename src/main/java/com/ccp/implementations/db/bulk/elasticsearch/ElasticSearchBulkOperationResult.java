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
import java.util.stream.Stream;/**
 * Represents the result of a single operation inside an Elasticsearch bulk response.
 * Finds the matching item in the result list by id and entity name, exposing the
 * HTTP status, error details and the original {@code CcpBulkItem}.
 */

class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	enum JsonFieldNames implements CcpJsonFieldName{
		entity, id, json, filteredRecords, status, bulkItem
	}
	
	private final CcpJsonRepresentation errorDetails;

	private final CcpBulkItem bulkItem;
	
	private final Integer status;
	
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
	
	public CcpJsonRepresentation getErrorDetails() {
		return this.errorDetails;
	}

	public CcpBulkItem getBulkItem() {
		return this.bulkItem;
	}

	public boolean hasError() {
		boolean empty = this.errorDetails.isEmpty();
		boolean hasErrorDetails = false == empty;
		return hasErrorDetails;
	}

	public int status() {
		return this.status;
	}


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

	@SuppressWarnings("serial")
	public static class CcpErrorBulkItemNotFound extends RuntimeException {
		private CcpErrorBulkItemNotFound(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {
			super( String.format("Id '%s' from entity '%s' not found. Complete list: " + result, bulkItem.id, bulkItem.entity.getEntityMetaData().entityName));
		}
	}
}
