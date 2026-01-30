package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.bulk.CcpErrorBulkItemNotFound;
import com.ccp.especifications.db.utils.CcpDbRequester;
class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	enum JsonFieldNames implements CcpJsonFieldName{
		entity, id, json, filteredRecords, status, error, bulkItem, errorDetails
	}
	
	private final CcpJsonRepresentation errorDetails;

	private final CcpBulkItem bulkItem;
	
	private final Integer status;
	
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {

		String entityName = bulkItem.entity.getEntityName();
		String operationName = bulkItem.operation.name();
		CcpDbRequester dependency = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String fieldNameToEntity = dependency.getFieldNameToEntity();
		String fieldNameToId = dependency.getFieldNameToId();
		List<CcpJsonRepresentation> map = result.stream().map(x -> x.getDynamicVersion().getInnerJson(operationName)).collect(Collectors.toList());
		
		List<CcpJsonRepresentation> filteredById = map.stream().filter(x -> x.getDynamicVersion().getAsString(fieldNameToId).equals(bulkItem.id)).collect(Collectors.toList());
		
		if(filteredById.isEmpty()) {
			CcpOtherConstants.EMPTY_JSON
			.put(JsonFieldNames.entity, bulkItem.entity)
			.put(JsonFieldNames.id, bulkItem.id)
			.put(JsonFieldNames.json, bulkItem.json)
			.put(JsonFieldNames.filteredRecords, bulkItem.id)

			;
			
			throw new CcpErrorBulkItemNotFound(bulkItem);
		}
		Optional<CcpJsonRepresentation> findFirst = filteredById.stream()
		.filter(x -> x.getDynamicVersion().getAsString(fieldNameToEntity).equals(entityName))
		.findFirst();
		
		boolean idNotFoundInTheEntity = false == findFirst.isPresent();
		
		if(idNotFoundInTheEntity) {
			throw new CcpErrorBulkItemNotFound(bulkItem, result);
		}
		
		CcpJsonRepresentation details = findFirst.get();

		this.status = details.getAsIntegerNumber(JsonFieldNames.status); 
		this.errorDetails = details.getInnerJson(JsonFieldNames.error);
		this.bulkItem = bulkItem;
	}
	
	public CcpJsonRepresentation getErrorDetails() {
		return errorDetails;
	}

	public CcpBulkItem getBulkItem() {
		return bulkItem;
	}

	public boolean hasError() {
		boolean empty = this.errorDetails.isEmpty();
		return false == empty;
	}

	public int status() {
		return this.status;
	}

	
	public String toString() {
		CcpJsonRepresentation asMap = this.asMap();
		String string = asMap.toString();
		return string;
	}

	public CcpJsonRepresentation asMap() {
		CcpJsonRepresentation asMap = this.bulkItem.asMap();
		CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON
				.put(JsonFieldNames.bulkItem, asMap)
				.put(JsonFieldNames.status, this.status)
				.put(JsonFieldNames.errorDetails, this.errorDetails)
				;
		return put;
	}
}
