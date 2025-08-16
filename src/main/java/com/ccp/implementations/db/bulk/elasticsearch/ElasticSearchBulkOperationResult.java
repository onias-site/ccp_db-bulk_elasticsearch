package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.exceptions.db.bulk.CcpErrorDbBulkItemNotFound;
enum ElasticSearchBulkOperationResultConstants  implements CcpJsonFieldName{
	entity, id, json, filteredRecords, status, error, bulkItem, errorDetails
	
}
class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	
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
			.put(ElasticSearchBulkOperationResultConstants.entity, bulkItem.entity)
			.put(ElasticSearchBulkOperationResultConstants.id, bulkItem.id)
			.put(ElasticSearchBulkOperationResultConstants.json, bulkItem.json)
			.put(ElasticSearchBulkOperationResultConstants.filteredRecords, bulkItem.id)

			;
			
			throw new CcpErrorDbBulkItemNotFound(bulkItem);
		}
		Optional<CcpJsonRepresentation> findFirst = filteredById.stream()
		.filter(x -> x.getDynamicVersion().getAsString(fieldNameToEntity).equals(entityName))
		.findFirst();
		
		boolean idNotFoundInTheEntity = findFirst.isPresent() == false;
		
		if(idNotFoundInTheEntity) {
			throw new CcpErrorDbBulkItemNotFound(bulkItem, result);
		}
		
		CcpJsonRepresentation details = findFirst.get();

		this.status = details.getAsIntegerNumber(ElasticSearchBulkOperationResultConstants.status); 
		this.errorDetails = details.getInnerJson(ElasticSearchBulkOperationResultConstants.error);
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
		return empty == false;
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
				.put(ElasticSearchBulkOperationResultConstants.bulkItem, asMap)
				.put(ElasticSearchBulkOperationResultConstants.status, this.status)
				.put(ElasticSearchBulkOperationResultConstants.errorDetails, this.errorDetails)
				;
		return put;
	}
}
