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
import java.util.stream.Stream;/**
 * Representa o resultado de uma operação individual dentro de uma resposta bulk do Elasticsearch.
 * Localiza o item correspondente na lista de resultados pelo id e pelo nome da entidade,
 * expondo status HTTP, detalhes de erro e o {@code CcpBulkItem} original.
 */

class ElasticSearchBulkOperationResult implements CcpBulkOperationResult{
	enum JsonFieldNames implements CcpJsonFieldName{
		entity, id, json, filteredRecords, status, error, bulkItem, errorDetails
	}
	
	private final CcpJsonRepresentation errorDetails;

	private final CcpBulkItem bulkItem;
	
	private final Integer status;
	
	public ElasticSearchBulkOperationResult(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {

		CcpEntityMetaData entityDetails = bulkItem.entity.getEntityMetaData();
		String entityName = entityDetails.entityName;
		CcpDbRequester dependency = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String fieldNameToEntity = dependency.getFieldNameToEntity();
		String fieldNameToId = dependency.getFieldNameToId();
		Stream<CcpJsonRepresentation> stream = result.stream();
		var streamMap = stream.map(x -> x.getInnerJson(bulkItem.operation));
		List<CcpJsonRepresentation> map = streamMap.collect(Collectors.toList());
		Stream<CcpJsonRepresentation> stream2 = map.stream();
		var filter = stream2.filter(x -> x.getAsString(new CcpFieldName(fieldNameToId)).equals(bulkItem.id));

		List<CcpJsonRepresentation> filteredById = filter.collect(Collectors.toList());
		boolean filteredByIdEmpty = filteredById.isEmpty();

		if(filteredByIdEmpty) {
			CcpErrorBulkItemNotFound ccpErrorBulkItemNotFound = new CcpErrorBulkItemNotFound(bulkItem, result);

			throw ccpErrorBulkItemNotFound;
		}
		Stream<CcpJsonRepresentation> stream3 = filteredById.stream();
		var filter2 = stream3
		.filter(x -> x.getAsString(new CcpFieldName(fieldNameToEntity)).equals(entityName));
		Optional<CcpJsonRepresentation> findFirst = filter2
		.findFirst();
		boolean findFirstPresent = findFirst.isPresent();

		boolean idNotFoundInTheEntity = false == findFirstPresent;
		
		if(idNotFoundInTheEntity) {
			CcpErrorBulkItemNotFound ccpErrorBulkItemNotFound2 = new CcpErrorBulkItemNotFound(bulkItem, result);
			throw ccpErrorBulkItemNotFound2;
		}
		
		CcpJsonRepresentation details = findFirst.get();

		this.status = details.getAsIntegerNumber(JsonFieldNames.status); 
		this.errorDetails = details.getInnerJson(JsonFieldNames.error);
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
		boolean valorIgual = false == empty;
		return valorIgual;
	}

	public int status() {
		return this.status;
	}

	
	public String toString() {
		CcpJsonRepresentation asMap = this.bulkItem.asMap();
		CcpJsonRepresentation put2 = CcpOtherConstants.EMPTY_JSON
				.put(JsonFieldNames.bulkItem, asMap);
				CcpJsonRepresentation put3 = put2
				.put(JsonFieldNames.status, this.status);
				CcpJsonRepresentation put = put3
				.put(JsonFieldNames.errorDetails, this.errorDetails)
				;
		String string = put.toString();
		return string;
	}

	@SuppressWarnings("serial")
	public static class CcpErrorBulkItemNotFound extends RuntimeException {
		private CcpErrorBulkItemNotFound(CcpBulkItem bulkItem, List<CcpJsonRepresentation> result) {
			super( String.format("Id '%s' from entity '%s' not found. Complete list: " + result, bulkItem.id, bulkItem.entity.getEntityMetaData().entityName));
		}
	}
}
