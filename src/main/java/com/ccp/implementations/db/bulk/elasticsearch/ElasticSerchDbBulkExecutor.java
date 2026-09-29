package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkExecutor;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpMethods;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.implementations.db.bulk.elasticsearch.ElasticSerchDbBulkExecutorSpecialWords.JsonFieldNames;
import java.util.stream.Stream;




/**
 * {@code CcpBulkExecutor} implementation that accumulates bulk items and sends them in a single
 * HTTP {@code POST /_bulk} request to Elasticsearch in NDJSON format.
 */
class ElasticSerchDbBulkExecutor implements CcpBulkExecutor{
	
	private final List<CcpBulkItem> bulkItems;
	
	public ElasticSerchDbBulkExecutor(List<CcpBulkItem> bulkItems) {
		this.bulkItems = bulkItems;
	}

	public CcpBulkExecutor addRecord(CcpBulkItem bulkItem) {
		ArrayList<CcpBulkItem> bulkItems = new ArrayList<>(this.bulkItems);
		bulkItems.add(bulkItem);
		ElasticSerchDbBulkExecutor response = new ElasticSerchDbBulkExecutor(bulkItems);
		return response;
	}

	public List<CcpBulkOperationResult> getBulkOperationResult() {
		boolean bulkItemsEmpty = this.bulkItems.isEmpty();
		if(bulkItemsEmpty) { 
			return new ArrayList<>();
		} 
		
		StringBuilder body = new StringBuilder();
		Stream<CcpBulkItem> bulkItemsStream = this.bulkItems.stream();
		var ndjsonItemsStream = bulkItemsStream.map( x -> new BulkItem(x));
		List<BulkItem> bulkItems = ndjsonItemsStream.collect(Collectors.toList());
		for (BulkItem bulkItem : bulkItems) {
			body.append(bulkItem.content);
		}
		CcpJsonRepresentation headers = CcpOtherConstants.EMPTY_JSON.put(ElasticSerchDbBulkExecutorSpecialWords.Content_Type, "application/x-ndjson;charset=utf-8");
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		String requestBody = body.toString();
		CcpJsonRepresentation bulkResponse = dbUtils.executeHttpRequest("elasticSearchBulk", "/_bulk", CcpHttpMethods.POST, 200, requestBody,  headers, CcpHttpResponseType.singleRecord);
		List<CcpJsonRepresentation> items = bulkResponse.getAsJsonList(JsonFieldNames.items);
		var bulkItemsCopyStream = new ArrayList<>(this.bulkItems).stream();
		var operationResultsStream = bulkItemsCopyStream.map(bulkItem -> new ElasticSearchBulkOperationResult(bulkItem, items));

		List<CcpBulkOperationResult> operationResults = operationResultsStream.collect(Collectors.toList());
		synchronized (String.class) {
			this.bulkItems.clear();
		}
		return operationResults;
	}

	public CcpBulkExecutor clearRecords() {
		ElasticSerchDbBulkExecutor response = new ElasticSerchDbBulkExecutor(new ArrayList<>());
		return response;
	}

	
}
