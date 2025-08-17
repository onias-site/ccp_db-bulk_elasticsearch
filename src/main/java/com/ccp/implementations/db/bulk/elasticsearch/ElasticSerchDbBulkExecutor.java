package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpMethods;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.implementations.db.bulk.elasticsearch.ElasticSerchDbBulkExecutorSpecialWords.JsonFieldNames;


enum ElasticSerchDbBulkExecutorSpecialWords implements CcpJsonFieldName{
	Content_Type("Content-Type"),
;
	static enum JsonFieldNames implements CcpJsonFieldName{
		items
	}
	private final String value;
	
	private ElasticSerchDbBulkExecutorSpecialWords(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}

}

class ElasticSerchDbBulkExecutor implements CcpDbBulkExecutor{
	
	private final List<CcpBulkItem> bulkItems;
	
	public ElasticSerchDbBulkExecutor(List<CcpBulkItem> bulkItems) {
		this.bulkItems = bulkItems;
	}

	public CcpDbBulkExecutor addRecord(CcpBulkItem bulkItem) {
		this.bulkItems.add(bulkItem);
		ElasticSerchDbBulkExecutor response = new ElasticSerchDbBulkExecutor(this.bulkItems);
		return response;
	}

	public List<CcpBulkOperationResult> getBulkOperationResult() {
		if(this.bulkItems.isEmpty()) {
			return new ArrayList<>();
		} 
		
		StringBuilder body = new StringBuilder();
		List<BulkItem> bulkItems = this.bulkItems.stream().map( x -> new BulkItem(x)).collect(Collectors.toList());
		for (BulkItem bulkItem : bulkItems) {
			body.append(bulkItem.content);
		}
		CcpJsonRepresentation headers = CcpOtherConstants.EMPTY_JSON.put(ElasticSerchDbBulkExecutorSpecialWords.Content_Type, "application/x-ndjson;charset=utf-8");
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpJsonRepresentation executeHttpRequest = dbUtils.executeHttpRequest("elasticSearchBulk", "/_bulk", CcpHttpMethods.POST, 200, body.toString(),  headers, CcpHttpResponseType.singleRecord);
		List<CcpJsonRepresentation> items = executeHttpRequest.getAsJsonList(JsonFieldNames.items);

		List<CcpBulkOperationResult> collect = this.bulkItems.stream().map(bulkItem -> new ElasticSearchBulkOperationResult(bulkItem, items)).collect(Collectors.toList());
		this.bulkItems.clear();
		return collect;
	}

	public CcpDbBulkExecutor clearRecords() {
		this.bulkItems.clear();
		ElasticSerchDbBulkExecutor response = new ElasticSerchDbBulkExecutor(this.bulkItems);
		return response;
	}

	
}
