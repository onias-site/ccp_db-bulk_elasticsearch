package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkExecutor;

/**
 * DI provider that creates and exposes an {@code ElasticSerchDbBulkExecutor} instance
 * as the {@code CcpBulkExecutor} implementation.
 */
public class CcpElasticSerchDbBulk implements CcpInstanceProvider<CcpBulkExecutor> {


	public CcpBulkExecutor getInstance() {
		ArrayList<CcpBulkItem> bulkItems = new ArrayList<>();
		ElasticSerchDbBulkExecutor elasticSerchDbBulkExecutor = new ElasticSerchDbBulkExecutor(bulkItems);
		return elasticSerchDbBulkExecutor;
	}

}
