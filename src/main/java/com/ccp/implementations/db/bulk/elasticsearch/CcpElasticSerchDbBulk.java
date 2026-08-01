package com.ccp.implementations.db.bulk.elasticsearch;

import java.util.ArrayList;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkExecutor;

/**
 * Provedor de DI que cria e expõe uma instância de {@code ElasticSerchDbBulkExecutor}
 * como implementação de {@code CcpBulkExecutor}.
 */
public class CcpElasticSerchDbBulk implements CcpInstanceProvider<CcpBulkExecutor> {


	public CcpBulkExecutor getInstance() {
		ArrayList<CcpBulkItem> bulkItems = new ArrayList<>();
		return new ElasticSerchDbBulkExecutor(bulkItems);
	}

}
