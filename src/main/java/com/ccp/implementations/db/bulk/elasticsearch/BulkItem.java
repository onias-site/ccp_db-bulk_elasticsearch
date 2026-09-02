package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;

/**
 * Representa um item individual de operação em bulk para o Elasticsearch. Converte um CcpBulkItem
 * (abstrato) em sua representação de texto no formato NDJSON (Newline Delimited JSON) exigido pela
 * API _bulk do Elasticsearch.
 */
class BulkItem {
	final String id;
	final String entity;
	final String content;

	public BulkItem(CcpBulkItem item) {

		String name = item.operation.name();
		BulkOperation valueOf = BulkOperation.valueOf(name);
		String content = valueOf.getContent(item);
		CcpEntityMetaData entityDetails = item.entity.getEntityMetaData();
		this.entity = entityDetails.entityName;
		this.content = content;
		this.id = item.id;
	}
	
	
	
	public String toString() {
		String valorMais = "BulkItem [id=" + id;
		String valorMaisMais = valorMais + ", entity=";
		String valorMaisMaisMais = valorMaisMais + entity;
		String valorMaisMaisMaisMais = valorMaisMaisMais + ", content=";
		String valorMaisMaisMaisMaisMais = valorMaisMaisMaisMais + content;
		String valorMaisMaisMaisMaisMaisMais = valorMaisMaisMaisMaisMais + "]";
		return valorMaisMaisMaisMaisMaisMais;
	}


	public int hashCode() {
		String entityMais = this.entity + this.id;
		int hashCode = (entityMais).hashCode();
		return hashCode;
	}
	
	
	public boolean equals(Object obj) {
		try {
			BulkItem other = (BulkItem)obj;
			boolean entityEquals = other.entity.equals(this.entity);

			boolean differentEntity = false == entityEquals;
			
			if(differentEntity) {
				return false;
			}
			boolean idEquals = other.id.equals(this.id);

			boolean differentId = false == idEquals;
			
			if(differentId) {
				return false;
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
