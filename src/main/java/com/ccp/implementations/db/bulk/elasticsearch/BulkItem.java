package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityDetails;

class BulkItem {
	final String id;
	final String entity;
	final String content;

	public BulkItem(CcpBulkItem item) {

		String name = item.operation.name();
		BulkOperation valueOf = BulkOperation.valueOf(name);
		String content = valueOf.getContent(item);
		CcpEntityDetails entityDetails = item.entity.getEntityDetails();
		this.entity = entityDetails.entityName;
		this.content = content;
		this.id = item.id;
	}
	
	
	
	public String toString() {
		return "BulkItem [id=" + id + ", entity=" + entity + ", content=" + content + "]";
	}


	public int hashCode() {
		return (this.entity + this.id).hashCode();
	}
	
	
	public boolean equals(Object obj) {
		try {
			BulkItem other = (BulkItem)obj;
			
			boolean differentEntity = false == other.entity.equals(this.entity);
			
			if(differentEntity) {
				return false;
			}
			
			boolean differentId = false == other.id.equals(this.id);
			
			if(differentId) {
				return false;
			}
			return true;
		} catch (Exception e) {
			return false;
		}
	}

}
