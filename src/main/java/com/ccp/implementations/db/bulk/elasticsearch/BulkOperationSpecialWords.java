package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.decorators.CcpJsonFieldName;

/**
 * Chaves de propriedade do sistema usadas por {@code BulkOperation} cujo valor real contém
 * ponto e por isso não pode ser escrito como identificador Java. Mesmo padrão de
 * {@code ElasticSerchDbBulkExecutorSpecialWords}: nome legal na constante, valor de verdade
 * no construtor, exposto por {@code getValue()}.
 */
enum BulkOperationSpecialWords implements CcpJsonFieldName {
	line_separator("line.separator"),
;
	private final String value;

	private BulkOperationSpecialWords(String value) {
		this.value = value;
	}

	public String getValue() {
		return this.value;
	}
}
