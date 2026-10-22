
package com.ccp.implementations.db.bulk.elasticsearch;

import com.ccp.constants.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkScriptedUpdateFields;
import com.ccp.especifications.db.utils.entity.decorators.engine.CcpEntityMetaData;


import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * Enum representing the Elasticsearch bulk operations ({@code delete}, {@code update},
 * {@code create}, {@code scriptedUpdate}). Each constant produces the action line and the second line of the NDJSON
 * pair for its operation via {@code getContent(CcpBulkItem)}.
 */
enum BulkOperation implements CcpJsonFieldName{
	/** {@code delete} action: no document line. */
	delete {

		/**
		 * No document line for a delete.
		 * @param json the record
		 * @return an empty text
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			return "";
		}
	},
	/**
	 * {@code update} action: the document line is {@code {"doc": record}} (partial update). Elasticsearch reads the
	 * document and writes it back with the version it read, so two updates of the same document at the same moment made
	 * the second fail with a version conflict (the copies of the disposable records, saved by concurrent writes of the
	 * same record, until 2026-10-21); the conflict is now retried by Elasticsearch itself ({@code retry_on_conflict}),
	 * which only applies the same partial document again.
	 */
	update {

		/**
		 * Wraps the record in {@code doc}.
		 * @param json the record
		 * @return the compact JSON line
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			CcpJsonRepresentation jsonWithDoc = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.doc, json);
			String asUgglyJson = jsonWithDoc.asUgglyJson();
			return asUgglyJson;
		}

		/**
		 * Asks Elasticsearch to retry the update when another update of the same document got there first.
		 * @param actionLine the action line
		 * @return the action line with {@code retry_on_conflict}
		 */
		CcpJsonRepresentation addActionOptions(CcpJsonRepresentation actionLine) {
			CcpJsonRepresentation actionLineWithRetries = actionLine.addToItem(this, JsonFieldNames.retry_on_conflict, RETRIES_ON_CONFLICT);
			return actionLineWithRetries;
		}
	},
	/** {@code create} action: the document line is the record itself. */
	create {

		/**
		 * Returns the record as a compact JSON line.
		 * @param json the record
		 * @return the compact JSON line
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			String asUgglyJson = json.asUgglyJson();
			return asUgglyJson;
		}
	},
	/**
	 * Atomic update by a Painless script: an {@code update} action whose document line is
	 * {@code {"script": {"source", "lang", "params"}, "upsert", "scripted_upsert"}}. Elasticsearch applies the updates of
	 * the same document one after the other; a version conflict is retried by Elasticsearch itself
	 * ({@code retry_on_conflict}).
	 */
	scriptedUpdate {

		/**
		 * Builds the script line from the fields of {@link CcpBulkScriptedUpdateFields}.
		 * @param json the script, its parameters, the upsert and whether the script also runs on the upsert
		 * @return the compact JSON line
		 */
		String getSecondLine(CcpJsonRepresentation json) {
			String source = json.getAsString(CcpBulkScriptedUpdateFields.script);
			CcpJsonRepresentation params = json.getInnerJson(CcpBulkScriptedUpdateFields.params);
			CcpJsonRepresentation upsert = json.getInnerJson(CcpBulkScriptedUpdateFields.upsert);
			boolean scriptedUpsert = json.getAsBoolean(CcpBulkScriptedUpdateFields.scriptedUpsert);
			String language = JsonFieldNames.painless.name();
			CcpJsonRepresentation scriptWithSource = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.source, source);
			CcpJsonRepresentation scriptWithLanguage = scriptWithSource.put(JsonFieldNames.lang, language);
			CcpJsonRepresentation script = scriptWithLanguage.put(JsonFieldNames.params, params);
			CcpJsonRepresentation lineWithScript = CcpOtherConstants.EMPTY_JSON.put(JsonFieldNames.script, script);
			CcpJsonRepresentation lineWithUpsert = lineWithScript.put(JsonFieldNames.upsert, upsert);
			CcpJsonRepresentation line = lineWithUpsert.put(JsonFieldNames.scripted_upsert, scriptedUpsert);
			String asUgglyJson = line.asUgglyJson();
			return asUgglyJson;
		}

		/**
		 * The Elasticsearch action of a scripted update is {@code update}.
		 * @return {@code update}
		 */
		CcpJsonFieldName getAction() {
			return update;
		}

		/**
		 * Asks Elasticsearch to retry the script when another update of the same document got there first.
		 * @param actionLine the action line
		 * @return the action line with {@code retry_on_conflict}
		 */
		CcpJsonRepresentation addActionOptions(CcpJsonRepresentation actionLine) {
			CcpJsonFieldName action = this.getAction();
			CcpJsonRepresentation actionLineWithRetries = actionLine.addToItem(action, JsonFieldNames.retry_on_conflict, RETRIES_ON_CONFLICT);
			return actionLineWithRetries;
		}
	}
	;
	/** The platform line separator, used to end the NDJSON lines. */
	static final String NEW_LINE = System.getProperty(BulkOperationSpecialWords.line_separator.getValue());

	/** How many times Elasticsearch retries an update (scripted or partial) that lost a version conflict. */
	static final int RETRIES_ON_CONFLICT = 10;

	/**
	 * Builds the two NDJSON lines of the item: the action line and the document line, each ended by {@link #NEW_LINE}.
	 * @param item the bulk item
	 * @return the NDJSON content
	 */
	public String getContent(CcpBulkItem item) {

		String firstLine = this.getFirstLine(item);

		String secondLine = this.getSecondLine(item.json);
		String firstLineWithNewLine = firstLine + NEW_LINE;
		String bothLines = firstLineWithNewLine + secondLine;

		String content = bothLines + NEW_LINE;

		return content;
	}

	/**
	 * Builds the action line: {@code {"<action>": {"_index": ..., "_id": ...}}}, plus the options of the operation.
	 * @param item the bulk item
	 * @return the compact JSON line
	 */
	private String getFirstLine(CcpBulkItem item) {
		CcpEntityMetaData entityDetails = item.entity.getEntityMetaData();
		String entityName = entityDetails.entityName;
		CcpJsonFieldName action = this.getAction();
		CcpJsonRepresentation actionWithIndex = CcpOtherConstants.EMPTY_JSON
				.addToItem(action, CcpJsonCommonsFields._index, entityName);
				CcpJsonRepresentation actionWithIndexAndId = actionWithIndex
				.addToItem(action, CcpJsonCommonsFields._id, item.id);
		CcpJsonRepresentation actionLine = this.addActionOptions(actionWithIndexAndId);
		String firstLine = actionLine.asUgglyJson();
		return firstLine;
	}

	/**
	 * The Elasticsearch action of the operation, which is also the key of its result in the bulk response.
	 * @return the operation itself, by default
	 */
	CcpJsonFieldName getAction() {
		return this;
	}

	/**
	 * Adds the options of the operation to the action line.
	 * @param actionLine the action line
	 * @return the action line itself, by default
	 */
	CcpJsonRepresentation addActionOptions(CcpJsonRepresentation actionLine) {
		return actionLine;
	}

	/**
	 * Builds the document line of the operation.
	 * @param json the record
	 * @return the document line
	 */
	abstract String getSecondLine(CcpJsonRepresentation json);

	/** Fields of the lines of the operations. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The partial document of an update. */
		doc,
		/** The script of a scripted update. */
		script,
		/** The source of the script. */
		source,
		/** The language of the script. */
		lang,
		/** The Painless language, the only one used. */
		painless,
		/** The parameters of the script. */
		params,
		/** The document created when there is none. */
		upsert,
		/** Whether the script also runs over the upsert. */
		scripted_upsert,
		/** How many times a version conflict is retried. */
		retry_on_conflict
	}
}
