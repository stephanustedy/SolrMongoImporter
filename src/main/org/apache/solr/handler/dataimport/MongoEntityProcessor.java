/**
 * Solr MongoDB data import handler
 *
 */
package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

/**
 * MongoDB entity processor
 *
 */
public class MongoEntityProcessor extends EntityProcessorBase {

	private static final Logger LOG = LoggerFactory.getLogger(EntityProcessorBase.class);

	/**
	 * MongoDB datasource
	 * 
	 */
	protected MongoDataSource dataSource;

	private String collection;

	/**
	 * Initialize
	 *
	 * @param context
	 */
	@Override
	public void init(Context context) {
		super.init(context);

		this.collection = context.getEntityAttribute(COLLECTION);

		if (this.collection == null) {
			throw new DataImportHandlerException(SEVERE, "Collection must be supplied");
		}
		this.dataSource = (MongoDataSource) context.getDataSource();
	}

	/**
	 * Initialize query
	 *
	 * @param q
	 */
	protected void initQuery(String q) {
		try {
			DataImporter.QUERY_COUNT.get().incrementAndGet();
			rowIterator = dataSource.getData(q, this.collection);
			this.query = q;
		} catch (DataImportHandlerException e) {
			throw e;
		} catch (Exception e) {
			LOG.error("The query failed '" + q + "'", e);
			throw new DataImportHandlerException(DataImportHandlerException.SEVERE, e);
		}
	}

	/**
	 * Get next row
	 *
	 * @return
	 */
	@Override
	public Map<String, Object> nextRow() {
		String q = context.getEntityAttribute(QUERY);

		if (rowIterator == null) {
			initQuery(context.replaceTokens(q));
		}
		return getNext();
	}

	/**
	 * Query option
	 * 
	 */
	public static final String QUERY = "query";
	
	/**
	 * Collection option
	 * 
	 */
	public static final String COLLECTION = "collection";

}
