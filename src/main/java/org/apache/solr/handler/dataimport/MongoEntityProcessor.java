/**
 * Solr MongoDB data import handler
 *
 */
package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
			q = replaceDateTimeToISODateTime(q);
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
		if (rowIterator == null) {
			String q = getQuery();
			initQuery(context.replaceTokens(q));
		}
		return getNext();
	}

	private static final Pattern Date_PATTERN = Pattern.compile("(\\d{4}[-|\\\\/]\\d{1,2}[-|\\\\/]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2})");
    private String replaceDateTimeToISODateTime(String s){
        Matcher m = Date_PATTERN.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String srcStr = m.group(1);
            String dstStr = srcStr.substring(0, 10) + "T" + srcStr.substring(11) +"Z";
            m.appendReplacement(sb, dstStr);
        }
        m.appendTail(sb);
        return sb.toString();
    }
    
	public String getQuery() {
        String queryString = context.getEntityAttribute(QUERY);
        if (Context.FULL_DUMP.equals(context.currentProcess())) {
            return queryString;
        }
        if (Context.DELTA_DUMP.equals(context.currentProcess()) || Context.FIND_DELTA.equals(context.currentProcess())) {
            String deltaImportQuery = context.getEntityAttribute(DELTA_IMPORT_QUERY);
            if (deltaImportQuery != null) return getDeltaImportQuery(deltaImportQuery);
        }
        LOG.warn("'deltaImportQuery' attribute is not specified for entity : " + entityName);
        return getDeltaImportQuery(queryString);
    }

    public String getDeltaImportQuery(String queryString) {
        return queryString;
    }

    public static final String COLLECTION = "collection";

    public static final String QUERY = "query";

    public static final String DELTA_QUERY = "deltaQuery";

    public static final String DELTA_IMPORT_QUERY = "deltaImportQuery";

}
