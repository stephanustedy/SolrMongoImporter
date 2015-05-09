/**
 * Solr MongoDB data import handler
 *
 */
package org.apache.solr.handler.dataimport;

import java.util.Map;

/**
 * MongoDB mapper transformer
 *
 */
public class MongoMapperTransformer extends Transformer {

	/**
	 * Transform row
	 *
	 * @param row
	 * @param context
	 * @return
	 */
	@Override
	public Object transformRow(Map<String, Object> row, Context context) {

		for (Map<String, String> map : context.getAllEntityFields()) {
			String mongoFieldName = map.get(MONGO_FIELD);
			if (mongoFieldName == null) {
				continue;
			}

			String columnFieldName = map.get(DataImporter.COLUMN);

			row.put(columnFieldName, row.get(mongoFieldName));

		}

		return row;
	}

	/**
	 * MongoDB field option
	 * 
	 */
	public static final String MONGO_FIELD = "mongoField";

	/**
	 * Date format option
	 * 
	 */
	public static final String DATE_FORMAT = "dateFormat";
}
