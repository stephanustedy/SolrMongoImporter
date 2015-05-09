/**
 * Solr MongoDB data import handler
 * 
 */
package org.apache.solr.handler.dataimport;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.text.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import static org.apache.solr.handler.dataimport.MongoMapperTransformer.DATE_FORMAT;
import static org.apache.solr.handler.dataimport.MongoMapperTransformer.MONGO_FIELD;

/**
 * Solr MongoDB data source
 *
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

	private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);
	
	private MongoCollection mongoCollection;
	private MongoDatabase mongoDb;
	private MongoClient mongoClient;
	private MongoCursor mongoCursor;

	private Map<String, String> dateFields;
	
	/**
	 * Initialize
	 * 
	 * @param context
	 * @param initProps
	 */
	@Override
	public void init(Context context, Properties initProps) {
		
		String databaseName = initProps.getProperty(DATABASE);
		String host = initProps.getProperty(HOST, "localhost");
		String port = initProps.getProperty(PORT, "27017");
		String username = initProps.getProperty(USERNAME);
		String password = initProps.getProperty(PASSWORD);

		/**
		 * Collect date formatted fields
		 * 
		 */
		this.dateFields = new HashMap<String, String>();
		
		for (Map<String, String> fields : context.getAllEntityFields()) {
			String dateFormat = fields.get(DATE_FORMAT);
			String mongoField = fields.get(MONGO_FIELD);
			
			if (dateFormat != null && mongoField != null) {
				this.dateFields.put(mongoField, dateFormat);
			}
			
		}

		if (databaseName == null) {
			throw new DataImportHandlerException(SEVERE, "Database must be supplied");
		}

		try {
			if (username != null) {
				MongoCredential credential = MongoCredential.createCredential(username, databaseName, password.toCharArray());

				this.mongoClient = new MongoClient(new ServerAddress(host, Integer.parseInt(port)), Arrays.asList(credential));
			} else {
				this.mongoClient = new MongoClient(new ServerAddress(host, Integer.parseInt(port)));
			}

			this.mongoClient.setReadPreference(ReadPreference.secondaryPreferred());

			this.mongoDb = this.mongoClient.getDatabase(databaseName);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new DataImportHandlerException(SEVERE, "Unable to connect to Mongo");
		}
	}

	/**
	 * Get data by query
	 * 
	 * @param query
	 * @return 
	 */
	@Override
	public Iterator<Map<String, Object>> getData(String query) {

		BasicDBObject queryObject = (BasicDBObject) JSON.parse(query);

		mongoCursor = this.mongoCollection.find(queryObject).iterator();

		ResultSetIterator resultSet = new ResultSetIterator(mongoCursor, this.dateFields);
		return resultSet.getIterator();
	}

	/**
	 * Get data
	 * 
	 * @param query
	 * @param collection
	 * @return 
	 */
	public Iterator<Map<String, Object>> getData(String query, String collection) {
		this.mongoCollection = this.mongoDb.getCollection(collection);
		return getData(query);
	}

	/**
	 * Result set iterator
	 * 
	 */
	private class ResultSetIterator {

		MongoCursor mongoCursor;

		Map<String, String> dateFields;
		
		Iterator<Map<String, Object>> resultSetIterator;

		public ResultSetIterator(MongoCursor mongoCursor, Map<String, String> dateFields) {
			this.mongoCursor = mongoCursor;
			this.dateFields = dateFields;

			resultSetIterator = new Iterator<Map<String, Object>>() {
				@Override
				public boolean hasNext() {
					return hasnext();
				}

				@Override
				public Map<String, Object> next() {
					if (!hasNext()) {
						throw new NoSuchElementException();
					}
					return getARow();
				}

				@Override
				public void remove() {

				}
			};

		}

		/**
		 * Get iterator
		 * 
		 * @return 
		 */
		public Iterator<Map<String, Object>> getIterator() {
			return resultSetIterator;
		}

		/**
		 * Get a data row
		 * 
		 * @return 
		 */
		private Map<String, Object> getARow() {
			Document document = (Document) getMongoCursor().next();

			Map<String, Object> result = new HashMap<>();

			Set<String> keys = getDocumentKeys(document, null);

			Iterator<String> keysIterator = keys.iterator();

			while (keysIterator.hasNext()) {
				String key = keysIterator.next();

				Object innerObject = getDocumentFieldValue(document, key);

				result.put(key, innerObject);
			}

			return result;
		}

		/**
		 * Get keys of a document
		 * 
		 * @param doc
		 * @param parentKey
		 * @return 
		 */
		private Set<String> getDocumentKeys(Document doc, String parentKey) {
			Set<String> keys = new HashSet<>();

			Set<String> docKeys = doc.keySet();

			Iterator<String> docKeysIterator = docKeys.iterator();

			while (docKeysIterator.hasNext()) {
				String docKey = docKeysIterator.next();
				
				if (doc.get(docKey) instanceof Document) {
					String parent;

					if (parentKey == null) {
						parent = docKey;
					} else {
						parent = parentKey + "." + docKey;
					}

					Set<String> subKeys = getDocumentKeys((Document) doc.get(docKey), parent);

					Iterator<String> subKeysIterator = subKeys.iterator();

					while (subKeysIterator.hasNext()) {
						keys.add(subKeysIterator.next());
					}
				} else if (doc.get(docKey) instanceof ArrayList) {
					ArrayList<Object> list = (ArrayList) doc.get(docKey);

					Iterator<Object> listIterator = list.iterator();

					Integer i = 0;

					while (listIterator.hasNext()) {
						String parent;

						if (parentKey == null) {
							parent = docKey + "." + i.toString();
						} else {
							parent = parentKey + "." + docKey + "." + i.toString();
						}

						Object listItem = listIterator.next();

						if (listItem instanceof Document || listItem instanceof ArrayList) {
							Set<String> subKeys = getDocumentKeys((Document) listItem, parent);

							Iterator<String> subKeysIterator = subKeys.iterator();

							while (subKeysIterator.hasNext()) {
								keys.add(subKeysIterator.next());
							}
						}

						i++;
					}
				} else {
					if (parentKey != null) {
						keys.add(parentKey + "." + docKey);
					} else {
						keys.add(docKey);
					}
				}
			}
			return keys;
		}

		/**
		 * Get a document filed value by field name
		 * 
		 * @param object
		 * @param fieldName
		 * @return 
		 */
		private Object getDocumentFieldValue(Document object, String fieldName) {

			final String[] fieldParts = fieldName.split("\\.");

			int i = 1;
			Object value = object.get(fieldParts[0]);

			while (i < fieldParts.length && (value instanceof Document || value instanceof ArrayList)) {
				if (value instanceof ArrayList) {
					value = ((ArrayList) value).get(Integer.parseInt(fieldParts[i]));
				} else {
					value = ((Document) value).get(fieldParts[i]);
				}
				i++;
			}
			
			/**
			 * Convert date to format required by Solr
			 * 
			 */
			if (!this.dateFields.isEmpty() && this.dateFields.containsKey(fieldName) && value instanceof String) {
				try {
					DateFormat dateFormat = new SimpleDateFormat(this.dateFields.get(fieldName));
					
					Date date = dateFormat.parse(value.toString());
				
					DateFormat solrDateFormat = new SimpleDateFormat(SOLR_DATE_FORMAT);
					
					value = solrDateFormat.format(date);
				} catch (Exception e) {
					LOG.warn("Date convertion error", e);
					
					value = null;
				}
			}

			return value;
		}
		
		/**
		 * Check if result set has next record
		 * 
		 * @return 
		 */
		private boolean hasnext() {
			if (mongoCursor == null) {
				return false;
			}
			try {
				if (mongoCursor.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			} catch (MongoException e) {
				close();
				wrapAndThrow(SEVERE, e);
				return false;
			}
		}
		
		/**
		 * Close cursor
		 * 
		 */
		private void close() {
			try {
				if (mongoCursor != null) {
					mongoCursor.close();
				}
			} catch (Exception e) {
				LOG.warn("Exception while closing result set", e);
			} finally {
				mongoCursor = null;
			}
		}
	}

	/**
	 * Get MongoDB cursor
	 * 
	 * @return 
	 */
	private MongoCursor getMongoCursor() {
		return this.mongoCursor;
	}

	/**
	 * Close
	 * 
	 */
	@Override
	public void close() {
		if (this.mongoCursor != null) {
			this.mongoCursor.close();
		}

		if (this.mongoClient != null) {
			this.mongoClient.close();
		}
	}

	/**
	 * Database option
	 * 
	 */
	public static final String DATABASE = "database";
	
	/**
	 * Host option
	 * 
	 */
	public static final String HOST = "host";
	
	/**
	 * Port option
	 * 
	 */
	public static final String PORT = "port";
	
	/**
	 * Username option
	 * 
	 */
	public static final String USERNAME = "username";
	
	/**
	 * Password option
	 * 
	 */
	public static final String PASSWORD = "password";
	
	/**
	 * Solr date format
	 * 
	 */
	public static final String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	
}
