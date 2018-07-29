/*
 * Abstract base class of Index Relation
 * @author Kevin Lundeen
 *
 */

class DbIndex {
public:
	/**
	 * Maximum number of columns in a composite index
	 */
    static const uint MAX_COMPOSITE = 32U;

	// ctor/dtor
    DbIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
            : relation(relation), name(name), key_columns(key_columns), unique(unique) {}
    virtual ~DbIndex() {}

	/**
	 * Create this index.
	 */
    virtual void create() = 0;

	/**
	 * Drop this index.
	 */
    virtual void drop() = 0;

	/**
	 * Open this index.
	 */
    virtual void open() = 0;

	/**
	 * Close this index.
	 */
    virtual void close() = 0;

	/**
	 * Lookup a specific search key.
	 * @param key_values  dictionary of values for the search key
	 * @returns           list of DbFile handles for records with key_values
	 */
    virtual Handles* lookup(ValueDict* key_values) const = 0;

	/**
	 * Lookup a range of search keys.
	 * @param min_key  dictionary of min (inclusive) search key
	 * @param max_key  dictionary of max (inclusive) search key
	 * @returns        list of DbFile handles for records in range
	 */
    virtual Handles* range(ValueDict* min_key, ValueDict* max_key) const {
        throw DbRelationError("range index query not supported");
    }

	/**
	 * Insert the index entry for the given record.
	 * @param record  handle (into relation) to the record to insert
	 *                (must be in the relation at time of insertion)
	 */
    virtual void insert(Handle record) = 0;

	/**
	 * Delete the index entry for the given record.
	 * @param record  handle (into relation) to the record to remove
	 *                (must still be in the relation at time of removal)
	 */
    virtual void del(Handle record) = 0;

protected:
    DbRelation& relation;
    Identifier name;
    ColumnNames key_columns;
    bool unique;
};
