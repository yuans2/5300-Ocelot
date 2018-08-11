#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!  TEMP using python code
        this->BtreeIndex(relation,name, unique);
        this->build_key_profile();
}

//Figure out the data types of each key component and encode them in self.key_profile,
//            a list of int/str classes.
void build_key_profile(){
    // FIXME TEMP using python code
    //check if  self.key in self.key_profile = [types_by_colname[column_name] for column_name in self.key]
    ColumnNames column_names = this->relation.get_column_names();
    ColumnAttributes column_attributes = this->relation.get_column_attributes();
    for (uint i = 0; i < column_names.size(); i++) {
        this->key_profile.push_back(column_attributes[i].get_data_type());
    }


}

BTreeIndex::~BTreeIndex() {
    delete(this->stat);
    delete(this->root);
    drop();
}

// Create the index.
void BTreeIndex::create() {
	// FIXME TEMP using python code
    this->file.create();
    this->stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
    this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
    this->closed= false;
    //build index , add every row from relation to index
    Handles* all_rows_handle= this->relation.select();
    for (auto const& handle : *all_rows_handle) {
        this->insert(handle);
    }
}

// Drop the index.
void BTreeIndex::drop() {
	// FIXME TEMP using python code
    this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
	// FIXME TEMP using python code
    if(this->closed){
        this->file.open();
        this->stat = new BTreeStat(this->file,this->STAT);
        if (this->stat->get_height() == 1)
            this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, false);
        else
            this->root = new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);

        this->closed= false;
    }
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
	// FIXME TEMP using python code
    this->file.close();
    this->stat = nullptr;
    this->root = nullptr;
    this->closed = true;


}
/*
 * LOOKUP
 * split into two sections, recursively search down the tree
 * */
// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	// FIXME TEMP using python code
    this->open();
    KeyValue* tkey_val= this->tkey(key_dict);
    Handles* handles = new Handles();
    Handle handle;
    handle= this->_lookup(this->root, this->stat->get_height(), _tKey);
    handles->push_back(handle);
    return handles;
}
Handle* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue *key) const {
   Handle* handle = new Handle();
    if(height==1){
        BTreeLeaf* leaf= node;
        handle= leaf->find_eq(key);
        return handle;
    }
    else{
        //BTree Node line210 - find verify , go down
        BTreeInterior* inter= node;
        this->lookup((inter->find(key, this->stat->get_height()), this->stat->get_height()-1, _tKey));
    }
}


//Finds all the rows whose columns are such that minkey <= columns <= maxkey.  Assumes key is a dictionary
//    whose keys are the column names in the index. Returns a list of row handles.
//Some index subclasses do not support range().
Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    //throw DbRelationError("Don't know how to do a range query on Btree index yet");



}

/*
 * INSERTION
 * need split root to add new root index
 * */
// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
	// FIXME TEMP using python code
	this->open();
	ValueDict* dict= this->relation->project(handle, this->key_columns);
	KeyValue* t_Key = this->tkey(dict);
	Insertion split_root = this->_insert(this->root,this->stat->get_height(),t_Key, handle);
    if(split_root != nullptr){
        split_root(split_root, this->root, this->stat->get_height());
	}

}
//recursively insert
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
    // Recursive insert. If a split happens at this level, return the (new node, boundary) of the split.
    Insertion insertion;
    if (height == 1)
        try{
            insertion = ((BTreeLeaf*)node)->insert(&key, handle);
        }
        catch (DbException &exe){
            //todo split_leaf()
        }

    else {
        Insertion new_kid = _insert(((BTreeInterior*)node)->find(key, height), height - 1, key, handle);
        if (BTreeNode::insertion_is_none(new_kid) == false){
            try{
                insertion = ((BTreeInterior*)node)->insert(&new_kid.second, new_kid.first);
            }
            catch (DbException &exe){
                //todo split_leaf()
            }

        }
    }
    return insertion;
}
//reformat root passed by insert
void split_root(Insertion split_root, BTreeNode* node, uint height ){
    BTreeInterior* root = new BTreeInterior(this->file, 0, this->key_profile, true);
    root->set_first(this->root->get_id());
    root->insert(split_root.second, split_root.first); //height/id
    root->save();
    this->stat->set_root_id(root->get_id());
    uint temp_height= this->stat->get_height() + 1;
    this->stat->set_height(temp_height);
    this->stat->save();
    this->root = root;

}
void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME TEMP using python code


    //tree never shrinks -- if all keys get deleted we still have an empty shell of tree
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {

	// FIXME TEMP using python code
    if(key == nullptr){
        return nullptr;
    }
    else{
        KeyValue* keyValue = new KeyValue();
        for(auto const& col: this->key_columns){
            //FIXME
            Value value = key->second;
            keyValue->push_back(value);
        }
    }
}




