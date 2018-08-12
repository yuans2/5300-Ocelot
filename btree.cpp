#include "btree.h"
using namespace std;

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
        build_key_profile();
}
//Build Profile
//Figure out the data types of each key component and encode them in self.key_profile,
//            a list of int/str classes.
void BTreeIndex::build_key_profile(){
    // FIXME TEMP using python code
    ColumnNames column_names = this->relation.get_column_names();
    ColumnAttributes column_attributes = this->relation.get_column_attributes();
    for (uint i = 0; i < column_names.size(); i++) {

        this->key_profile.push_back(column_attributes[i].get_data_type());

    }

}
//destructor
BTreeIndex::~BTreeIndex() {
    delete(this->stat);
    delete(this->root);
    drop();
}

// Create the index.
void BTreeIndex::create() {

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

    this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
    if(this->closed){
        this->file.open();
        this->stat = new BTreeStat(this->file,this->STAT, this->key_profile);
        if (this->stat->get_height() == 1)
            this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, false);
        else
            this->root = new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);

        this->closed= false;
    }
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
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

    KeyValue* tkey_val= this->tkey(key_dict);
    Handles* handles = new Handles();
    handles= this->_lookup(this->root, this->stat->get_height(), tkey_val);
    return handles;
}
Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue *key) const {
    //handles for lookup
    Handles* handles = new Handles();
    //get a handle
    Handle handle;
    if(height==1){
        handle = ((BTreeLeaf*)node)->find_eq(key);
        handles->push_back(handle);
        return handles;
    }
    else{
        BTreeInterior* inter= (BTreeInterior*)node;
        return this->_lookup((inter->find(key, this->stat->get_height())), this->stat->get_height()-1, key);
    }
}


//RANGE
Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
}

/*
 * INSERTION
 * need split root to add new root index
 * */
// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
	this->open();
	ValueDict* dict= this->relation.project(handle, &key_columns);
	KeyValue* t_Key = this->tkey(dict);
	Insertion split_root = this->_insert(this->root,this->stat->get_height(),t_Key, handle);
    if(!BTreeNode::insertion_is_none(split_root) ){
        //split_root(split_root_in, this->root, this->stat->get_height());
        BTreeInterior* root = new BTreeInterior(this->file, 0, this->key_profile, true);
        root->set_first(this->root->get_id());
        root->insert(&split_root.second, split_root.first); //height/id
        root->save();
        this->stat->set_root_id(root->get_id());
        uint temp_height= this->stat->get_height() + 1;
        this->stat->set_height(temp_height);
        this->stat->save();

        this->root = root;
	}

}
//recursively insert
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
    // Recursive insert. If a split happens at this level, return the (new node, boundary) of the split.
    Insertion insertion;
    if (height == 1)

        insertion = ((BTreeLeaf*)node)->insert(key, handle);

    else {
        Insertion new_insertion = _insert(((BTreeInterior*)node)->find(key, height), height - 1, key, handle);
        if (!BTreeNode::insertion_is_none(new_insertion)){

            insertion = ((BTreeInterior*)node)->insert(&new_insertion.second, new_insertion.first);

        }
    }
    return insertion;
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME TEMP using python code

}
KeyValue *BTreeIndex::tkey(const ValueDict *key) const {

        KeyValue* keyValue = new KeyValue();
        //get Value from key
        for(auto const col: *key){
            keyValue->push_back(col.second);
        }
        return keyValue;

}
//BTREE TESTING
bool test_btree(){

    Identifier testID= "test_btree";
    ColumnNames columnNames;
    columnNames.push_back("a");
    columnNames.push_back("b");
    ColumnAttributes colAttributes;
    ColumnAttribute ca;
    ColumnAttribute ca2;
    ca= ColumnAttribute::INT;
    ca2= ColumnAttribute::INT;
    colAttributes.push_back(ca);
    colAttributes.push_back(ca2);
    HeapTable table(testID, columnNames, colAttributes);
    table.create();
    //for inserts
    ValueDict row1;
    ValueDict row2;
    //For results
    ValueDict result_1;
    ValueDict result_2;
    ValueDict result_3;
    ValueDict result_4;

    //append values
    row1["a"] = Value(12);
    row1["b"] = Value(99);
    row2["a"] = Value(88);
    row2["b"] = Value(101);

    //test values
    result_1["a"]= Value(12);
    result_2["a"]= Value(88);
    result_3["a"]= Value(6);

    table.insert(&row1);
    table.insert(&row2);
    ValueDict row;
    ColumnNames columnNames2;
    columnNames2.push_back("a");
    columnNames2.push_back("b");
    for (uint i = 1; i < 1000; i++) {
        row["a"] = Value(i + 100);
        row["b"] = Value(-i);
        table.insert(&row);
    }
    DbIndex* index = new BTreeIndex(table, "test_btreeIndex", columnNames2, true);
    index->create();

    //test 1
    bool result=false;
    Handles* handles_t1 = new Handles();

    handles_t1 = index->lookup(&result_1);
    for(auto const& handle: *handles_t1){
        ValueDict* row_proj = table.project(handle);
        if((*row_proj)["a"] == result_1["a"] ){
            cout<<"pass t1"<<endl;
            result= true;
        }
        delete  row_proj;
    }
    if(!result){
        cout<<"failed t1" << endl;
    }
    result= false;
    delete  handles_t1;

    //t2
    result=false;
    Handles* handles_t2 = new Handles();
    handles_t2 = index->lookup(&result_2);
    for(auto const& handle: *handles_t2){
        ValueDict* row_proj = table.project(handle);
        if((*row_proj)["a"] == result_2["a"] ){
            cout<<"pass t2"<<endl;
            result= true;
        }
        delete  row_proj;
    }
    if(!result){
        cout<<"failed t2"<<endl;
    }
    result= false;
    delete  handles_t2;

    //t3
    result=false;
    Handles* handles_t3 = new Handles();
    handles_t3 = index->lookup(&result_3);
    for(auto const& handle: *handles_t3){
        ValueDict* row_proj = table.project(handle);
        if((*row_proj)["a"] == result_3["a"] ){
            cout<<"pass t3"<<endl;
            result= true;
        }
        delete  row_proj;
    }
    if(!result){
        cout<<"failed t3"<<endl;
    }
    result= false;
    delete  handles_t3;
//t4
    Handles* handles_t4 = new Handles();
    for (uint j = 0; j <10 ; j++) {
        for (uint i = 0; j < 1000; j++) {
            result_4["a"]= Value(i + 100);
            result_4["a"]= Value(-i);
             handles_t4 = index->lookup(&result_4);
            for (auto const& handle : *handles_t4) {
                ValueDict* row_proj = table.project(handle);
                if ((*row_proj)["a"] == result_4["a"] &&(*row_proj)["b"] == result_4["b"]) {
                    result = true;
                }
                delete row_proj;
            }

        }
    }
    if(!result){
        cout<<"failed t4"<<endl;
    }
    delete handles_t4;
    delete index;
    table.drop();
    return result;
}



