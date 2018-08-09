/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
#include "EvalPlan.h"
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// Prints query results
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
					case ColumnAttribute::BOOLEAN:
						out << (value.n == 0 ? "false" : "true");
						break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

//Deconstructor
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

// Executes query statement
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();
    if (SQLExec::indices == nullptr)
        SQLExec::indices = new Indices();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}
ValueDict* SQLExec::get_where_conjunction(const Expr *expr){
    ValueDict* where = new ValueDict();

    if (expr->type == hsql::kExprOperator)
    {
        if (expr->opType == hsql::Expr::SIMPLE_OP &&  expr->opChar == '=')
        {
            Identifier identifier = expr->expr->name;

            switch (expr->expr2->type){
                case hsql::kExprLiteralString: {
                    (*where)[identifier] = Value(expr->expr2->name);
                    break;
                }
                case hsql::kExprLiteralInt: {
                    (*where)[identifier] = Value(int32_t(expr->expr2->ival));
                    break;
                }
                default:
                    throw DbRelationError("Not valid data type.");
                    break;
            }


        }
        //FIX AND
        }else if (expr->opType == hsql::Expr::AND) {
            ValueDict* left_where= get_where_conjunction(expr->expr);
            ValueDict* right_where= get_where_conjunction(expr->expr2);

            where->insert(left_where->begin(), left_where->end());
            where->insert(right_where->begin(), right_where->end());
            delete left_where;
            delete right_where;
        }
        else {
            throw  DbRelationError("Invalid where statement");
        }
    return where;
}



//Insert row into table
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    //get table name
    Identifier table_name = statement->tableName;
    //get table
    DbRelation& table = SQLExec::tables->get_table(table_name);
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    ValueDict row;
    uint i = 0;
    Handle insertHandle;
    //get column info
    if (statement->columns != nullptr) {
        for (auto const& col : *statement->columns) {
            column_names.push_back(col);
        }
    }
    else {
        for (auto const& col: table.get_column_names()) {
            column_names.push_back(col);
        }
    }
    //insert into row
    for (auto const& col : *statement->values) {
        switch (col->type) {
            case kExprLiteralString:
                row[column_names[i]] = Value(col->name);
                i++;
                break;
            case kExprLiteralInt:
                row[column_names[i]] = Value(col->ival);
                i++;
                break;
            default:
                return new QueryResult("Not valid data type");//shouldn't add

        }

    }
    //insert row to table
    insertHandle = table.insert(&row); //add insert to handle
    //get index names
    IndexNames index_names = SQLExec::indices->get_index_names(table_name);
    // index
    for (Identifier ind_name : index_names) {
        DbIndex& index = SQLExec::indices->get_index(table_name, ind_name);
        index.insert(insertHandle); // dummy for now
    }
    //query index info
    string has_indices= "";
    if(index_names.size() >= 1){
        has_indices = " and "+ to_string(index_names.size())+ " indices";
    }

    return new QueryResult("Successfully inserted 1 row into "
                           + table_name + has_indices);
}



// DELETE FROM ...
QueryResult *SQLExec::del(const DeleteStatement *statement)
{
    // return new QueryResult("DELETE statement not yet implemented"); // FIXME

    // get table name
    Identifier table = statement->tableName;

//    if (ensure_table_exist(table) == false)
//        throw SQLExecError(table + " does not exist");

    // get table and where clauses
    DbRelation &tb = SQLExec::tables->get_table(table);

    // make the evaluation plan
    EvalPlan *plan = new EvalPlan(tb);

    if (statement->expr != NULL)
        plan = new EvalPlan(get_where_conjunction(statement->expr), plan);

    // and execute it to get a list of handles

    EvalPlan *ep = plan->optimize();
    EvalPipeline pipeline = ep->pipeline();
    Handles *handles = pipeline.second;

    // remove from indices
    auto index_names = SQLExec::indices->get_index_names(table);

    u_long n = 0;
    u_long m = index_names.size();

    for (auto const &handle : *handles)
    {
        n++;
        for (auto const index_name : index_names)
        {
            DbIndex &index = SQLExec::indices->get_index(table, index_name);
            index.del(handle);
        }
        // remove from table
        tb.del(handle);
    }
    string has_indices= "";


    return new QueryResult("successfully deleted " + to_string(n)+ " rows from "+ table + to_string(m)+ " indices");
}



QueryResult *SQLExec::select(const SelectStatement *statement) {

    Identifier table_name = statement->fromTable->getName();
    DbRelation& table = SQLExec::tables->get_table(table_name);
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = table.get_column_attributes(*column_names);
    //stat base of plan at tablescan
    EvalPlan *plan = new EvalPlan(table);
    //
    //ValueDict where;
    if(statement->whereClause != nullptr){
        plan = new EvalPlan(get_where_conjunction(statement->whereClause), plan);
    }
    if (statement->selectList != nullptr){
        for (auto const& expr : *statement->selectList)
        {
            if (expr->type == kExprStar){
                ColumnNames get_column_names = table.get_column_names();

                for (auto const &col : get_column_names)
                    column_names->push_back(col);
            }else if (expr->type == kExprColumnRef)
                column_names->push_back(string(expr->name));

            else{
                    column_names->push_back(string(expr->name));

            }
        }
        plan = new EvalPlan(column_names, plan);

    }

    EvalPlan *optimized = plan->optimize();
    ValueDicts *rows = optimized->evaluate();

    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(rows->size()) + " rows");
}

// Defines data type of column, stores data identifier and attribute
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    column_name = col->name;
   
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("unrecognized data type");
    }
}

// Executes create statement for tables and indexes
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}
 
 // Creates table as defined by SQL statement
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
   
    ColumnNames column_names;
    ColumnAttributes column_attributes;
   
    Identifier column_name;
    ColumnAttribute column_attribute;

    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
  
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
  
    try {
        Handles c_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception& e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
           
            throw;
        }

    } catch (exception& e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
      
        throw;
    }
    return new QueryResult("created " + table_name);
}

// Creates index for specified table
QueryResult *SQLExec::create_index(const CreateStatement *statement) {

	Identifier index_name = statement->indexName;
	Identifier table_name = statement->tableName;

	DbRelation& table = SQLExec::tables->get_table(table_name);

	const ColumnNames& table_columns = table.get_column_names();

	for(auto const& col_name: *statement->indexColumns){
		if(find(table_columns.begin(), table_columns.end(),col_name) == table_columns.end()){
			throw SQLExecError(string("column '" + string(col_name) + "' does not exist"));
		}
	}

	ValueDict row;

	row["table_name"] = Value(table_name);
	row["index_name"] = Value(index_name);
	row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE");
	
	int seq = 0;

	Handles inHandles;

	try {
		for(auto const &col_name: *statement->indexColumns) {
			row["seq_in_index"] = Value(++seq);
			row["column_name"] = Value(col_name);
			inHandles.push_back(SQLExec::indices->insert(&row));
		}

		DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
		
		index.create();

	}catch(...){
		try {
			for(auto const &handle: inHandles){
				SQLExec::indices->del(handle);
			}
		}catch(...){}

		throw;
	}
   
	return new QueryResult("created index " + index_name);
    
}

// Executes drop of table or index
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}
 
// Drops a table from database
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
 
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME || 
         table_name==Indices::TABLE_NAME) {
        throw SQLExecError("cannot drop a schema table");
	}
 
    ValueDict where;
 
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);

    //remove any indexes on table
    IndexNames theseIndices = SQLExec::indices->get_index_names(table_name);

    if (!theseIndices.empty()) {
       for (auto const& index_name: theseIndices) { 
       	// This is what causes the warning when making, and is becuase the index.drop()
    	// function has been commented below.
        DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
     
         // remove from _indices schema
        Handles* handles = SQLExec::indices->select(&where);
  
        for (auto const& handle: *handles)
            SQLExec::indices->del(handle);
    
        // index.drop() causes segfault due to dummy function
        //index.drop();
        
        delete handles;
      }
    }
 
    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
 
    Handles* handles = columns.select(&where);
 
    for (auto const& handle: *handles)
        columns.del(handle);
 
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(string("dropped ") + table_name);
}

// Drops index from specified table
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;
 
    if(SQLExec::indices->get_index_names(table_name).empty())
		throw SQLExecError("index does not exist"); 
 
    ValueDict where;
 
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
   
    // get the table
    // This is what causes the warning when making, and is becuase the index.drop()
    // function has been commented below.
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
 
    // remove from _indices schema
    Handles* handles = SQLExec::indices->select(&where);
 
    for (auto const& handle: *handles)
        SQLExec::indices->del(handle);
 
    // index.drop() causes a segfault, likely due to it refering to a dummy function
    //index.drop();

    delete handles;

    return new QueryResult(string("dropped index ") + index_name);

}

// Executes show for tables, columns and indexes
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

// Returns index information for specified table
QueryResult *SQLExec::show_index(const ShowStatement *statement) {

    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
   
    where["table_name"] = Value(statement->tableName);
    Handles* handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
  
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::indices->project(handle, column_names); 
        rows->push_back(row);
    }
  
    delete handles;
  
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

// Returns tables in database
QueryResult *SQLExec::show_tables() {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts* rows = new ValueDicts;
 
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
     
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME &&
            table_name != Indices::TABLE_NAME) {
            rows->push_back(row);
    	}

    }

    delete handles;

    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

// Returns columns of a specified table
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
   
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        
        rows->push_back(row);
    }
  
    delete handles;
  
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

