/**
 * @file helper.cpp - helper function definitions to execute SQL
 * @see "Seattle University, cpsc4300/5300, summer 2018"
 */

#include <sstream>
#include "helper.h"
#include<iostream>

using namespace std;
/*
 * Translate Operator Expression
 *
 * Format: expression->expr OPERATOR expression->expr2
 * 
 * e.g. expr < expr2
 *      expr AND expr2
 *      expr OR expr2
 *      NOT expr
 *
 * @param expression Expr object
 * @return a string of SQL statement
 */
using namespace std;

std::string translate_operator_expression(const hsql::Expr* expression)
{
	if (expression == NULL)
	{
		return "";
	}

	std::ostringstream string_stream;

	switch(expression->opType)
	{
		case hsql::Expr::SIMPLE_OP:
		{
			string_stream << translate_expression(expression->expr) << " " << expression->opChar << " ";
			break;
		}
		case hsql::Expr::AND:
		{
			string_stream << translate_expression(expression->expr) << " " << "AND ";
			break;
		}
		case hsql::Expr::OR:
		{
			string_stream << translate_expression(expression->expr) << " " << "OR ";
			break;
		}
		case hsql::Expr::NOT:
		{
			string_stream << "NOT " << translate_expression(expression->expr);
			break;
		}
		default:
		{
			string_stream << "Unknown opType: " << expression->opType;
			break;
		}
	}

	if (expression->expr2 != NULL)
	{
		string_stream << translate_expression(expression->expr2);
	}

	return string_stream.str();
}

/*
 * Translate Expr expression
 *
 * Expression can be:
 *
 *   *: SELECT *
 *   tableName.columnName: foo.x
 *   LiteralString: "FOO"
 *   LiteralInt: 1
 *   ExpressionOperator: f.id = g.id
 *
 * @param expression EXPR object
 * @return a string of SQL statement  
 *
*/
std::string translate_expression(const hsql::Expr* expression)
{
	std::ostringstream string_stream;

	switch(expression->type)
	{
		/* 
		 * kExprStar
		 * e.g. SELECT * FROM ...
		 */
		case hsql::kExprStar:
		{
			string_stream << "*";
			break;
		}
		/*
		 * kExprColumnRef:
		 * e.g. SELECT a, b, g.c FROM ...
		 */
		case hsql::kExprColumnRef:
		{
			if (expression->table != NULL)
			{
				string_stream << expression->table << ".";
			}

			string_stream << expression->name;
			break;

		}
		case hsql::kExprLiteralString:
		{
			string_stream << expression->name;
			break;
		}
		case hsql::kExprLiteralFloat:
		{
			string_stream << expression->fval;
			break;
		}
		case hsql::kExprLiteralInt:
		{
			string_stream << expression->ival;
			break;
		}
		case hsql::kExprOperator:
		{
			string_stream << translate_operator_expression(expression);
			break;
		}
		default:
		{
			string_stream << "Unsupported expression type:" << expression->type;
		}
	}

	if (expression->alias != NULL)
	{
		string_stream << "AS " << expression->alias;
	}

	return string_stream.str();
}

/*
 * Translate Join Definition
 *
 * joinDefinition->left JOINTYPE joinDefinition->right ON joinDefinition->condition
 *
 * @param join_definition JoinDefinition object
 * @return a string of SQL statement
 */
std::string translate_join(const hsql::JoinDefinition* join_definition)
{
	std::ostringstream string_stream;

	std::string left_table = translate_table_ref_info(join_definition->left);
	std::string right_table = translate_table_ref_info(join_definition->right);

	string_stream << left_table << " ";

	switch(join_definition->type)
	{
		case hsql::kJoinInner:
		{
			string_stream << "JOIN ";
			break;
		}
		case hsql::kJoinOuter:
		{
			string_stream << "OUTER JOIN ";
			break;
		}
		case hsql::kJoinLeft:
		{
			string_stream << "LEFT JOIN ";
			break;
		}
		case hsql::kJoinRight:
		{
			string_stream << "RIGHT JOIN ";
			break;
		}
		case hsql::kJoinLeftOuter:
		{
			string_stream << "LEFT OUTER JOIN ";
			break;
		}
		case hsql::kJoinRightOuter:
		{
			string_stream << "RIGHT OUTER JOIN ";
			break;
		}
		case hsql::kJoinCross:
		{
			string_stream << "CROSS JOIN ";
			break;
		}
		default:
		{
			string_stream << "UNKNOWN JOIN ";
			break;
		}
	}

	string_stream << translate_table_ref_info(join_definition->right) << " ";
	string_stream << "ON " << translate_expression(join_definition->condition);

	return string_stream.str();
}

/*
 * Translate TableRefInfo
 *
 *@param table TableRef object
 *@return a string of the SQL statement 
 */

std::string translate_table_ref_info(const hsql::TableRef* table)
{
	std::ostringstream string_stream;

	switch(table->type)
	{
		case hsql::kTableName:
		{
			string_stream << table->name;
			break;
		}
		case hsql::kTableSelect:
		{
			string_stream << translate_select(table->select);
			break;
		}
		case hsql::kTableJoin:
		{
			string_stream << translate_join(table->join);
			break;
		}
		case hsql::kTableCrossProduct:
		{
			for(uint i = 0; i < table->list->size() - 1; i++)
			{
				string_stream << translate_table_ref_info(table->list->at(i)) << ", ";
			}

			string_stream << translate_table_ref_info(table->list->back());
			break;
		}
		default:
		{
			string_stream << "Unsupported TableRef: " << table->type;
		}
	}

	if (table->alias != NULL)
	{
		string_stream << " AS " << table->alias;
	}

	return string_stream.str();
}


/*
 * Translate SELECT statement
 *
 * SELECT <# selectStatement->(Expr)selectList >
 * FROM <# selectStatement->(TableRef)fromTable >
 * WHERE <# selectStatement->(Expr)whereClause >
 *
 * @param select_statement SelectStatement object
 * @return a string of SQL statement
 *
*/
std::string translate_select(const hsql::SelectStatement* select_statement)
{
	std::ostringstream string_stream;

	// SELECT
	string_stream << "SELECT ";

	// Deal with one (no ',') or multiple elements (with ',') in selectList
	for (uint i = 0; i < select_statement->selectList->size() - 1; i++)
	{
		string_stream << translate_expression(select_statement->selectList->at(i)) << ", ";
	}
	
	string_stream << translate_expression(select_statement->selectList->back()) << " ";


	// FROM
	string_stream << "FROM " << translate_table_ref_info(select_statement->fromTable);


	// WHERE
	if (select_statement->whereClause != NULL)
	{
		string_stream << " WHERE " << translate_expression(select_statement->whereClause) << " ";
	}

	return string_stream.str();
}

/*
 * Translate Column Definition
 *
 * @param column_definition ColumnDefinition object
 * @return a string of SQL statement
*/
std::string translate_column_definition(const hsql::ColumnDefinition* column_definition)
{
	if (column_definition == NULL)
	{
		return "";
	}

	std::ostringstream string_stream;

	string_stream << column_definition->name << " ";

	switch(column_definition->type)
	{
		case hsql::ColumnDefinition::TEXT:
		{
			string_stream << "TEXT";
			break;
		}
		case hsql::ColumnDefinition::INT:
		{
			string_stream << "INT";
			break;
		}
		case hsql::ColumnDefinition::DOUBLE:
		{
			string_stream << "DOUBLE";
			break;
		}
		default:
		{
			string_stream << "UNKNOWN";
		}
	}

	return string_stream.str();
}

/*
 * Translate Column Definitions
 *
 * (Name1 Type, Name2 Type, ...)
 * @param columns a ColumnDefinition list
 * @return a string of the SQL statement  
 *
*/
std::string translate_column_definitions(const std::vector<hsql::ColumnDefinition*>* columns)
{
	if (columns == NULL || columns->size() == 0)
	{
		return "";
	}

	std::ostringstream string_stream;

	string_stream << "(";

	for (uint i = 0; i < columns->size() - 1; i++)
	{
		hsql::ColumnDefinition* column_definition = columns->at(i);
		string_stream << translate_column_definition(column_definition) << ", ";		
	}

	string_stream << translate_column_definition(columns->back()) << ")";

	return string_stream.str();
}

/*
 * Translate Create Statement
 *
 * CREATE TABLE tableName (Name1 Type, Name2 Type, ...)  
 * @param create_statement CreateStatement object
 * @return a string of the SQL statement
 *
*/
std::string translate_create(const hsql::CreateStatement* create_statement)
{
	std::ostringstream string_stream;

	string_stream << "CREATE TABLE " << create_statement->tableName;

	if (create_statement->columns != NULL && create_statement->columns->size() > 0)
	{
		string_stream << " " << translate_column_definitions(create_statement->columns);
	}

	return string_stream.str();
}

/**
 * Execute SQL statement
 * @param sql_statement SQLstatement object
 * @return a string of the SQL statement 
 */
string QueryToString(const hsql::SQLStatement* sql_statement)
{
	if (sql_statement == NULL)
	{
		return "hsql::SQLStatement* is NULL";
	}

	switch(sql_statement->type())
	{
		case hsql::kStmtSelect:
		{
			return translate_select((const hsql::SelectStatement*)sql_statement);
		}
		case hsql::kStmtCreate:
		{
      cout << "here" << endl;
			return translate_create((const hsql::CreateStatement*)sql_statement);
		}
		default:
		{
			return "Unsupported query, currently only supports Create and Select";
		}

	}
}	
