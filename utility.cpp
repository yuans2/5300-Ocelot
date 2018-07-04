#include "utility.h"

/*
 * Translate Operator Expression
 *
 * Format: expression->expr OPERATOR expression->expr2
 * 
 * e.g. expr < expr2
 *      expr AND expr2
 *      expr OR expr2
 *      NOT expr
 */

std::string translateOperatorExpression(const hsql::Expr* expression)
{
	if (expression == NULL)
	{
		return "";
	}

	std::ostringstream stringStream;

	switch(expression->opType)
	{
		case hsql::Expr::SIMPLE_OP:
		{
			stringStream << translateExpression(expression->expr) << " " << expression->opChar << " ";
			break;
		}
		case hsql::Expr::AND:
		{
			stringStream << translateExpression(expression->expr) << " " << "AND ";
			break;
		}
		case hsql::Expr::OR:
		{
			stringStream << translateExpression(expression->expr) << " " << "OR ";
			break;
		}
		case hsql::Expr::NOT:
		{
			stringStream << "NOT " << translateExpression(expression->expr);
			break;
		}
		default:
		{
			stringStream << "Unknown opType: " << expression->opType;
			break;
		}
	}

	if (expression->expr2 != NULL)
	{
		stringStream << translateExpression(expression->expr2);
	}

	return stringStream.str();
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
*/
std::string translateExpression(const hsql::Expr* expression)
{
	std::ostringstream stringStream;

	switch(expression->type)
	{
		/* 
		 * kExprStar
		 * e.g. SELECT * FROM ...
		 */
		case hsql::kExprStar:
		{
			stringStream << "*";
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
				stringStream << expression->table << ".";
			}

			stringStream << expression->name;
			break;

		}
		case hsql::kExprLiteralString:
		{
			stringStream << expression->name;
			break;
		}
		case hsql::kExprLiteralFloat:
		{
			stringStream << expression->fval;
			break;
		}
		case hsql::kExprLiteralInt:
		{
			stringStream << expression->ival;
			break;
		}
		case hsql::kExprOperator:
		{
			stringStream << translateOperatorExpression(expression);
			break;
		}
		default:
		{
			stringStream << "Unsupported expression type:" << expression->type;
		}
	}

	if (expression->alias != NULL)
	{
		stringStream << "AS " << expression->alias;
	}

	return stringStream.str();
}

/*
 * Translate Join Definition
 *
 * joinDefinition->left JOINTYPE joinDefinition->right ON joinDefinition->condition
 *
 */
std::string translateJoin(const hsql::JoinDefinition* joinDefinition)
{
	std::ostringstream stringStream;

	std::string leftTable = translateTableRefInfo(joinDefinition->left);
	std::string rightTable = translateTableRefInfo(joinDefinition->right);

	stringStream << leftTable << " ";

	switch(joinDefinition->type)
	{
		case hsql::kJoinInner:
		{
			stringStream << "INNER JOIN ";
			break;
		}
		case hsql::kJoinOuter:
		{
			stringStream << "OUTER JOIN ";
			break;
		}
		case hsql::kJoinLeft:
		{
			stringStream << "LEFT JOIN ";
			break;
		}
		case hsql::kJoinRight:
		{
			stringStream << "RIGHT JOIN ";
			break;
		}
		case hsql::kJoinLeftOuter:
		{
			stringStream << "LEFT OUTER JOIN ";
			break;
		}
		case hsql::kJoinRightOuter:
		{
			stringStream << "RIGHT OUTER JOIN ";
			break;
		}
		case hsql::kJoinCross:
		{
			stringStream << "CROSS JOIN ";
			break;
		}
		default:
		{
			stringStream << "UNKNOWN JOIN ";
			break;
		}
	}

	stringStream << translateTableRefInfo(joinDefinition->right) << " ";
	stringStream << "ON " << translateExpression(joinDefinition->condition);

	return stringStream.str();
}

/*
 * Translate TableRefInfo
 *
 *
 */

std::string translateTableRefInfo(const hsql::TableRef* table)
{
	std::ostringstream stringStream;

	switch(table->type)
	{
		case hsql::kTableName:
		{
			stringStream << table->name;
			break;
		}
		case hsql::kTableSelect:
		{
			stringStream << translateSelect(table->select);
			break;
		}
		case hsql::kTableJoin:
		{
			stringStream << translateJoin(table->join) << " ";
			break;
		}
		case hsql::kTableCrossProduct:
		{
			for(int i = 0; i < table->list->size() - 1; i++)
			{
				stringStream << translateTableRefInfo(table->list->at(i)) << ", ";
			}

			stringStream << translateTableRefInfo(table->list->back()) << " ";
			break;
		}
		default:
		{
			stringStream << "Unsupported TableRef: " << table->type;
		}
	}

	if (table->alias != NULL)
	{
		stringStream << " AS " << table->alias;
	}

	return stringStream.str();
}


/*
 * Translate SELECT statement
 *
 * SELECT <# selectStatement->(Expr)selectList >
 * FROM <# selectStatement->(TableRef)fromTable >
 * WHERE <# selectStatement->(Expr)whereClause >
 *
*/
std::string translateSelect(const hsql::SelectStatement* selectStatement)
{
	std::ostringstream stringStream;

	// SELECT
	stringStream << "SELECT ";

	// Deal with one (no ',') or multiple elements (with ',') in selectList
	for (int i = 0; i < selectStatement->selectList->size() - 1; i++)
	{
		stringStream << translateExpression(selectStatement->selectList->at(i)) << ", ";
	}
	
	stringStream << translateExpression(selectStatement->selectList->back()) << " ";


	// FROM
	stringStream << "FROM " << translateTableRefInfo(selectStatement->fromTable);


	// WHERE
	if (selectStatement->whereClause != NULL)
	{
		stringStream << " WHERE " << translateExpression(selectStatement->whereClause) << " ";
	}

	return stringStream.str();
}

/*
 * Translate Column Definition
 *
 * Name Type  
 *
*/
std::string translateColumnDefinition(const hsql::ColumnDefinition* columnDefinition)
{
	if (columnDefinition == NULL)
	{
		return "";
	}

	std::ostringstream stringStream;

	stringStream << columnDefinition->name << " ";

	switch(columnDefinition->type)
	{
		case hsql::ColumnDefinition::TEXT:
		{
			stringStream << "TEXT";
			break;
		}
		case hsql::ColumnDefinition::INT:
		{
			stringStream << "INT";
			break;
		}
		case hsql::ColumnDefinition::DOUBLE:
		{
			stringStream << "DOUBLE";
			break;
		}
		default:
		{
			stringStream << "UNKNOWN";
		}
	}

	return stringStream.str();
}

/*
 * Translate Column Definitions
 *
 * (Name1 Type, Name2 Type, ...)  
 *
*/
std::string translateColumnDefinitions(const std::vector<hsql::ColumnDefinition*>* columns)
{
	if (columns == NULL || columns->size() == 0)
	{
		return "";
	}

	std::ostringstream stringStream;

	stringStream << "(";

	for (int i = 0; i < columns->size() - 1; i++)
	{
		hsql::ColumnDefinition* columnDefinition = columns->at(i);
		stringStream << translateColumnDefinition(columnDefinition) << ", ";		
	}

	stringStream << translateColumnDefinition(columns->back()) << ")";

	return stringStream.str();
}

/*
 * Translate Create Statement
 *
 * CREATE TABLE tableName (Name1 Type, Name2 Type, ...)  
 *
*/
std::string translateCreate(const hsql::CreateStatement* createStatement)
{
	std::ostringstream stringStream;

	stringStream << "CREATE TABLE " << createStatement->tableName;

	if (createStatement->columns != NULL && createStatement->columns->size() > 0)
	{
		stringStream << " " << translateColumnDefinitions(createStatement->columns);
	}

	return stringStream.str();
}


std::string execute(const hsql::SQLStatement* sqlStatement)
{
	if (sqlStatement == NULL)
	{
		return "hsql::SQLStatement* is NULL";
	}

	switch(sqlStatement->type())
	{
		case hsql::kStmtSelect:
		{
			return translateSelect((const hsql::SelectStatement*)sqlStatement);
		}
		case hsql::kStmtCreate:
		{
			return translateCreate((const hsql::CreateStatement*)sqlStatement);
		}
		default:
		{
			return "Unsupported query, currently only supports Create and Select";
		}

	}
	
}

std::string execute(std::string query)
{
	hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

	if (result->isValid())
	{
		return execute(result->getStatement(0));
	}
	else
	{
		return "Invalid SQL: " + query;
	}
}
