/**
 * @file helper.cpp - helper function declarations to execute SQL
 * @see "Seattle University, cpsc4300/5300, summer 2018"
 */

#pragma once

#include <string>
#include "SQLParser.h"

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
std::string translate_operator_expression(const hsql::Expr* expression);

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
std::string translate_expression(const hsql::Expr* expression);

/*
 * Translate Join Definition
 *
 * joinDefinition->left JOINTYPE joinDefinition->right ON joinDefinition->condition
 *
 * @param join_definition JoinDefinition object
 * @return a string of SQL statement
 */
std::string translate_join(const hsql::JoinDefinition* join_definition);

/*
 * Translate TableRefInfo
 *
 *@param table TableRef object
 *@return a string of the SQL statement 
 */

std::string translate_table_ref_info(const hsql::TableRef* table);

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
std::string translate_select(const hsql::SelectStatement* select_statement);

/*
 * Translate Column Definition
 *
 * @param column_definition ColumnDefinition object
 * @return a string of SQL statement
*/
std::string translate_column_definition(const hsql::ColumnDefinition* column_definition);

/*
 * Translate Column Definitions
 *
 * (Name1 Type, Name2 Type, ...)
 * @param columns a ColumnDefinition list
 * @return a string of the SQL statement  
 *
*/
std::string translate_column_definitions(const std::vector<hsql::ColumnDefinition*>* columns);

/*
 * Translate Create Statement
 *
 * CREATE TABLE tableName (Name1 Type, Name2 Type, ...)  
 * @param create_statement CreateStatement object
 * @return a string of the SQL statement
 *
*/
std::string translate_create(const hsql::CreateStatement* create_statement);

/**
 * Execute SQL statement
 * @param sql_statement SQLstatement object
 * @return a string of the SQL statement 
 */
std::string QueryToString(const hsql::SQLStatement* sql_statement);
