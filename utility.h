#ifndef UTILITY_H
#define UTILITY_H

#include<iostream>
#include<sstream>
#include<string>
#include "SQLParser.h"

std::string translateOperatorExpression(const hsql::Expr* expression);

std::string translateExpression(const hsql::Expr* expression);

std::string translateJoin(const hsql::JoinDefinition* joinDefinition);

std::string translateTableRefInfo(const hsql::TableRef* table);

std::string translateSelect(const hsql::SelectStatement* selectStatement);

std::string translateColumnDefinition(const hsql::ColumnDefinition* columnDefinition);

std::string translateColumnDefinitions(const std::vector<hsql::ColumnDefinition*>* columns);

std::string translateCreate(const hsql::CreateStatement* createStatement);

std::string execute(const hsql::SQLStatement* sqlStatement);

std::string execute(std::string query);

#endif