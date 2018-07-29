# 5300-Ocelot

  common.h – Common aliases for types.
  
  db_block.h – Abstract class for block.
  
  slotted_page.h – Slotted page function declarations 
  
  slotted_page.cpp – Slotted page function definitions.
  
  db_file.h – Abstract class for file.
  
  heap_file.h – Heap file function declarations.
  
  heap_file.cpp – Heap file function definitions.
  
  db_relation.h – Abstract class for table.
  
  heap_table.h – Heap table function declarations.
  
  heap_table.cpp – Heap table function definitions.
  
  unit_test.h – Unit test function declarations.
  
  Unit_test.cpp – Unit test function definitions.
  
  helper.h – Helper function declarations to execute SQL.
  
  helper.cpp – Helper function definitions to execute SQL.
  
  sql5300.cpp – Main entry for the relation manager’s SQL shell.
  
  SQLExec.h - SQLExec function declarations

  SQLExec.cpp - Concrete implementation for executing SQL statements

  schema_tables.h - Abstract base class for _tables, _columns, _indices

  schema_tables.cpp - Concrete implementation for managing _tables, _columns, and _indices

  db_index.h - Abstract class for index
  
  MakeFile – Tool to compile.


Compile and run:

  $ make – Compile the code
  
  $ ./sql5300  DbEnvPath ( eg: ~/cpsc5300/data) – Run the code/start shell  
  
  SQL> SQL – Get translated SQL back.
  
  The below tests may not function properly after changes made for 
  milestone 4.
  
  SQL>  test_slotted_page – To run SlottedPage unit test 
  
  SQL>  test_heap_file – To run HeapFile unit test
  
  SQL>  test_heap_table – To run HeapTable unit test
  
  $ make clean – Remove all the object files. 
  
