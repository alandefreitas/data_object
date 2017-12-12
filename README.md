# Data Objects for Modern C++

There are few object libraries for Modern C++. Our project tries to accomplish *speed* with *easy syntax* and *simple integration*.  

* A single header file [`data_object.hpp`](https://github.com/alandefreitas/data_object/) represents a base class for any database connection. Other files represent specific database drivers by defining few  short virtual functions (See the [integrating](#integrating)).

* We use modern C++ and templates to achieve convertibility between all types of data in a transparent manner (See the [examples](#examples)). STL-like containers hold the results, and we use a simple syntax analogous to [PHP data objects](http://php.net/manual/en/book.pdo.php).

Table of contents:

- [Integrating](#integrating)
- [Examples](#examples)
    - [Connecting to a database](#connecting to a database)
    - [Simple queries](#simple-queries)
    - [Simple statements](#simple-statements)
    - [Fetching results](#fetching-results)
    - [Fetching columns](#fetching-columns)
    - [Prepared statements](#prepared-statements)
    - [Binding parameters and values](#binding-parameters-and-values)
    - [Binding columns](#binding-columns)
    - [Transactions](#transactions)
    - [Error handling](#error-handling)
    - [Other useful functions](#other-useful-functions)
- [Writing your own driver](#writing-your-own-driver)
    - [Finding the library](#finding-the-library)
    - [The driver class](#the-driver-class)
- [Issues](#issues)
    - [Note on compilers](#note-on-compilers)
    - [Other issues](#other-issues)

the header `data_object.h` defines the base data object. The derived drivers are in the folder `driver`. They are all in the folder [`include`](https://github.com/alandefreitas/include/). Example:

```cpp
#include "driver/pgsql.h"
// ...
#include "driver/sqlite.h"
// ... etc
```

You can easily install all database libraries with package managers.

On Linux:

```bash
sudo apt-get install sqlite3 libsqlite3-dev
sudo apt-get install postgresql-9.6 pgadmin3
```

On OS X:

```bash
brew install sqlite
brew install postgresql
# ... etc
```

The examples in the documentation presuppose you are using the `wpp::db` namespace.

```
using namespace wpp::db;
```

Remember to turn on C++11 on your compiler (`-std=c++11`). 

## Examples

### Connecting to a database

The driver objects create connections to the database:

```cpp
sqlite con("sqlite:databasefile.db");
pgsql con("pgsql:host=127.0.0.1;dbname=smartlao;port=5432","username","password");
```

Applications that manage more than one data object will usually keep pointers to the base class as in:

```cpp
std::unique_ptr<data_object> con(new sqlite("sqlite:databasefile.db"));
std::unique_ptr<data_object> con(new pgsql("pgsql:host=127.0.0.1;dbname=smartlao;port=5432","username","password"));
```

### Simple queries

Connection objects can execute simple queries with the `exec` method.

```cpp
// create a query string
// std::string sql = "CREATE TABLE ... ;" ;
con->exec(sql);
```

### Simple statements

For most uses, however, you will want to keep the statement derived from a query so you can work with it.

```cpp
sqlite::stmt stmt = con->query(sql);
```

A `sqlite::stmt` works like an iterator that manages query results. You can also use it as a boolean to check if everything is OK.

```cpp
if (stmt) {
    std::cout << "All good" << std::endl;
}
```

### Fetching results

```cpp
std::string sql = "SELECT id, name, email, salary FROM employee WHERE salary > 60000.00";
pgsql::stmt stmt = con->query(sql);
if (stmt){
    while (row r = stmt->fetch()){
        std::cout << r["id"] << " ";
        std::cout << r["name"] << " ";
        std::cout << r["email"] << " ";
        std::cout << r["salary"] << " ";
        std::cout << std::endl;
    }
}
```

A row is accessed either by their column names or their 0-indexed column numbers. Note that column numbers are faster, or course.

```cpp
std::cout << r[0] << " ";
std::cout << r["name"] << " ";
std::cout << r[2] << " ";
std::cout << r["salary"] << " ";
```

If you want to fetch all results immediately, the command `fetch_all` saves the query result in a `result` object.

```cpp
result res = stmt->fetch_all();
```

The `result` object is a normal container whose rows you can iterate:

```cpp
result res = stmt->fetch_all();
for (row& r: res){
    std::cout << r[0] << " ";
    std::cout << r["name"] << " ";
    std::cout << r[2] << " ";
    std::cout << r["salary"] << " ";
}
```

or:

```cpp
result res = stmt->fetch_all();
for (result::iterator iter = res.begin(); iter != res.end(); ++iter){
    // ... 
}
```

### Fetching columns

The `fetch_column` method allows fetching a specific field:

```cpp
while (field f = stmt->fetch_column(col)){
    std::cout << "Column: " << col << " Field: " << f << std::endl;
}
```

### Prepared statements

Use `prepare` instead of `query` to use prepared statements:

```cpp
std::string sql = "INSERT INTO employee(name, email, salary) VALUES(?, ?, ?)";
pgsql::stmt stmt = con->prepare(sql);
```

### Binding parameters and values

Once you have prepared a statement, you can bind parameters:

```cpp
std::string sql = "INSERT INTO employee(name, email, salary) VALUES(?, ?, ?)";
pgsql::stmt stmt = con->prepare(sql);
if (stmt){
    for (int i = 0; i < 10; ++i) {
        stmt->bind_param(1,"Paul McCartney");
        std::string email = "paul@mccartnet.com";
        stmt->bind_param(2,email);
        stmt->bind_param(3,100000.00);
        stmt->execute();
    }
}
```

In the example above, binding the parameter copies first parameter `"Paul McCartnet"` on the statement (*by value*). The second parameter binds `"paul@mccartney.com"` to the statement *by reference*. 

This means that things are much faster and productive because there are no copies going around and because you don't have to rebind a variable every time you change. It also means if you change the variable `email` before executing the query, the drive will consider the most recent value. For instance:
 
```cpp
std::string email = "paul@mccartnet.com";
stmt->bind_param(2,email);
email = "britney@spears.com";
stmt->execute();
```
 
This example above would insert `"britney@spears.com"` into the database. Not `"paul@mccartney.com"`. Make sure that is what you want. If you want to make force binding the value and not the parameter, you can use:

```cpp
std::string email = "paul@mccartnet.com";
stmt->bind_value(2,email);
email = "britney@spears.com";
stmt->execute();
```

This example above would insert `"paul@mccartney.com"` into the database. In the line `stmt->bind_param(3,100000.00);`, the data object binds the value because we are sending a constant.

For convenience, you can also bind parameters and values using placeholders in the query:

 ```cpp
 std::string sql = "INSERT INTO employee(name, email, salary) VALUES(:name, :email, :salary)";
 pgsql::stmt stmt = con->prepare(sql);
 if (stmt){
     for (int i = 0; i < 10; ++i) {
         stmt->bind_param(":name","Paul McCartney");
         std::string email ="paul@mccartnet.com";
         stmt->bind_param(":email",email);
         stmt->bind_param(":salary",100000.00);
         stmt->execute();
     }
 }
 ```
 
Again, this convenience is usually a little slower than the positional version. 

### Binding columns

Instead of returning a `row` or a `result`, the data objects can also save the result straight to variables you choose to bind, making it more convenient and faster to fetch results.

```cpp
std::string sql = "SELECT id, name, salary FROM employee ORDER BY salary DESC LIMIT 1";
long id;
std::string name;
double salary;
if (pgsql::stmt stmt = con->query(sql)) {
    std::cout << "Fetching highest salary to bound columns" << std::endl;
    stmt->bind_column(1,id);
    stmt->bind_column(2,name);
    stmt->bind_column(3,salary);
    if (stmt->fetch()) {
        std::cout << "id: " << id << ", name: " << name << ", salary: " << salary << std::endl;
    }
}
```

In the example we show above, the query will return the columns `id`, `name`, `salary` for each row we fetch. Instead of working with rows, we can create our own variables and bind them to the result columns:
 
```cpp
// creating variables
long id;
std::string name;
double salary;
// ...
// binding
stmt->bind_column(1,id);
stmt->bind_column(2,name);
stmt->bind_column(3,salary);
```

When we fetch the results, the values go straight to our variables:

```cpp
while (stmt->fetch()) {
    std::cout << "id: " << id << ", name: " << name << ", salary: " << salary << std::endl;
}
```

Similar to parameters, the data objects can bind columns by their names:

```cpp
// creating variables
long id;
std::string name;
double salary;
// ...
// binding
stmt->bind_column("id",id);
stmt->bind_column("name",name);
stmt->bind_column("salary",salary);
```

### Transactions

Transactions allow a group of queries to be applied safely to the database. If something goes wrong, the driver throws an exception and the query can be rolled back.

```cpp
try{
    con->begin_transaction();
    con->exec("insert into employee (name, salary, email) values ('Joe',  50000.00, 'joe@joe.com')");
    con->exec("insert into employee (name, salary, email) values ('John', 60000.00, 'john@john.com')");
    con->commit();
} catch (std::exception &e){
    con->roll_back();
    std::cout << e.what() << std::endl;
}
```

The result of the queries executed between `begin_transaction()` and `commit()` are only applied after the `commit()`. If something goes wrong, the query is rolled back by the function `roll_back()`.

### Error handling

You can always check for error with the `error()` function. 

```cpp
// int rows = con->exec("...");
if (con->error()){
    // handle it
}
```

The `error()` function is available for both connection and statements:

```cpp
if (stmt){
    stmt->bind_param(1, highest_id);
    stmt->execute();
    if (!stmt->error()){
        std::cout << "Query executed with without problems" << std::endl;
    }
}
```

The `error_string()` function helps us report the error so we can do something about it.

```cpp
if (stmt->error()){
    std::cerr << stmt->error_string() << std::endl;
}
```

You can also use the `set_attribute(error_mode)` function to decide how you want errors to be handled. 

* `error_mode::ERRMODE_SILENT` doesn't do anything (but the `error()` and `error_string()` functions are still available if you need them) 
* `error_mode::ERRMODE_WARNING` always reports the error 
* `error_mode::ERRMODE_EXCEPTION` throws an exception

Exceptions can be most useful sometimes:

```cpp
con->set_attribute(error_mode::ERRMODE_EXCEPTION);
try {
    stmt = con->query(sql);
    if (stmt){
        if (stmt->error()){
            std::cerr << stmt->error_string() << std::endl;
        }
    }
} catch (std::exception &e){
    std::cerr << "Exception: " << e.what() << std::endl;
}
```

### Other useful functions

Some other useful functions are:

* `row_count()` returns the number of rows affected by the last query
* `last_insert_id()` returns the id of the last column inserted in the database
* `set_attribute(attr)` change the data object settings. Some examples are:
    * con->set_attribute(error_mode::ERRMODE_SILENT);
    * con->set_attribute(case_conversion::CASE_NATURAL);
    * con->set_attribute(null_handling::NULL_NATURAL);
    * con->set_attribute(attribute_type::ATTR_PERSISTENT,false);
    * con->set_attribute(attribute_type::ATTR_AUTOCOMMIT,true);
* `get_attribute(attribute_type)` returns the current value of the attributes
* `quote(s)` quotes a string so it becomes safe for SQL queries. But don't worry. That happens automatically when you execute queries. Example:
    * Unquoted string: `Co'mpl''ex "st'"ring`
    * Quoted string: `'Co''mpl''''ex "st''"ring'`
* `get_column_meta(position)` returns a `column_data` object with information about the column 

## Writing your own driver

Data objects make it easy to add new drivers to your application so you can manage more databases with the same interface by only overriding a few virtual functions.

### The driver class

You can start by making a copy of one of the drivers in [`include/driver`](https://github.com/alandefreitas/data_object/include/driver) and editing it. [`include/driver/sqlite.h`](https://github.com/alandefreitas/data_object/include/driver/sqlite.h) is probably the simplest driver you can you as reference.

From then, all you have to do is override the virtual functions so they do what you want. If you have a nice driver, contemplate contributing to this project.

### Finding the library

The first thing you have to do to define a new driver is look for the library so you can link it. Edit [`findpackages.cmake`](https://github.com/alandefreitas/data_object/findpackages.cmake) and include the commands to find your driver. Example:

```cmake
find_package(SQLITE3)
include_directories(${SQLITE3_INCLUDE_DIRS})
set(ALL_LIBRARIES ${ALL_LIBRARIES} ${SQLITE3_LIBRARIES})
```

If cmake's `find_package` command can find your library, then great. Else, you can create a module to do that by creating a copy of one of the modules in [`cmake/Modules`](https://github.com/alandefreitas/data_object/cmake/Modules) and replace the commands `find_path` and `find_library` to define the library and header files you are looking for.
 

## Issues

### Note on compilers

Assure your compiler supports C++11 without bugs or it won't work. Most operating systems come with updated compilers and most compilers work properly if you update them. We do not intend to maintain macros with workarounds to support broken compilers.

### Other issues

If you have issues, you can:

* [Create a new issue](https://github.com/alandefreitas/data_object/issues/new).
* [Look at closed issues](https://github.com/alandefreitas/data_object/issues?q=is%3Aissue+is%3Aclosed)

Thank you for trying this library. [Send me an email](mailto:alandefreitas@gmail.com) for any special considerations.