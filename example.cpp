#include <iostream>
#include <string>
#include <thread>
#include <random>
#include <chrono>
#include "result.h"
#include "data_object.h"
#include "driver/pgsql.h"
#include "driver/sqlite.h"

using namespace wpp::db;

///////////////////////////////////////////////////////////////
//                    EXAMPLE FUNCTIONS                      //
///////////////////////////////////////////////////////////////
std::string name_generator() {
    const std::vector<std::string> names = {"Freddy Mercury", "Robert Plant", "Elvis Presley", "David Bowie",
                                            "Jim Morrison", "John Lennon", "Paul McCartney", "Steve Perry",
                                            "Steven Tyler", "Elton John", "Mick Jagger", "Roger Daltrey",
                                            "Ronnie James Dio", "Chris Cornell", "Axl Rose", "Johnny Cash",
                                            "Stevie Nicks", "Janis Joplin", "Ann Wilson", "Ozzy Osbourne",
                                            "Pat Benatar", "Kurt Cobain", "Eddie Vedder", "Bon Scott", "Billy Joel",
                                            "Eric Clapton", "Jon Bon Jovi", "Phil Collins"};
    static unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    static std::default_random_engine generator(seed);
    static std::uniform_int_distribution<int> distribution(0, names.size() - 1);
    size_t pos1 = distribution(generator);
    size_t pos2 = distribution(generator);
    return std::string(names[pos1].substr(0, names[pos1].find(' ', 0))) + " " +
           std::string(names[pos2].substr(names[pos2].find(' ', 0) + 1));
}

std::string email_generator() {
    const std::vector<std::string> names = {"freddy@mercury.com", "robert@plant.com", "elvis@presley.com",
                                            "david@bowie.com", "jim@morrison.com", "john@lennon.com",
                                            "paul@mccartney.com", "steve@perry.com", "steven@tyler.com",
                                            "elton@john.com", "mick@jagger.com", "roger@daltrey.com", "ronnie@dio.com",
                                            "chris@cornell.com", "axl@rose.com", "johnny@cash.com", "stevie@nicks.com",
                                            "janis@joplin.com", "ann@wilson.com", "ozzy@osbourne.com",
                                            "pat@benatar.com", "kurt@cobain.com", "eddie@vedder.com", "bon@scott.com",
                                            "billy@joel.com", "eric@clapton.com", "jon@bon_jovi.com",
                                            "phil@collins.com"};
    static unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    static std::default_random_engine generator(seed);
    static std::uniform_int_distribution<int> distribution(0, names.size() - 1);
    size_t pos1 = distribution(generator);
    size_t pos2 = distribution(generator);
    return std::string(names[pos1].substr(0, names[pos1].find(' ', 0))) + "@" +
           std::string(names[pos2].substr(names[pos2].find('@', 0) + 1));
}

double salary_generator() {
    const std::vector<std::string> names = {"freddy mercury", "robert plant", "elvis presley", "david bowie",
                                            "jim morrison", "john lennon", "paul mccartney", "steve perry",
                                            "steven tyler", "elton john", "mick jagger", "roger daltrey", "ronnie dio",
                                            "chris cornell", "axl rose", "johnny cash", "stevie nicks", "janis joplin",
                                            "ann wilson", "ozzy osbourne", "pat benatar", "kurt cobain", "eddie vedder",
                                            "bon scott", "billy joel", "eric clapton", "jon bon_jovi", "phil collins"};
    static unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    static std::default_random_engine generator(seed);
    static std::normal_distribution<double> distribution(60000.00, 15000.00);
    return distribution(generator);
}

void creating_table_example(std::unique_ptr<data_object> &con) {
    std::string sql = "CREATE TABLE IF NOT EXISTS employee ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "name VARCHAR( 250 ) NOT NULL,"
            "email VARCHAR( 250 ) NOT NULL,"
            "salary DOUBLE NOT NULL);";
    con->exec(sql);
}

void inserting_example(std::unique_ptr<data_object> &con) {
    std::string sql = "INSERT INTO employee(name, email, salary) VALUES(?, ?, ?)";
    pgsql::stmt stmt = con->prepare(sql);
    if (stmt) {
        for (int i = 0; i < 10; ++i) {
            stmt->bind_param(1, name_generator());
            std::string email = email_generator();
            stmt->bind_param(2, email);
            stmt->bind_param(3, salary_generator());
            stmt->execute();
        }
    }
}

void selecting_all_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT id, name, email, salary FROM employee WHERE salary > 60000.00";
    pgsql::stmt stmt = con->query(sql);
    if (stmt) {
        std::cout << "Employees that earn more than $60,000.00" << std::endl;
        std::cout << "Id \t Name \t E-mail \t Salary" << std::endl;
        while (row r = stmt->fetch()) {
            std::cout << r[0] << r[1] << "\t" << r["email"] << "\t" << r["salary"] << "\t" << std::endl;
        }
    }
}

void fetching_all_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT name, email, salary FROM employee WHERE salary < 60000.00";
    if (pgsql::stmt stmt = con->query(sql)) {
        std::cout << "Employees that earn less than $60,000.00" << std::endl;
        std::cout << "Name \t E-mail \t Salary" << std::endl;
        result res = stmt->fetch_all();
        for (row &r: res) {
            std::cout << r[0] << "\t" << r["email"] << "\t" << r["salary"] << "\t" << std::endl;
        }
    }
}

void fetching_column_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT name, email, salary FROM employee WHERE salary > 60000.00";
    int col = 0;
    int row = 1;
    if (pgsql::stmt stmt = con->query(sql)) {
        while (field f = stmt->fetch_column(col)) {
            std::cout << "Column: " << col << " of row " << row << ": " << f << std::endl;
            col = (col + 1) % 3;
        }
    }
}

void selecting_like_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT name, email FROM employee WHERE name LIKE ?";
    if (pgsql::stmt stmt = con->prepare(sql)) {
        std::vector<std::string> names = {name_generator(), name_generator(), name_generator(), name_generator()};
        for (size_t i = 0; i < names.size(); ++i) {
            std::cout << "Looking for employees named " << names[i].substr(0, names[i].find(' ')) << std::endl;
            std::string first_name = names[i].substr(0, names[i].find(' ')) + "%";
            stmt->bind_param(1, first_name);
            if (stmt->execute()) {
                bool first = true;
                while (row r = stmt->fetch()) {
                    if (first) {
                        std::cout << "First name\tName\tE-mail\t" << std::endl;
                        first = false;
                    }
                    std::cout << first_name << ": \t" << r[0] << "\t" << r[1] << "\t" << std::endl;
                }
            }
        }
    }
}

void selecting_and_deleting_exemple(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT id FROM employee ORDER BY ID DESC LIMIT 1";
    pgsql::stmt stmt;
    long highest_id;
    stmt = con->query(sql);
    if (stmt) {
        if (row r = stmt->fetch()) {
            std::cout << "Highest ID is " << r[0] << std::endl;
            highest_id = r[0];
        }
    }
    sql = "DELETE FROM employee WHERE id = ?";
    stmt = con->prepare(sql);
    if (stmt) {
        stmt->bind_param(1, highest_id);
        stmt->execute();
        if (!stmt->error()) {
            std::cout << "We deleted this " << highest_id << "th smarty pants" << std::endl;
        }
    }
}

void selecting_and_updating_example(std::unique_ptr<data_object> &con) {
    pgsql::stmt stmt;
    std::string sql = "SELECT id, name, salary FROM employee ORDER BY ID DESC LIMIT 1";
    long highest_id;
    double salary;
    stmt = con->query(sql);
    if (stmt) {
        if (row r = stmt->fetch()) {
            highest_id = r[0];
            salary = r[2];
            std::cout << "Highest ID is now " << r[0] << " with " << r["name"] << " who earns " << salary << std::endl;
        }
    }
    sql = "UPDATE employee SET salary = ? WHERE id = ?";
    stmt = con->prepare(sql);
    if (stmt) {
        salary *= 1.2;
        stmt->bind_param(1, salary);
        stmt->bind_param(2, highest_id);
        stmt->execute();
    }
    sql = "SELECT name, email, salary FROM employee WHERE id = ?";
    stmt = con->prepare(sql);
    if (stmt) {
        stmt->bind_param(1, highest_id);
        stmt->execute();
        if (row r = stmt->fetch()) {
            std::cout << "That's the new guy!" << std::endl;
            std::cout << r[0] << "\t" << r[1] << "\t" << r[2] << std::endl;
        }
    }
}

void binding_column_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT id, name, salary FROM employee ORDER BY salary DESC LIMIT 1";
    long id;
    std::string name;
    double salary;
    if (pgsql::stmt stmt = con->query(sql)) {
        std::cout << "Fetching highest salary to bound columns" << std::endl;
        stmt->bind_column(1, id);
        stmt->bind_column(2, name);
        stmt->bind_column(3, salary);
        if (stmt->fetch()) {
            std::cout << "id: " << id << ", name: " << name << ", salary: " << salary << std::endl;
        }
    }
}

void binding_column_by_name_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT id, name, salary FROM employee ORDER BY salary ASC LIMIT 1";
    long id;
    std::string name;
    double salary;
    if (pgsql::stmt stmt = con->query(sql)) {
        std::cout << "Fetching lowest salary to bound columns" << std::endl;
        stmt->bind_column("id", id);
        stmt->bind_column("name", name);
        stmt->bind_column("salary", salary);
        if (stmt->fetch()) {
            std::cout << "id: " << id << ", name: " << name << ", salary: " << salary << std::endl;
        }
    }
}

void binding_parameter_by_name_example(std::unique_ptr<data_object> &con) {
    std::string sql = "SELECT name, salary FROM employee WHERE salary > :salary ORDER BY salary ASC";
    if (pgsql::stmt stmt = con->prepare(sql)) {
        double minimum_wage = 20000.00;
        bool results = true;
        while (results) {
            results = false;
            stmt->bind_param(":salary", minimum_wage);
            std::string name;
            double salary;
            stmt->bind_column("name", name);
            stmt->bind_column("salary", salary);
            if (stmt->execute()) {
                std::cout << "Employees that earn more than $" << minimum_wage << ": ";
                while (row r = stmt->fetch()) {
                    results = true;
                    std::cout << name << " ($" << salary << "); ";
                }
                std::cout << ((!results) ? "No one" : "") << std::endl;
            }
            minimum_wage += 10000;
        }
    }
}

void transaction_example(std::unique_ptr<data_object> &con) {
    try {
        con->begin_transaction();
        con->exec("insert into employee (name, salary, email) values ('Joe',  50000.00, 'joe@joe.com')");
        con->exec("insert into employee (name, salary, email) values ('John', 60000.00, 'john@john.com')");
        con->commit();
    } catch (std::exception &e) {
        con->roll_back();
        std::cout << e.what() << std::endl;
    }
}

void error_handling_example(std::unique_ptr<data_object> &con) {
    pgsql::stmt stmt;
    std::string sql = "SELECT qwerasdf FROM qwerasdf ORDER BY qwerasdf DESC LIMIT qwerasdf";
    con->set_attribute(error_mode::ERRMODE_SILENT);
    stmt = con->query(sql);
    if (con->error()) {
        std::cerr << con->error_string() << std::endl;
    }
    if (stmt) {
        if (stmt->error()) {
            std::cerr << stmt->error_string() << std::endl;
        }
    } else {
        std::cerr << "Warning: stmt couldn't be created" << std::endl;
    }
    con->set_attribute(error_mode::ERRMODE_WARNING);
    stmt = con->query(sql);
    if (stmt) {
        if (stmt->error()) {
            std::cerr << "Error String: " << stmt->error_string() << std::endl;
        }
    }
    con->set_attribute(error_mode::ERRMODE_EXCEPTION);
    try {
        stmt = con->query(sql);
        if (stmt) {
            if (stmt->error()) {
                std::cerr << stmt->error_string() << std::endl;
            }
        }
    } catch (std::exception &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
}

void exec_example(std::unique_ptr<data_object> &con) {
    int rows = con->exec("INSERT INTO employee (name, salary, email) VALUES ('Maria',  90000.00, 'maria@maria.com')");
    if (con->error()) {
        std::cout << "Couldn't put Maria in the database" << std::endl;
    } else {
        std::cout << rows << " row" << ((rows != 1) ? "s" : "") << " inserted in the database" << std::endl;
        std::cout << "Last inserted row was " << con->last_insert_id() << std::endl;
    }
    rows = con->exec("DELETE FROM employee WHERE name = 'Maria'");
    if (con->error()) {
        std::cout << "Couldn't remove Maria from the database" << std::endl;
    } else {
        std::cout << rows << " row" << ((rows != 1) ? "s" : "") << " deleted from the database" << std::endl;
    }
}

void setup_example(std::unique_ptr<data_object> &con) {
    con->set_attribute(error_mode::ERRMODE_SILENT);
    con->set_attribute(case_conversion::CASE_NATURAL);
    con->set_attribute(null_handling::NULL_NATURAL);
    con->set_attribute(attribute_type::ATTR_PERSISTENT, false);
    con->set_attribute(attribute_type::ATTR_AUTOCOMMIT, true);
    bool auto_commit = con->get_attribute(attribute_type::ATTR_AUTOCOMMIT);
    std::cout << "Auto Commit is " << (auto_commit ? "ON" : "OFF") << std::endl;
}

void quote_example(std::unique_ptr<data_object> &con) {
    std::string s = "Nice";
    std::cout << "Unquoted string: " << s << std::endl;
    std::cout << "Quoted string: " << con->quote(s) << std::endl;
    s = "Complex String";
    std::cout << "Unquoted string: " << s << std::endl;
    std::cout << "Quoted string: " << con->quote(s) << std::endl;
    s = "Co'mpl''ex \"st'\"ring";
    std::cout << "Unquoted string: " << s << std::endl;
    std::cout << "Quoted string: " << con->quote(s) << std::endl;
}

void cursor_example(std::unique_ptr<data_object> &con) {
    auto stmt = con->prepare("SELECT id FROM employee WHERE salary > 10000");
    auto another_stmt = con->prepare("SELECT name FROM employee WHERE salary > 20000");
    stmt->execute();
    if (stmt) {
        if (stmt->fetch()) {
            std::cout << "Leaving unfetched rows" << std::endl;
        }
    }
    stmt->close_cursor();
    another_stmt->execute();
}

void column_meta_data_example(std::unique_ptr<data_object> &con) {
    auto select = con->query("SELECT COUNT(*) FROM employee");
    column_data meta = select->get_column_meta(0);
    std::cout << "meta.param_type: " << (int) meta.param_type << std::endl;
    std::cout << "meta.native_type: " << meta.native_type << std::endl;
    std::cout << "meta.table: " << meta.table << std::endl;
    std::cout << "meta.native_table_id: " << meta.native_table_id << std::endl;
    std::cout << "meta.name: " << meta.name << std::endl;
    std::cout << "meta.len: " << meta.len << std::endl;
    std::cout << "meta.maxlen: " << meta.maxlen << std::endl;
    std::cout << "meta.precision: " << meta.precision << std::endl;
}

int main() {
    //std::unique_ptr<data_object> con(new pgsql("host=127.0.0.1;dbname=mydbname;port=5432","my_username","my_password"));
    std::unique_ptr<data_object> con(new sqlite("sqlite:databasefile.db"));
    creating_table_example(con);
    std::cout << std::endl;
    inserting_example(con);
    std::cout << std::endl;
    selecting_all_example(con);
    std::cout << std::endl;
    fetching_all_example(con);
    std::cout << std::endl;
    fetching_column_example(con);
    std::cout << std::endl;
    selecting_like_example(con);
    std::cout << std::endl;
    selecting_and_deleting_exemple(con);
    std::cout << std::endl;
    selecting_and_updating_example(con);
    std::cout << std::endl;
    binding_column_example(con);
    std::cout << std::endl;
    binding_column_by_name_example(con);
    std::cout << std::endl;
    binding_parameter_by_name_example(con);
    std::cout << std::endl;
    transaction_example(con);
    std::cout << std::endl;
    error_handling_example(con);
    std::cout << std::endl;
    exec_example(con);
    std::cout << std::endl;
    setup_example(con);
    std::cout << std::endl;
    quote_example(con);
    std::cout << std::endl;
    cursor_example(con);
    std::cout << std::endl;
    column_meta_data_example(con);
    std::cout << std::endl;
    return 0;
}