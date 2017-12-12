//
// Created by Alan de Freitas on 22/11/2017.
//
#ifndef WPP_RESULT_CONTAINER_H
#define WPP_RESULT_CONTAINER_H

#include <cstddef>
#include <cstring>
#include <utility>
#include <iterator>
#include <stdexcept>
#include <algorithm>
#include <vector>
#include <string>
#include <initializer_list>
#include <boost/algorithm/string/case_conv.hpp>

namespace wpp {
    namespace db {
        enum data_type { STRING, INTEGER, REAL, BOOLEAN, NULL_DATA };

        class field
                : public std::string {
            public:
                // Construct from...
                field() : std::string(), _data_type(NULL_DATA) {}

                field(std::string &s) : std::string(s), _data_type(STRING) {}

                field(bool c) : std::string(c ? "TRUE" : "FALSE"), _data_type(BOOLEAN) {}

                field(int n) : std::string(std::to_string(n)), _data_type(INTEGER) {}

                field(double n) : std::string(std::to_string(n)), _data_type(REAL) {}

                field(float n) : std::string(std::to_string(n)), _data_type(REAL) {}

                // Convert to...
                operator bool() const {
                    if (_data_type == BOOLEAN) {
                        if (*this == "1" || *this == "True" || *this == "TRUE" || *this == "true") {
                            return true;
                        };
                    } else if (_data_type == INTEGER) {
                        return !this->empty() && *this != "0";
                    } else if (_data_type == REAL) {
                        return !this->empty();
                    } else if (_data_type == STRING) {
                        return !this->empty();
                    } else if (_data_type == NULL_DATA) {
                        return false;
                    }
                    return false;
                }

                operator int() const { return this->empty() ? 0 : std::stoi(*this); }

                operator long() const { return this->empty() ? 0 : std::stol(*this); }

                operator unsigned int() const { return this->empty() ? 0 : std::stoul(*this); }

                operator long long() const { return this->empty() ? 0 : std::stoll(*this); }

                operator unsigned long long() const { return this->empty() ? 0 : std::stoull(*this); }

                operator float() const { return this->empty() ? 0.0 : std::stof(*this); }

                operator double() const { return this->empty() ? 0.0 : std::stod(*this); }

                operator long double() const { return this->empty() ? 0.0 : std::stold(*this); }

                template<typename T>
                bool operator>>(T &obj) const { return to(obj); }

                field &operator=(const field &rhs) {
                    std::string::operator=(rhs);
                    return *this;
                };

                field &operator=(const std::string &rhs) {
                    std::string::operator=(rhs);
                    return *this;
                };

                field &operator=(const int &a) { return *this = std::to_string(a); };

                /// Column name
                std::string name() const { return this->_table_column; }

                data_type type() const { return this->_data_type; }

                std::string table() const { return this->_table; }

                size_t num() const { return col(); }

                size_t table_column() const { return col(); }

                /// Is this field's value null
                bool is_null() const {
                    return _data_type == NULL_DATA;
                }

                bool set_null() const {
                    return _data_type == NULL_DATA;
                }

                size_type size() const noexcept;

                size_t idx() const noexcept { return m_row; }

                size_t col() const noexcept { return size_t(m_col); }

            protected:
                size_t m_row;
                size_t m_col;
                std::string _table;
                std::string _table_column;
                std::string _value;
                std::string _error;
                data_type _data_type;
        };

        class row
                : public std::vector<field> {
            public:
                row() : std::vector<field>() {};

                row(size_t n) : std::vector<field>(n) {};

                row(size_t n, field f) : std::vector<field>(n, f) {};

                // operators
                reference operator[](size_t n) { return vector<field>::operator[](n); };

                const_reference operator[](size_t n) const { return vector<field>::operator[](n); };

                reference operator[](const std::string &s) {
                    return vector<field>::operator[](this->column_number(s.c_str()));
                }

                const_reference operator[](const std::string &s) const {
                    return vector<field>::operator[](this->column_number(s.c_str()));
                }

                template<int N>
                reference operator[](const char (&f)[N]) { return vector<field>::operator[](this->column_number(f)); }

                template<int N>
                const_reference operator[](const char (&f)[N]) const {
                    return vector<field>::operator[](this->column_number(f));
                }

                // at operator
                reference at(const char f[]) { return vector<field>::at(this->column_number(f)); }

                const_reference at(const char f[]) const { return vector<field>::at(this->column_number(f)); }

                reference at(const std::string &s) { return vector<field>::at(this->column_number(s.c_str())); }

                const_reference at(const std::string &s) const {
                    return vector<field>::at(this->column_number(s.c_str()));
                }

                // Implicit conversion operator
                operator bool() const { return !this->empty(); }

                ///////////////////////////////////////////////////////////////
                //                            GETTERS                        //
                ///////////////////////////////////////////////////////////////
                /// row number in the query
                size_t row_number() const noexcept { return size_t(m_index); }

                size_t num() const { return this->row_number(); }

                size_t num_of_fields() { return this->size(); }

                /// Column number from name
                size_t column_number(const char column_name[]) const {
                    return std::find_if(this->_columns.begin(), this->_columns.end(),
                                        [column_name](std::shared_ptr<std::string> const &x) {
                                            return (x) ? *x == column_name : false;
                                        }) - this->_columns.begin();
                }

                size_t column_number(const std::string &column_name) const {
                    return column_number(column_name.c_str());
                }

                // Column name from number
                std::string column_name(size_t n) const { return (this->_columns[n]) ? *this->_columns[n] : ""; }

                // Column type
                data_type column_type(size_t n) const {
                    return this->_data_types[n];
                }

                unsigned int column_type(const std::string &column_name) const {
                    return column_type(column_number(column_name));
                }

                unsigned int column_type(const char column_name[]) const {
                    return column_type(column_number(column_name));
                }

                /// Column table
                unsigned int column_table(size_t ColNum) const;

                unsigned int column_table(int ColNum) const { return column_table(size_t(ColNum)); }

                unsigned int column_table(const std::string &ColName) const {
                    return column_table(column_number(ColName));
                }

                /// Original column name
                size_t table_column(size_t ColNum) const;

                size_t table_column(int ColNum) const { return table_column(size_t(ColNum)); }

                size_t table_column(const std::string &ColName) const { return table_column(column_number(ColName)); }

                // Produce a slice of this row, containing the given range of columns.
                //row slice(size_t begin_pos, size_t end_pos) const {
                //    row result(this->begin()+begin_pos, this->begin() + end_pos);
                //    return result;
                //}
                ///////////////////////////////////////////////////////////////
                //                           SETTERS                         //
                ///////////////////////////////////////////////////////////////
                // push_back with column name
                void push_back(field data) {
                    vector<field>::push_back(data);
                    _columns.emplace_back(nullptr);
                    _data_types.emplace_back(STRING);
                }

                void push_back(std::string column_name, field data) {
                    vector<field>::push_back(data);
                    _columns.emplace_back(new std::string(column_name));
                    _data_types.emplace_back(STRING);
                }

                void push_back(std::shared_ptr<std::string> column_name, field data) {
                    vector<field>::push_back(data);
                    _columns.emplace_back(column_name);
                    _data_types.emplace_back(STRING);
                }

                ///////////////////////////////////////////////////////////////
                //                              HELPERS                      //
                ///////////////////////////////////////////////////////////////
                // "Container" operators
                // get a new row with only the column pos
                row pluck(size_t pos) {
                    return row(1, this->operator[](pos));
                }
                // change the column names to lower case
                //row& change_key_case (){
                //    for (auto &&column : this->_columns) {
                //        boost::algorithm::to_lower(column);
                //    }
                //    return *this;
                //}
            protected:
                std::vector<std::shared_ptr<std::string>> _columns;
                std::vector<data_type> _data_types;
                unsigned int m_index;
        };

        class result
                : public std::vector<row> {
            public:
                // Constructors
                result() : std::vector<row>() {};

                result(size_t n) : std::vector<row>(n) {};

                result(size_t n, std::initializer_list<std::string> column_names) :
                        std::vector<row>(n, wpp::db::row(column_names.size())),
                        _columns(column_names.begin(), column_names.end()),
                        _data_types(_columns.size(), data_type::STRING) {};

                // Number of columns
                size_t columns() const noexcept {
                    return this->_columns.size();
                }

                size_t size_rows() const noexcept {
                    return this->size();
                }

                size_t size_columns() const noexcept {
                    return this->_columns.size();
                }

                // Column number
                size_t column_number(const char column_name[]) const {
                    return std::find(this->_columns.begin(), this->_columns.end(), column_name) -
                           this->_columns.begin();
                }

                size_t column_number(const std::string &column_name) const {
                    return column_number(column_name.c_str());
                }

                // Column name
                std::string column_name(size_t n) const { return this->_columns[n]; }

                // Column type
                data_type column_type(size_t n) const {
                    return this->_data_types[n];
                }

                data_type column_type(int n) const { return column_type(size_t(n)); }

                unsigned int column_type(const std::string &column_name) const {
                    return column_type(column_number(column_name));
                }

                unsigned int column_type(const char column_name[]) const {
                    return column_type(column_number(column_name));
                }

                /// Column table
                unsigned int column_table(size_t ColNum) const;

                unsigned int column_table(int ColNum) const { return column_table(size_t(ColNum)); }

                unsigned int column_table(const std::string &ColName) const {
                    return column_table(column_number(ColName));
                }

                /// Original column name
                size_t table_column(size_t ColNum) const;

                size_t table_column(int ColNum) const { return table_column(size_t(ColNum)); }

                size_t table_column(const std::string &ColName) const { return table_column(column_number(ColName)); }

                /// Query that produced this result, if available (empty std::string otherwise)
                const std::string &query() const noexcept;

                /// If command was INSERT
                unsigned int inserted_row_id() const;

                /// If command was INSERT, UPDATE, or DELETE
                size_type affected_rows() const;

                // "Container" operations
                vector <field> pluck(std::string value) {
                    return this->pluck(this->column_number(value));
                }

                vector <std::pair<field, field>> pluck(std::string value, std::string key) {
                    return this->pluck(this->column_number(value), this->column_number(key));
                }

                vector <field> pluck(size_t pos) {
                    vector <field> r;
                    r.reserve(this->size());
                    for (row &_row : *this) {
                        r.push_back(_row[pos]);
                    }
                    return r;
                }

                vector <std::pair<field, field>> pluck(size_t pos, size_t pos2) {
                    vector <std::pair<field, field>> r;
                    r.reserve(this->size());
                    for (row &_row : *this) {
                        r.push_back({_row[pos2], _row[pos]});
                    }
                    return r;
                }

            protected:
                std::vector<std::string> _columns;
                std::vector<data_type> _data_types;
        };
    }
}
#endif //WPP_RESULT_CONTAINER_H
