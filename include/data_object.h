//
// Created by Alan de Freitas on 18/11/2017.
//
#ifndef WPP_DATA_OBJECT_H
#define WPP_DATA_OBJECT_H

#include <stdio.h>
#include <sstream>
#include <regex>
#include <memory>
#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <unordered_map>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/format.hpp>
#include <boost/variant.hpp>
#include "result.h"

namespace wpp {
    namespace db {
        ///////////////////////////////////////////////////////////////
        //                          ENUMERATIONS                     //
        ///////////////////////////////////////////////////////////////
        enum param_type {
            PARAM_NULL,
            PARAM_INT,
            PARAM_STR,
            PARAM_BOOL,
            PARAM_FLOAT,
            PARAM_LOB
        };
        enum fetch_orientation {
            FETCH_ORI_NEXT,
            FETCH_ORI_PRIOR,
            FETCH_ORI_FIRST,
            FETCH_ORI_LAST,
            FETCH_ORI_ABS,
            FETCH_ORI_REL
        };
        enum attribute_type {
            ATTR_AUTOCOMMIT,
            ATTR_PREFETCH,
            ATTR_TIMEOUT,
            ATTR_ERRMODE,
            ATTR_SERVER_VERSION,
            ATTR_CLIENT_VERSION,
            ATTR_SERVER_INFO,
            ATTR_CONNECTION_STATUS,
            ATTR_CASE,
            ATTR_CURSOR_NAME,
            ATTR_CURSOR,
            ATTR_ORACLE_NULLS,
            ATTR_PERSISTENT,
            ATTR_STATEMENT_CLASS,
            ATTR_FETCH_TABLE_NAMES,
            ATTR_FETCH_CATALOG_NAMES,
            ATTR_DRIVER_NAME,
            ATTR_STRINGIFY_FETCHES,
            ATTR_MAX_COLUMN_LEN,
            ATTR_DEFAULT_FETCH_MODE,
            ATTR_EMULATE_PREPARES,
            ATTR_DEFAULT_STR_PARAM,
            ATTR_DRIVER_SPECIFIC = 1000
        };
        enum cursor_type {
            CURSOR_FWDONLY,
            CURSOR_SCROLL
        };
        enum error_mode {
            ERRMODE_SILENT,
            ERRMODE_WARNING,
            ERRMODE_EXCEPTION
        };
        enum case_conversion {
            CASE_NATURAL,
            CASE_UPPER,
            CASE_LOWER
        };
        enum null_handling {
            NULL_NATURAL,
            NULL_EMPTY_STRING,
            NULL_TO_STRING
        };
        enum param_event {
            PARAM_EVT_ALLOC,
            PARAM_EVT_FREE,
            PARAM_EVT_EXEC_PRE,
            PARAM_EVT_EXEC_POST,
            PARAM_EVT_FETCH_PRE,
            PARAM_EVT_FETCH_POST,
            PARAM_EVT_NORMALIZE
        };
        enum placeholder_support {
            PLACEHOLDER_NONE,
            PLACEHOLDER_NAMED,
            PLACEHOLDER_POSITIONAL
        };
        enum parser_option {
            PARSER_TEXT = 1,
            PARSER_BIND,
            PARSER_BIND_POS,
            PARSER_EOI
        };
        ///////////////////////////////////////////////////////////////
        //                          STRUCTURES                       //
        ///////////////////////////////////////////////////////////////
        struct column_data {
            std::string name;
            std::string table;
            enum param_type param_type;
            int len;
            size_t maxlen;
            unsigned long precision;
            std::string native_type;
            unsigned long native_table_id;
            std::vector<std::string> flags;
        };

        struct driver_option
                : public boost::variant<int, std::string> {
            driver_option() : boost::variant<int, std::string>() {}

            driver_option(bool v) : boost::variant<int, std::string>((int) v) {}

            driver_option(std::string v) : boost::variant<int, std::string>(v) {}

            driver_option(int v) : boost::variant<int, std::string>(v) {}

            operator bool() { return this->get_bool(); }

            operator std::string() { return this->get_string(); }

            operator int() { return this->get_int(); }

            bool is_int() const {
                return this->which() == 0;
            }

            bool is_string() const {
                return this->which() == 1;
            }

            int get_int() const {
                if (this->is_int()) {
                    return boost::get<int>(*this);
                } else {
                    return std::stoi(boost::get<std::string>(*this));
                }
            }

            std::string get_string() const {
                if (is_string()) {
                    return boost::get<std::string>(*this);
                } else {
                    return std::to_string(boost::get<int>(*this));
                }
            }

            bool get_bool() const {
                if (is_int()) {
                    return boost::get<int>(*this) != 0;
                } else {
                    return !boost::get<std::string>(*this).empty();
                }
            }
        };

        class data_object_statement;

        struct bound_param_data {
            void *parameter;
            std::type_index parameter_typeinfo{typeid(std::string)};
            std::unique_ptr<std::string> parameter_data;                /* the variable itself (for bind_value) */
            long paramno{-1}; /* if -1, then it has a name, and we don't know the index *yet* */
            std::string name;
            data_object_statement *stmt;
            enum param_type param_type;
            bool is_param;
        };
        struct scanner {
            char *ptr;
            char *cur;
            char *tok;
            char *end;
        };
        struct placeholder {
            std::string unquoted;
            size_t position_in_query;
            int bindno = -1;
            std::string quoted;
            int freeq = 1;
        };
        ///////////////////////////////////////////////////////////////
        //                    META-PROGRAMMING HELPERS               //
        ///////////////////////////////////////////////////////////////
        template<class T>
        struct is_c_str
                : std::integral_constant<
                        bool,
                        std::is_same<char const *, typename std::decay<T>::type>::value ||
                        std::is_same<char *, typename std::decay<T>::type>::value ||
                        std::is_same<char[], typename std::decay<T>::type>::value
                > {
        };
        template<typename T>
        struct is_arithmetic_or_string_variable
                : std::integral_constant<bool, std::is_arithmetic<std::decay<T>>::value ||
                                               std::is_assignable<std::string, std::decay<T>>::value ||
                                               is_c_str<T>::value> {
        };
        template<typename T>
        struct is_arithmetic_or_string
                : std::integral_constant<bool,
                        is_arithmetic_or_string_variable<std::decay<T>>::value> {
        };
        template<typename T>
        struct is_arithmetic_or_string_reference
                : std::integral_constant<bool,
                        is_arithmetic_or_string<T>::value && std::is_reference<T>::value> {
        };
        template<typename T>
        struct is_arithmetic_or_string_const
                : std::integral_constant<bool, is_arithmetic_or_string<T>::value && std::is_const<T>::value> {
        };
        template<typename T>
        struct is_pure_char
                : std::integral_constant<bool,
                        std::is_same<T, char>::value || std::is_same<T, char16_t>::value ||
                        std::is_same<T, char32_t>::value || std::is_same<T, wchar_t>::value ||
                        std::is_same<T, signed char>::value> {
        };
        template<typename T>
        struct is_bool
                : std::integral_constant<bool, std::is_same<T, bool>::value> {
        };
        template<typename T>
        struct is_pure_int
                : std::integral_constant<bool,
                        std::is_integral<T>::value && !is_bool<T>::value && !is_pure_char<T>::value> {
        };

        template<typename T>
        class has_stream {
                typedef char one;
                typedef long two;

                template<typename C>
                static one test(typeof(&C::operator<<));

                template<typename C>
                static two test(...);

            public:
                enum { value = sizeof(test<T>(0)) == sizeof(char) };
        };

        ///////////////////////////////////////////////////////////////
        //                    STATEMENT DECLARATION                  //
        ///////////////////////////////////////////////////////////////
        class data_object;

        class data_object_statement {
            public:
                friend data_object;

                template<typename T, typename T2>
                friend
                class data_object_crtp;

                ///////////////////////////////////////////////////////////////
                //                    FUNCTIONS FOR THE USER                 //
                ///////////////////////////////////////////////////////////////
                template<typename P, typename T>
                bool bind_param(P param_no_or_name, const T &parameter) {
                    return this->register_bound_param(param_no_or_name, parameter, true, false);
                };

                template<typename P, typename T>
                bool bind_value(P param_no_or_name, T parameter) {
                    return this->register_bound_param(param_no_or_name, parameter, true, true);
                };

                template<typename P, typename T>
                bool bind_column(P column_no_or_name, const T &column) {
                    return this->register_bound_param(column_no_or_name, column, false, false);
                }

                bool execute(std::vector<std::pair<std::string, std::string>> input_parameters = {});

                wpp::db::row fetch(fetch_orientation orientation = fetch_orientation::FETCH_ORI_NEXT, long offset = 0);

                wpp::db::field fetch_column(long column_number = 0);

                wpp::db::result fetch_all();

                long row_count() { return _row_count; };

                std::string error_code() { return _error_code; };

                std::string error_string();

                bool error() { return this->_error_code.empty(); };

                std::vector<std::string> error_info();

                bool set_attribute(long attribute, driver_option value);

                driver_option get_attribute(long attribute);

                driver_option get_attribute(attribute_type attribute) { return this->get_attribute((long) attribute); }

                long column_count() { return this->_columns.size(); };

                column_data get_column_meta(long column);

                bool next_rowset();

                bool close_cursor();

                ///////////////////////////////////////////////////////////////
                //           SPECIAL VERSIONS OF THE FUNCTIONS ABOVE         //
                ///////////////////////////////////////////////////////////////
                template<typename P, int N>
                bool bind_param(P param_no_or_name, const char (&parameter)[N]) {
                    return this->bind_value<P, std::string>(param_no_or_name, parameter);
                };

                template<typename P, typename T>
                bool bind_param(P param_no_or_name, const T &&param) {
                    const T param_copy = param;
                    return this->bind_value(param_no_or_name, param_copy);
                }

                template<typename P, int N>
                bool bind_value(P param_no_or_name, const char (&parameter)[N]) {
                    return this->register_bound_param<P, std::string>(param_no_or_name, parameter, true, true);
                };
            protected:
                ///////////////////////////////////////////////////////////////
                //           FUNCTIONS TO BE DEFINED BY THE DRIVER           //
                ///////////////////////////////////////////////////////////////
                virtual int executer();

                virtual int fetcher(fetch_orientation ori, long offset);

                virtual int describer(int colno);

                virtual int get_col(int colno, std::string &ptr, int &caller_frees);

                virtual int param_hook(bound_param_data &attr, param_event val);

                virtual int set_attribute_func(long attr, driver_option &val);

                virtual int get_attribute(long attr, driver_option &val);

                virtual int get_column_meta(long colno, column_data &return_value);

                virtual int next_rowset_func();

                virtual int cursor_closer();

                ///////////////////////////////////////////////////////////////
                //       AUXILIARY FUNCTION THAT DO MOST OF THE REAL WORK    //
                ///////////////////////////////////////////////////////////////
                int parse_params(std::string &inquery, std::string &outquery);

                int dispatch_param_event(param_event event_type);

                int do_fetch(bool do_bind,
                             wpp::db::row &return_value,
                             enum fetch_orientation ori,
                             long offset);

                int do_fetch_common(enum fetch_orientation ori, long offset, bool do_bind);

                void fetch_value(std::unique_ptr<std::string> &dest, int colno);

                int generic_stmt_attr_get(driver_option &return_value, attribute_type attr);

                int generic_stmt_attr_get(driver_option &return_value, long attr);

                int do_next_rowset();

                int really_register_bound_param(bound_param_data &param, int is_param);

                int rewrite_name_to_position(struct bound_param_data &param);

                static const std::string sqlstate_state_to_description(std::string state);

                int describe_columns();

                int bind_input_parameters(std::vector<std::pair<std::string, std::string>> &input_params);

                int emulate_parameter_binding();

                int first_execution(int &);

                static int scan(scanner *s);

                void map_the_name_to_column(bound_param_data &param);

                void update_bound_columns();

                template<typename P, typename T>
                int register_bound_param(P param_no_or_name, T &parameter, const bool is_param, const bool make_copy) {
                    bound_param_data param;
                    if (!set_param_num_or_data<P>(param_no_or_name, param)) {
                        return false;
                    }
                    if (make_copy) {
                        param.parameter_data.reset(new std::string(std::move(data_object_statement::to_string<T>(parameter))));
                        param.parameter = (void *) (param.parameter_data.get());
                        param.parameter_typeinfo = typeid(std::string);
                        param.param_type = PARAM_STR;
                    } else {
                        param.parameter = (void *) (&parameter);
                        param.parameter_typeinfo = typeid(T);
                        param.param_type = data_object_param_type<T>();
                    }
                    if (!this->really_register_bound_param(param, is_param)) {
                        return false;
                    }
                    return true;
                }

                bool is_integer_hash(const size_t t) {
                    return t == typeid(char).hash_code() ||
                           t == typeid(short int).hash_code() ||
                           t == typeid(int).hash_code() ||
                           t == typeid(long int).hash_code() ||
                           t == typeid(long long int).hash_code() ||
                           t == typeid(unsigned char).hash_code() ||
                           t == typeid(unsigned short int).hash_code() ||
                           t == typeid(unsigned int).hash_code() ||
                           t == typeid(unsigned long int).hash_code() ||
                           t == typeid(unsigned long long int).hash_code();
                }

                bool is_floating_hash(const size_t t) {
                    return t == typeid(float).hash_code() ||
                           t == typeid(double).hash_code() ||
                           t == typeid(long double).hash_code();
                }

                bool is_bool_hash(const size_t t) {
                    return t == typeid(bool).hash_code();
                }

                bool is_string_hash(const size_t t) {
                    return t == typeid(std::string).hash_code();
                }

                bool is_nullptr_hash(const size_t t) {
                    return t == typeid(std::nullptr_t).hash_code();
                }

                std::string byte_to_string(const std::type_index t, void *data) {
                    if (t.hash_code() == typeid(char).hash_code()) {
                        return std::to_string(*((char *) data));
                    } else if (t.hash_code() == typeid(short int).hash_code()) {
                        return std::to_string(*((short int *) data));
                    } else if (t.hash_code() == typeid(int).hash_code()) {
                        return std::to_string(*((int *) data));
                    } else if (t.hash_code() == typeid(long int).hash_code()) {
                        return std::to_string(*((long int *) data));
                    } else if (t.hash_code() == typeid(long long int).hash_code()) {
                        return std::to_string(*((long long int *) data));
                    } else if (t.hash_code() == typeid(unsigned char).hash_code()) {
                        return std::to_string(*((unsigned char *) data));
                    } else if (t.hash_code() == typeid(unsigned short int).hash_code()) {
                        return std::to_string(*((unsigned short int *) data));
                    } else if (t.hash_code() == typeid(unsigned int).hash_code()) {
                        return std::to_string(*((unsigned int *) data));
                    } else if (t.hash_code() == typeid(unsigned long int).hash_code()) {
                        return std::to_string(*((unsigned long int *) data));
                    } else if (t.hash_code() == typeid(unsigned long long int).hash_code()) {
                        return std::to_string(*((unsigned long long int *) data));
                    } else if (t.hash_code() == typeid(float).hash_code()) {
                        return std::to_string(*((float *) data));
                    } else if (t.hash_code() == typeid(double).hash_code()) {
                        return std::to_string(*((double *) data));
                    } else if (t.hash_code() == typeid(long double).hash_code()) {
                        return std::to_string(*((long double *) data));
                    } else if (t.hash_code() == typeid(bool).hash_code()) {
                        return std::to_string(*((bool *) data));
                    } else if (t.hash_code() == typeid(std::string).hash_code()) {
                        return *((std::string *) data);
                    } else {
                        return "";
                    }
                }

                void string_to_byte(const std::type_index t, void *parameter_address, std::string &data) {
                    if (t.hash_code() == typeid(char).hash_code()) {
                        char *address_as_original_type = (char *) parameter_address;
                        *address_as_original_type = !data.empty() ? data[0] : ' ';
                    } else if (t.hash_code() == typeid(short int).hash_code()) {
                        short int *address_as_original_type = (short int *) parameter_address;
                        *address_as_original_type = (std::stoi(data));
                    } else if (t.hash_code() == typeid(int).hash_code()) {
                        int *address_as_original_type = (int *) parameter_address;
                        *address_as_original_type = (std::stoi(data));
                    } else if (t.hash_code() == typeid(long int).hash_code()) {
                        long int *address_as_original_type = (long int *) parameter_address;
                        *address_as_original_type = (std::stol(data));
                    } else if (t.hash_code() == typeid(long long int).hash_code()) {
                        long long int *address_as_original_type = (long long int *) parameter_address;
                        *address_as_original_type = (std::stoll(data));
                    } else if (t.hash_code() == typeid(unsigned char).hash_code()) {
                        unsigned char *address_as_original_type = (unsigned char *) parameter_address;
                        *address_as_original_type = (!data.empty() ? data[0] : ' ');
                    } else if (t.hash_code() == typeid(unsigned short int).hash_code()) {
                        unsigned short int *address_as_original_type = (unsigned short int *) parameter_address;
                        *address_as_original_type = (std::stoul(data));
                    } else if (t.hash_code() == typeid(unsigned int).hash_code()) {
                        unsigned int *address_as_original_type = (unsigned int *) parameter_address;
                        *address_as_original_type = (std::stoul(data));
                    } else if (t.hash_code() == typeid(unsigned long int).hash_code()) {
                        unsigned long int *address_as_original_type = (unsigned long int *) parameter_address;
                        *address_as_original_type = (std::stoull(data));
                    } else if (t.hash_code() == typeid(unsigned long long int).hash_code()) {
                        unsigned long long int *address_as_original_type = (unsigned long long int *) parameter_address;
                        *address_as_original_type = (std::stoull(data));
                    } else if (t.hash_code() == typeid(float).hash_code()) {
                        float *address_as_original_type = (float *) parameter_address;
                        *address_as_original_type = (std::stof(data));
                    } else if (t.hash_code() == typeid(double).hash_code()) {
                        double *address_as_original_type = (double *) parameter_address;
                        *address_as_original_type = (std::stod(data));
                    } else if (t.hash_code() == typeid(long double).hash_code()) {
                        long double *address_as_original_type = (long double *) parameter_address;
                        *address_as_original_type = (std::stold(data));
                    } else if (t.hash_code() == typeid(bool).hash_code()) {
                        bool *address_as_original_type = (bool *) parameter_address;
                        *address_as_original_type = ((data == "1" || data == "true" || data == "True" || data == "TRUE")
                                                     ? true : false);
                    } else if (t.hash_code() == typeid(std::string).hash_code()) {
                        std::string *address_as_original_type = (std::string *) parameter_address;
                        *address_as_original_type = std::string(data);
                    }
                }

                template<typename T>
                std::string
                to_string(std::enable_if_t<is_pure_int<T>::value || std::is_floating_point<T>::value, T> param) {
                    return std::to_string(param);
                }

                std::string to_string(bool param) {
                    return param ? "1" : "0";
                }

                template<typename T>
                std::string to_string(std::enable_if_t<!(is_pure_int<T>::value || std::is_floating_point<T>::value || std::is_same<T,std::nullptr_t>::value) &&
                                                       (std::is_assignable<std::string, T>::value ||
                                                        is_c_str<T>::value), T> param) {
                    return std::string(param);
                }

                template<typename T>
                std::string to_string(std::enable_if_t<std::is_same<T,std::nullptr_t>::value, T> param) {
                    return std::string("");
                }

                template<typename T>
                std::string to_string(std::enable_if_t<has_stream<T>::value, T> param) {
                    std::stringstream ss;
                    ss << param;
                    return ss.str();
                }

                template<typename T>
                std::string to_string(std::enable_if_t<std::is_pointer<T>::value && !is_c_str<T>::value, T> param) {
                    if (param) {
                        return to_string(*param);
                    } else {
                        return "nullptr";
                    }
                }

                template<typename T, std::enable_if_t<
                        std::is_same<const char *, T>::value || std::is_same<char[], T>::value, T> = 0>
                std::string to_string(T param) {
                    return std::string(param);
                }

                std::string to_string(const char *param) {
                    return std::string(param);
                }

                template<typename T>
                bool
                set_param_num_or_data(typename std::enable_if<std::is_integral<T>::value, T>::type &param_no_or_name,
                                      bound_param_data &param);

                template<typename T>
                bool
                set_param_num_or_data(typename std::enable_if<!std::is_integral<T>::value, T>::type &param_no_or_name,
                                      bound_param_data &param);

                template<typename T>
                constexpr param_type data_object_param_type() {
                    const size_t t = typeid(T).hash_code();
                    if (is_integer_hash(t)) {
                        return param_type::PARAM_INT;
                    } else if (is_floating_hash(t)) {
                        return param_type::PARAM_FLOAT;
                    } else if (is_bool_hash(t)) {
                        return param_type::PARAM_BOOL;
                    } else if (is_nullptr_hash(t)) {
                        return param_type::PARAM_NULL;
                    } else {
                        return param_type::PARAM_STR;
                    }
                }

                bool is_integer_type(const std::type_index t) {
                    return is_integer_hash(t.hash_code());
                }

                bool is_floating_type(const std::type_index t) {
                    return is_floating_hash(t.hash_code());
                }

                bool is_bool_type(const std::type_index t) {
                    return is_bool_hash(t.hash_code());
                }

                bool is_string_type(const std::type_index t) {
                    return !is_integer_hash(t.hash_code()) && !is_floating_hash(t.hash_code()) &&
                           !is_bool_hash(t.hash_code());
                }

            protected:
                ///////////////////////////////////////////////////////////////
                //                           DATA MEMBERS                    //
                ///////////////////////////////////////////////////////////////
                // settings
                int _supports_placeholders{placeholder_support::PLACEHOLDER_POSITIONAL};
                // parameters
                std::unordered_map<std::string, bound_param_data> _bound_param;
                std::unordered_map<std::string, std::string> _bound_param_map;
                std::unordered_map<std::string, bound_param_data> _bound_columns;
                // state
                bool _executed = false;
                std::vector<column_data> _columns;
                long _row_count;
                std::string _query_string;
                std::string _active_query_string;
                std::string _named_rewrite_template;
                // error state
                std::string _error_code = "000000";
                std::string _error_info = "";
                std::string _error_supp = "";
                // database
                data_object *_dbh;
        };

        ///////////////////////////////////////////////////////////////
        //                    DATA OBJECT CLASS                      //
        ///////////////////////////////////////////////////////////////
        class data_object {
            public:
                friend data_object_statement;

                template<typename T, typename T2>
                friend
                class data_object_crtp;

                using stmt = std::shared_ptr<wpp::db::data_object_statement>;
                using statement = std::shared_ptr<wpp::db::data_object_statement>;

                data_object(std::string _data_source = ":memory:",
                            std::string username = "",
                            std::string passwd = "",
                            std::unordered_map<attribute_type, driver_option> driver_options = {}) {};

                void data_object_factory(std::string _data_source,
                                         std::string username,
                                         std::string passwd,
                                         std::unordered_map<attribute_type, driver_option> driver_options = {});

                ///////////////////////////////////////////////////////////////
                //                    FUNCTIONS FOR THE USER                 //
                ///////////////////////////////////////////////////////////////
                bool begin_transaction();

                bool commit();

                bool roll_back();

                bool in_transaction();

                int set_attribute(error_mode value);

                int set_attribute(case_conversion value);

                int set_attribute(null_handling value);

                int set_attribute(attribute_type value, bool on_off);

                int set_attribute(attribute_type value, driver_option);

                driver_option get_attribute(attribute_type attribute);

                long exec(std::string query);

                std::string last_insert_id(std::string seqname = "");

                std::string error_code();

                std::string error_string();

                std::vector<std::string> error_info();

                std::string quote(std::string string, param_type paramtype = param_type::PARAM_STR);

                bool error() { return this->_error_code != "000000"; }

            protected:
                ///////////////////////////////////////////////////////////////
                //           FUNCTIONS TO BE DEFINED BY THE DRIVER           //
                ///////////////////////////////////////////////////////////////
                virtual int handle_factory(std::unordered_map<attribute_type, driver_option> options = {}) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support handle_factory");
                    return 0;
                }

                virtual int
                preparer(const std::string sql,
                         std::shared_ptr<data_object_statement> &stmt,
                         std::unordered_map<attribute_type, driver_option> driver_options = {}) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support preparer");
                    return false;
                }

                virtual long doer(const std::string sql) {
                    data_object::raise_impl_error(this, nullptr, "IM001",
                                                  "driver does not support doer(const std::string sql)");
                    return false;
                }

                virtual bool quoter(std::string unquoted,
                                    std::string &quoted,
                                    param_type paramtype = param_type::PARAM_STR) {
                    // ad hoc quoting
                    size_t start_pos = 0;
                    while ((start_pos = unquoted.find("'", start_pos)) != std::string::npos) {
                        unquoted.replace(start_pos, 1, "''");
                        start_pos += 2;
                    }
                    quoted = "'" + unquoted + "'";
                    return 1;
                };

                virtual int begin() {
                    throw std::logic_error("This driver doesn't support transactions");
                    return false;
                }

                virtual int commit_func() {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support commit_func");
                    return false;
                }

                virtual int rollback() {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support rollback");
                    return false;
                }

                virtual int set_attribute_func(long attr, const driver_option &val) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support set_attribute");
                    return false;
                }

                virtual std::string last_id(const std::string name) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support last_insert_id()");
                    return "";
                }

                virtual int fetch_err(const data_object_statement *stmt, std::vector<std::string> &info) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not implement fetch_err");
                    info.resize(3);
                    info[1] = "000000";
                    info[2] = "driver does not implement fetch_err";
                    return 0;
                }

                virtual int get_attribute(long attr, driver_option &val) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support getting attributes");
                    return false;
                }

                virtual int check_liveness() {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support check_liveness");
                    return false;
                }

                //virtual const function_entry *get_driver_methods(long kind);
                virtual void persistent_shutdown() {
                    data_object::raise_impl_error(this, nullptr, "IM001",
                                                  "driver does not support persistent_shutdown");
                }

                virtual int in_transaction_func() {
                    return (this->_in_txn);
                }

            protected:
                ///////////////////////////////////////////////////////////////
                //                          HELPERS                          //
                ///////////////////////////////////////////////////////////////
                static void raise_impl_error(data_object *dbh,
                                             data_object_statement *stmt,
                                             const std::string sqlstate,
                                             const std::string supp);

                static std::string addcslashes(std::string tmp, std::string what);

                static void handle_error(data_object &dbh, data_object_statement &stmt);

                static void handle_error(data_object &dbh);

                int attribute_set(attribute_type attr, driver_option value);

                ///////////////////////////////////////////////////////////////
                //                          MEMBERS                          //
                ///////////////////////////////////////////////////////////////
                // credentials
                std::string _username;
                std::string _password;
                std::string _data_source;
                // state
                unsigned _is_closed:1;
                unsigned _alloc_own_columns:1;
                unsigned _in_txn:1;
                std::string _persistent_id;
                unsigned int _refcount;
                std::shared_ptr<data_object_statement> _query_stmt;
                // error state
                error_mode _error_mode;
                std::string _error_code = "000000";
                std::string _error_info = "";
                std::string _error_supp = "";
                // settings
                static std::unordered_map<std::string, wpp::db::data_object *> persistent_list;
                std::string _driver_name;
                unsigned _is_persistent:1;
                unsigned _auto_commit:1;
                unsigned _max_escaped_char_length:3;
                null_handling _oracle_nulls:2;
                unsigned _stringify:1;
                case_conversion _native_case;
                case_conversion _desired_case;
            public:
                ///////////////////////////////////////////////////////////////
                //VIRTUALS FOR THE CRTP (SHOULD NOT BE DEFINED BY THE DRIVER)//
                ///////////////////////////////////////////////////////////////
                virtual std::shared_ptr<data_object_statement>
                prepare(std::string statement, std::unordered_map<attribute_type, driver_option> options = {}) {
                    std::shared_ptr<data_object_statement> stmt(new data_object_statement);
                    stmt->_query_string = statement;
                    stmt->_dbh = this;
                    if (this->preparer(statement, stmt, options)) {
                        return std::move(stmt);
                    }
                    if (this->_error_code != "000000") {
                        data_object::handle_error(*this, *stmt);
                    }
                    return nullptr;
                }

                virtual std::shared_ptr<data_object_statement> query(std::string sql) {
                    std::shared_ptr<data_object_statement> stmt(new data_object_statement);
                    const std::string statement = sql;
                    const size_t statement_len = sql.size();
                    this->_error_code = "000000";
                    if (this->_query_stmt) {
                        this->_query_stmt = nullptr;
                    }
                    stmt->_query_string = statement;
                    stmt->_active_query_string = stmt->_query_string;
                    stmt->_dbh = this;
                    if (this->preparer(statement, stmt)) {
                        stmt->_error_code = "000000";
                        if (stmt->executer()) {
                            int ret = 0;
                            if (!stmt->_executed) {
                                if (stmt->_dbh->_alloc_own_columns) {
                                    ret = stmt->describe_columns();
                                }
                            }
                            stmt->_executed = 1;
                            if (ret) {
                                return std::move(stmt);
                            }
                        }
                    }
                    this->_query_stmt = stmt;
                    if (stmt->_error_code != "000000") {
                        data_object::handle_error(*stmt->_dbh, *stmt);
                    }
                    return nullptr;
                }
        };

        std::unordered_map<std::string, wpp::db::data_object *> data_object::persistent_list{};

        template<typename derived_data_object, typename derived_statement>
        class data_object_crtp
                : public wpp::db::data_object {
            public:
                friend data_object_statement;

                data_object_crtp(std::string dsn = ":memory:", // that works for sqlite
                                 std::string username = "", // some databases don't require username and passwords
                                 std::string passwd = "",
                                 std::unordered_map<attribute_type, driver_option> options = {}) :
                        wpp::db::data_object(dsn, username, passwd, options) {
                };

                virtual std::shared_ptr<data_object_statement>
                prepare(std::string statement,
                        std::unordered_map<attribute_type, driver_option> options = {}) override {
                    std::shared_ptr<derived_statement> stmt(new derived_statement);
                    stmt->_query_string = statement;
                    stmt->_dbh = this;
                    if (this->preparer(statement, stmt, options)) {
                        return std::dynamic_pointer_cast<data_object_statement>(stmt);
                    }
                    if (this->_error_code != "000000") {
                        data_object::handle_error(*this, *stmt);
                    }
                    return nullptr;
                }

                virtual int
                preparer(const std::string sql,
                         std::shared_ptr<derived_statement> &stmt,
                         std::unordered_map<attribute_type, driver_option> driver_options = {}) {
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support preparer");
                    return false;
                }

                //virtual derived_statement query(std::string sql) = 0;
                virtual std::shared_ptr<data_object_statement> query(std::string statement) override {
                    std::shared_ptr<derived_statement> stmt(new derived_statement);
                    this->_error_code = "000000";
                    if (this->_query_stmt) {
                        this->_query_stmt = nullptr;
                    }
                    /* unconditionally keep this for later reference */
                    stmt->_query_string = statement;
                    stmt->_active_query_string = stmt->_query_string;
                    stmt->_dbh = this;
                    /* prepare the statement */
                    if (this->preparer(statement, stmt)) {
                        stmt->_error_code = "000000";
                        /* execute the statement */
                        if (stmt->executer()) {
                            int ret = 1;
                            if (!stmt->_executed) {
                                /* get column data */
                                if (stmt->_dbh->_alloc_own_columns) {
                                    ret = stmt->describe_columns();
                                }
                                stmt->_executed = 1;
                            }
                            if (ret) {
                                return std::dynamic_pointer_cast<data_object_statement>(stmt);
                            }
                        }
                    }
                    /* something broke */
                    this->_query_stmt = stmt;
                    if (stmt->_error_code != "000000") {
                        data_object::handle_error(*stmt->_dbh, *stmt);
                    }
                    return nullptr;
                }
        };

        ///////////////////////////////////////////////////////////////
        //                    STATEMENT DEFINITION                   //
        ///////////////////////////////////////////////////////////////
        template<typename T>
        bool
        data_object_statement::set_param_num_or_data(typename std::enable_if<std::is_integral<T>::value, T>::type &param_no_or_name,
                                                     bound_param_data &param) {
            param.paramno = param_no_or_name;
            if (param.paramno > 0) {
                --param.paramno; /* make it zero-based internally */
                return true;
            } else if (param.name.empty()) {
                data_object::raise_impl_error(this->_dbh, this, "HY093", "Columns/Parameters are 1-based");
                return false;
            }
            return false;
        }

        std::string data_object_statement::error_string() {
            std::vector<std::string> info = this->error_info();
            std::string return_value = "Error ";
            if (!info.empty()) {
                return_value += info[0];
                if (info.size() >= 2 && !info[1].empty()) {
                    return_value += ": " + info[1];
                    if (info.size() >= 3 && !info[2].empty()) {
                        return_value += ", " + info[2] + ".";
                    } else {
                        return_value += ".";
                    }
                    return return_value;
                } else {
                    std::string s = sqlstate_state_to_description(info[0]);
                    if (!s.empty()) {
                        return_value += ": " + s + ".";
                    } else {
                        return_value += ".";
                    }
                    return return_value;
                }
            }
            return return_value;
        };

        std::string data_object::error_string() {
            std::vector<std::string> info = this->error_info();
            std::string return_value = "Error ";
            if (!info.empty()) {
                return_value += info[0];
                if (info.size() >= 2 && !info[1].empty()) {
                    return_value += ": " + info[1];
                    if (info.size() >= 3 && !info[2].empty()) {
                        return_value += ", " + info[2] + ".";
                    } else {
                        return_value += ".";
                    }
                    return return_value;
                } else {
                    std::string s = data_object_statement::sqlstate_state_to_description(info[0]);
                    if (!s.empty()) {
                        return_value += ": " + s + ".";
                    } else {
                        return_value += ".";
                    }
                    return return_value;
                }
            }
            return return_value;
        };

        template<typename T>
        bool
        data_object_statement::set_param_num_or_data(typename std::enable_if<!std::is_integral<T>::value, T>::type &param_no_or_name,
                                                     bound_param_data &param) {
            param.paramno = -1;
            if (std::is_assignable<std::string, T>::value || has_stream<T>::value) {
                param.name = data_object_statement::to_string<T>(param_no_or_name);
                return true;
            } else {
                param.name.clear();
                data_object::raise_impl_error(this->_dbh, this, "HY093",
                                              std::string("Columns/Parameters cannot bind to ") + typeid(T).name());
                return false;
            }
        }

        int
        data_object_statement::bind_input_parameters(std::vector<std::pair<std::string, std::string>> &input_params) {
            if (!input_params.empty()) {
                bound_param_data param;
                this->_bound_param.clear();
                unsigned long num_index = 0;
                for (std::pair<std::string, std::string> &item : input_params) {
                    std::string key = item.first;
                    std::string tmp = item.second;
                    if (!key.empty()) {
                        param.name = key;
                        param.paramno = -1;
                    } else {
                        param.paramno = num_index;
                    }
                    param.param_type = param_type::PARAM_STR;
                    param.parameter_data.reset(new std::string(std::move(tmp)));
                    param.parameter = &param.parameter_data;
                    if (!really_register_bound_param(param, 1)) {
                        return false;
                    }
                }
            }
            return true;
        };

        int data_object_statement::emulate_parameter_binding() {
            this->_active_query_string.clear();
            int ret = parse_params(this->_query_string, this->_active_query_string);
            if (ret == 0) {
                /* no changes were made */
                this->_active_query_string = this->_query_string;
                ret = 1;
            } else if (ret == -1) {
                /* something broke */
                return false;
            }
            return ret;
        }

        int data_object_statement::first_execution(int &ret) {
            /* this is the first execute */
            if (this->_dbh->_alloc_own_columns && !this->_columns.empty()) {
                /* for "big boy" drivers, we need to allocate memory to fetch
                 * the results into, so lets do that now */
                ret = this->describe_columns();
            }
            this->_executed = true;
            return ret;
        }

        bool data_object_statement::execute(std::vector<std::pair<std::string, std::string>> input_params) {
            if (!this->_dbh) {
                return false;
            }
            if (!bind_input_parameters(input_params)) {
                return false;
            }
            const bool we_should_emulate_parameter_binding =
                    placeholder_support::PLACEHOLDER_NONE == this->_supports_placeholders;
            if (we_should_emulate_parameter_binding) {
                if (!this->emulate_parameter_binding()) {
                    return false;
                }
            } else if (!dispatch_param_event(param_event::PARAM_EVT_EXEC_PRE)) {
                return false;
            }
            int ret = 1;
            if (this->executer()) {
                if (!this->_executed) {
                    ret = first_execution(ret);
                }
                if (ret && !dispatch_param_event(param_event::PARAM_EVT_EXEC_POST)) {
                    return false;
                }
                return (bool) ret;
            }
            return false;
        }

        wpp::db::row data_object_statement::fetch(fetch_orientation ori, long off) {
            if (!this->_dbh) {
                return wpp::db::row();
            }
            wpp::db::row return_value;
            if (!this->do_fetch(true, return_value, ori, off)) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
                return wpp::db::row();
            }
            return return_value;
        }

        void data_object::handle_error(data_object &dbh, data_object_statement &stmt) {
            dbh._error_code = stmt._error_code;
            if (dbh._error_mode == ERRMODE_SILENT) {
                return;
            } else {
                std::string err = stmt._error_code;
                std::string msg = data_object_statement::sqlstate_state_to_description(err);
                if (msg.empty()) {
                    msg = "<<Unknown error>>";
                }
                std::string supp;
                long native_code = 0;
                std::string complete_message;
                std::vector<std::string> info;
                info.push_back(err);
                if (dbh.fetch_err(&stmt, info)) {
                    if (info.size() >= 2) {
                        native_code = std::regex_match(info[1], std::regex("^\\d+$")) ? stol(info[1]) : 0;
                    }
                    if (info.size() >= 3) {
                        supp = info[2];
                    }
                }
                std::stringstream ss;
                if (!supp.empty()) {
                    ss << boost::format("SQLSTATE[%s]: %s: %ld %s") % err % msg % native_code % supp;
                } else {
                    ss << boost::format("SQLSTATE[%s]: %s") % err % msg;
                }
                complete_message = ss.str();
                if (dbh._error_mode == error_mode::ERRMODE_WARNING) {
                    std::cerr << "Warning: " << complete_message << std::endl;
                } else if (dbh._error_mode == error_mode::ERRMODE_EXCEPTION) {
                    throw std::runtime_error(complete_message);
                }
            }
        }

        void data_object::handle_error(data_object &dbh) {
            if (dbh._error_mode == ERRMODE_SILENT) {
                return;
            } else {
                std::string err = dbh._error_code;
                std::string msg = data_object_statement::sqlstate_state_to_description(err);
                if (msg.empty()) {
                    msg = "<<Unknown error>>";
                }
                std::string supp;
                long native_code = 0;
                std::string complete_message;
                std::vector<std::string> info;
                info.push_back(err);
                if (dbh.fetch_err(nullptr, info)) {
                    if (info.size() >= 2) {
                        native_code = std::regex_match(info[1], std::regex("^\\d+$")) ? stol(info[1]) : 0;
                    }
                    if (info.size() >= 3) {
                        supp = info[2];
                    }
                }
                std::stringstream ss;
                if (!supp.empty()) {
                    ss << boost::format("SQLSTATE[%s]: %s: %ld %s") % err % msg % native_code % supp;
                } else {
                    ss << boost::format("SQLSTATE[%s]: %s") % err % msg;
                }
                complete_message = ss.str();
                if (dbh._error_mode == error_mode::ERRMODE_EXCEPTION) {
                    std::cerr << complete_message << std::endl;
                } else if (dbh._error_mode == error_mode::ERRMODE_EXCEPTION) {
                    throw std::runtime_error(complete_message);
                }
            }
        }

        int data_object_statement::do_fetch(bool do_bind,
                                            wpp::db::row &return_value,
                                            enum fetch_orientation ori,
                                            long offset) {
            if (!this->do_fetch_common(ori, offset, do_bind)) {
                return 0;
            }
            return_value.clear();
            return_value.reserve(this->_columns.size());
            for (size_t idx = 0; idx < this->_columns.size(); idx++) {
                std::unique_ptr<std::string> val;
                this->fetch_value(val, idx);
                return_value.push_back(this->_columns[idx].name, *val);
            }
            return 1;
        }

        void data_object_statement::update_bound_columns() {
            for (std::pair<const std::string, bound_param_data> &bound_column : this->_bound_columns) {
                bound_param_data &param = bound_column.second;
                if (param.paramno >= 0) {
                    std::unique_ptr<std::string> val;
                    this->fetch_value(val, param.paramno);
                    if (val) {
                        string_to_byte(param.parameter_typeinfo, param.parameter, *val);
                    }
                }
            }
        }

        int data_object_statement::do_fetch_common(enum fetch_orientation ori, long offset, bool do_bind) {
            if (!this->_executed) {
                return 0;
            }
            if (!data_object_statement::dispatch_param_event(PARAM_EVT_FETCH_PRE)) {
                return 0;
            }
            if (!this->fetcher(ori, offset)) {
                return 0;
            }
            /* some drivers might need to describe the columns now */
            if (this->_columns.empty() && !this->describe_columns()) {
                return 0;
            }
            if (!data_object_statement::dispatch_param_event(PARAM_EVT_FETCH_POST)) {
                return 0;
            }
            if (do_bind) {
                this->update_bound_columns();
            }
            return 1;
        }

        inline void data_object_statement::fetch_value(std::unique_ptr<std::string> &dest, int colno) {
            int caller_frees = 0;
            column_data &col = this->_columns[colno];
            std::string value;
            this->get_col(colno, value, caller_frees);
            if (!value.empty() && !(value.empty() && this->_dbh->_oracle_nulls == null_handling::NULL_EMPTY_STRING)) {
                dest.reset(new std::string(std::move(value)));
            } else {
                dest.reset(new std::string(""));
            }
        }

        wpp::db::result data_object_statement::fetch_all() {
            int error = 0;
            wpp::db::row data;
            wpp::db::result return_value;
            if (!error) {
                this->_error_code = "00000";
                if (!this->do_fetch(1, data, FETCH_ORI_NEXT, 0)) {
                    error = 2;
                }
            }
            if (!error) {
                do {
                    return_value.push_back(data);
                } while (this->do_fetch(1, data, FETCH_ORI_NEXT, 0));
            }
            if (error) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
                return_value.clear();
            }
            return return_value;
        }

        void data_object_statement::map_the_name_to_column(bound_param_data &param) {
            for (int i = 0; i < this->_columns.size(); i++) {
                if (this->_columns[i].name == param.name) {
                    param.paramno = i;
                    break;
                }
            }
            if (param.paramno == -1) {
                std::string tmp = "Did not find column name '" + param.name +
                                  "' in the defined columns; it will not be bound";
                data_object::raise_impl_error(this->_dbh, this, "HY000", tmp);
            }
        }

        int data_object_statement::really_register_bound_param(bound_param_data &param, int is_param) {
            const bool is_column = !is_param;
            const bool parameter_pointer_is_valid = param.parameter != nullptr;
            const bool parameter_points_to_internal_data = param.parameter != (void *) (&param.parameter_data);
            const bool param_is_reference = parameter_pointer_is_valid && !parameter_points_to_internal_data;
            param.stmt = this;
            param.is_param = is_param;
            std::unordered_map<std::string, bound_param_data> *hash = is_param ? &this->_bound_param
                                                                               : &this->_bound_columns;
            if (is_column && !param.name.empty() && !this->_columns.empty()) {
                this->map_the_name_to_column(param);
            }
            if (!param.name.empty()) {
                if (is_param && param.name[0] != ':') {
                    param.name = ":" + param.name;
                }
            }
            if (is_param && !this->rewrite_name_to_position(param)) {
                param.name.clear();
                return 0;
            }
            if (!this->param_hook(param, PARAM_EVT_NORMALIZE)) {
                param.name.clear();
                return 0;
            }
            if (param.paramno >= 0) {
                hash->erase(std::to_string(param.paramno));
            }
            /* allocate storage for the parameter, keyed by its "canonical" name */
            std::unordered_map<std::string, bound_param_data>::iterator pparam;
            if (!param.name.empty()) {
                pparam = hash->emplace(std::make_pair(param.name, std::move(param))).first;
            } else {
                pparam = hash->emplace(std::make_pair(std::to_string(param.paramno), std::move(param))).first;
            }
            /* tell the driver we just created a parameter */
            if (!this->param_hook(pparam->second, PARAM_EVT_ALLOC)) {
                /* undo storage allocation; the hash will free the parameter name if required */
                hash->erase(pparam);
                return 0;
            }
            return 1;
        }

        int data_object_statement::rewrite_name_to_position(struct bound_param_data &param) {
            if (!this->_bound_param_map.empty()) {
                /* rewriting :name to ? style.
                 * We need to fixup the parameter numbers on the parameters.
                 * If we find that a given named parameter has been used twice,
                 * we will raise an error, as we can't be sure that it is safe
                 * to bind multiple parameters onto the same zval in the underlying
                 * driver */
                int position = 0;
                if (!this->_named_rewrite_template.empty()) {
                    /* this is not an error here */
                    return 1;
                }
                if (param.name.empty()) {
                    /* do the reverse; map the parameter number to the name */
                    auto iter_name = this->_bound_param_map.find(std::to_string(param.paramno));
                    if (iter_name != this->_bound_param_map.end()) {
                        return 1;
                    } else {
                        data_object::raise_impl_error(this->_dbh, this, "HY093", "parameter was not defined");
                        return 0;
                    }
                }
                for (auto &&param_map : this->_bound_param_map) {
                    std::string name = param_map.first;
                    if (name != param.name) {
                        position++;
                        continue;
                    }
                    if (param.paramno >= 0) {
                        data_object::raise_impl_error(this->_dbh, this, "IM001",
                                                      "data_object refuses to handle repeating the same :named parameter for multiple positions with this driver, as it might be unsafe to do so.  Consider using a separate name for each parameter instead");
                        return -1;
                    }
                    param.paramno = position;
                    return 1;
                }
                data_object::raise_impl_error(this->_dbh, this, "HY093", "parameter was not defined");
                return 0;
            }
            return 1;
        }

        int data_object_statement::parse_params(std::string &inquery, std::string &outquery) {
            scanner s;
            size_t ptr = 0;
            size_t output_len;
            int t;
            uint32_t bindno = 0;
            int ret = 0;
            std::unordered_map<std::string, bound_param_data> params;
            std::unique_ptr<bound_param_data> param;
            int query_type = placeholder_support::PLACEHOLDER_NONE;
            std::vector<placeholder> placeholders;
            size_t plc;
            s.cur = &inquery[0u];
            s.end = &inquery[0u] + inquery.length() + 1;
            /* phase 1: look for args */
            while ((t = scan(&s)) != parser_option::PARSER_EOI) {
                if (t == PARSER_BIND || t == PARSER_BIND_POS) {
                    if (t == PARSER_BIND) {
                        int len = s.cur - s.tok;
                        if ((inquery < (s.cur - len)) && isalnum(*(s.cur - len - 1))) {
                            continue;
                        }
                        query_type |= PLACEHOLDER_NAMED;
                    } else {
                        query_type |= PLACEHOLDER_POSITIONAL;
                    }
                    placeholder p;
                    p.unquoted.resize(s.cur - s.tok);
                    std::copy(s.tok, s.cur, p.unquoted.begin());
                    p.position_in_query = s.cur - &inquery[0u] - p.unquoted.size();
                    p.bindno = bindno++;
                    placeholders.emplace_back(std::move(p));
                }
            }
            if (bindno == 0) {
                /* nothing to do; good! */
                return 0;
            }
            /* did the query make sense to me? */
            const bool the_query_doesnt_makes_sense = query_type == (PLACEHOLDER_NAMED | PLACEHOLDER_POSITIONAL);
            if (the_query_doesnt_makes_sense) {
                /* they mixed both types; punt */
                data_object::raise_impl_error(this->_dbh, this, "HY093", "mixed named and positional parameters");
                ret = -1;
                return ret;
            }
            const bool the_query_matches_native_syntax =
                    this->_supports_placeholders == query_type && this->_named_rewrite_template.empty();
            if (the_query_matches_native_syntax) {
                /* query matches native syntax */
                ret = 0;
                return ret;
            }
            if (!this->_named_rewrite_template.empty()) {
                /* magic/hack.
                 * We we pretend that the query was positional even if
                 * it was named so that we fall into the
                 * named rewrite case below.  Not too pretty,
                 * but it works. */
                query_type = PLACEHOLDER_POSITIONAL;
            }
            /* Do we have placeholders but no bound this->_bound_param */
            const bool placeholders_without_parameters = bindno && this->_bound_param.empty() &&
                                                         this->_supports_placeholders ==
                                                         placeholder_support::PLACEHOLDER_NONE;
            if (placeholders_without_parameters) {
                data_object::raise_impl_error(this->_dbh, this, "HY093", "no parameters were bound");
                ret = -1;
                return ret;
            }
            const bool placeholders_do_not_match_parameters =
                    !this->_bound_param.empty() && bindno != this->_bound_param.size() &&
                    this->_supports_placeholders == placeholder_support::PLACEHOLDER_NONE;
            if (placeholders_do_not_match_parameters) {
                /* extra bit of validation for instances when same this->_bound_param are bound more than once */
                if (query_type != PLACEHOLDER_POSITIONAL && bindno > this->_bound_param.size()) {
                    int ok = 1;
                    for (plc = 0; plc < placeholders.size(); ++plc) {
                        std::unordered_map<std::string, bound_param_data>::iterator param_iter = this->_bound_param.find(
                                std::string(placeholders[plc].unquoted));
                        if (param_iter != this->_bound_param.end()) {
                            ok = 0;
                            break;
                        }
                    }
                    if (ok) {
                        goto safe;
                    }
                } else {
                    data_object::raise_impl_error(this->_dbh, this, "HY093",
                                                  "number of bound variables does not match number of tokens");
                    ret = -1;
                    return ret;
                }
            }
            safe:
            /* what are we going to do ? */
            if (this->_supports_placeholders == placeholder_support::PLACEHOLDER_NONE) {
                /* query generation */
                /* let's quote all the values */
                for (plc = 0; plc < placeholders.size(); ++plc) {
                    std::unordered_map<std::string, bound_param_data>::iterator param_iter;
                    if (query_type == PLACEHOLDER_POSITIONAL) {
                        param_iter = this->_bound_param.find(
                                std::to_string(placeholders[plc].bindno));
                    } else {
                        param_iter = this->_bound_param.find(std::string(placeholders[plc].unquoted));
                    }
                    if (param_iter != this->_bound_param.end()) {
                        ret = -1;
                        data_object::raise_impl_error(this->_dbh, this, "HY093", "parameter was not defined");
                        return ret;
                    }
                    void *parameter = param_iter->second.parameter;
                    std::type_index parameter_type_index = param_iter->second.parameter_typeinfo;
                    enum param_type param_type = param->param_type;
                    std::string buf;
                    /* assume all types are nullable */
                    if (!parameter) {
                        param_type = PARAM_NULL;
                    }
                    switch (param_type) {
                        case PARAM_BOOL:
                            placeholders[plc].quoted = byte_to_string(parameter_type_index, parameter);
                            placeholders[plc].freeq = 0;
                            break;
                        case PARAM_INT:
                            placeholders[plc].quoted = byte_to_string(parameter_type_index, parameter);
                            placeholders[plc].freeq = 1;
                            break;
                        case PARAM_NULL:
                            placeholders[plc].quoted = "nullptr";
                            placeholders[plc].freeq = 0;
                            break;
                        default:
                            buf = byte_to_string(parameter_type_index, parameter);
                            if (!this->_dbh->quoter(buf, placeholders[plc].quoted, param_type)) {
                                /* bork */
                                ret = -1;
                                this->_dbh->_error_code = this->_error_code;
                                return ret;
                            }
                            placeholders[plc].freeq = 1;
                    }
                }
                rewrite:
                outquery.reserve(inquery.size() + placeholders.size() * 5);
                /* and build the query */
                plc = 0 /*current placeholder*/;
                ptr = 0 /*inquery position*/;
                do {
                    t = placeholders[plc].position_in_query - ptr;
                    if (t > 0) {
                        outquery += inquery.substr(ptr, t);
                    }
                    outquery += placeholders[plc].quoted;
                    ptr += t + placeholders[plc].unquoted.size();
                    ++plc;
                } while (plc < placeholders.size());
                t = inquery.size() - ptr;
                if (t > 0) {
                    outquery += inquery.substr(ptr, t);
                }
                ret = 1;
                return ret;
            } else if (query_type == PLACEHOLDER_POSITIONAL) {
                /* rewrite ? to :pdoX */
                std::string name;
                std::string idxbuf;
                std::string tmpl = !this->_named_rewrite_template.empty() ? this->_named_rewrite_template : ":pdo%d";
                int bind_no = 1;
                output_len = inquery.size();
                this->_bound_param_map.clear();
                for (plc = 0; plc < placeholders.size(); ++plc) {
                    int skip_map = 0;
                    std::string p;
                    name = placeholders[plc].unquoted;
                    std::stringstream ss;
                    /* check if bound parameter is already available */
                    auto p_iter = this->_bound_param_map.find(name);
                    const bool this_param_is_bound = p_iter != this->_bound_param_map.end();
                    if (this_param_is_bound) {
                        p = p_iter->second;
                    }
                    if (name == "?" || !this_param_is_bound) {
                        ss << boost::format(tmpl) % bind_no++;
                        idxbuf = ss.str();
                    } else {
                        idxbuf = p;
                        skip_map = 1;
                    }
                    placeholders[plc].quoted = idxbuf;
                    placeholders[plc].freeq = 1;
                    if (!skip_map) {
                        /* create a mapping */
                        if (!this->_named_rewrite_template.empty()) {
                            this->_bound_param_map[name] = idxbuf;
                        } else {
                            this->_bound_param_map[std::to_string(placeholders[plc].bindno)] = idxbuf;
                        }
                    }
                }
                goto rewrite;
            } else {
                /* rewrite :name to ? */
                for (plc = 0; plc < placeholders.size(); ++plc) {
                    std::string name = placeholders[plc].unquoted;
                    placeholders[plc].quoted = "?";
                }
                goto rewrite;
            }
            return ret;
        }

        wpp::db::field data_object_statement::fetch_column(long column_number) {
            if (!this->do_fetch_common(FETCH_ORI_NEXT, 0, true)) {
                if (this->_error_code == "00000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
                return wpp::db::field();
            }
            std::unique_ptr<std::string> return_value;
            this->fetch_value(return_value, column_number);
            if (return_value) {
                return *return_value;
            } else {
                return wpp::db::field();
            }
        }

        const std::string data_object_statement::sqlstate_state_to_description(std::string state) {
            static const std::unordered_map<std::string, std::string> err_hash = {
                    {"00000", "No error"},
                    {"01000", "Warning"},
                    {"01001", "Cursor operation conflict"},
                    {"01002", "Disconnect error"},
                    {"01003", "NULL value eliminated in set function"},
                    {"01004", "String data, right truncated"},
                    {"01006", "Privilege not revoked"},
                    {"01007", "Privilege not granted"},
                    {"01008", "Implicit zero bit padding"},
                    {"0100C", "Dynamic result sets returned"},
                    {"01P01", "Deprecated feature"},
                    {"01S00", "Invalid connection std::string attribute"},
                    {"01S01", "Error in row"},
                    {"01S02", "Option value changed"},
                    {"01S06", "Attempt to fetch before the result set returned the first rowset"},
                    {"01S07", "Fractional truncation"},
                    {"01S08", "Error saving File DSN"},
                    {"01S09", "Invalid keyword"},
                    {"02000", "No data"},
                    {"02001", "No additional dynamic result sets returned"},
                    {"03000", "Sql statement not yet complete"},
                    {"07002", "COUNT field incorrect"},
                    {"07005", "Prepared statement not a cursor-specification"},
                    {"07006", "Restricted data type attribute violation"},
                    {"07009", "Invalid descriptor index"},
                    {"07S01", "Invalid use of default parameter"},
                    {"08000", "Connection exception"},
                    {"08001", "Client unable to establish connection"},
                    {"08002", "Connection name in use"},
                    {"08003", "Connection does not exist"},
                    {"08004", "Server rejected the connection"},
                    {"08006", "Connection failure"},
                    {"08007", "Connection failure during transaction"},
                    {"08S01", "Communication link failure"},
                    {"09000", "Triggered action exception"},
                    {"0A000", "Feature not supported"},
                    {"0B000", "Invalid transaction initiation"},
                    {"0F000", "Locator exception"},
                    {"0F001", "Invalid locator specification"},
                    {"0L000", "Invalid grantor"},
                    {"0LP01", "Invalid grant operation"},
                    {"0P000", "Invalid role specification"},
                    {"21000", "Cardinality violation"},
                    {"21S01", "Insert value list does not match column list"},
                    {"21S02", "Degree of derived table does not match column list"},
                    {"22000", "Data exception"},
                    {"22001", "String data, right truncated"},
                    {"22002", "Indicator variable required but not supplied"},
                    {"22003", "Numeric value out of range"},
                    {"22004", "Null value not allowed"},
                    {"22005", "Error in assignment"},
                    {"22007", "Invalid datetime format"},
                    {"22008", "Datetime field overflow"},
                    {"22009", "Invalid time zone displacement value"},
                    {"2200B", "Escape character conflict"},
                    {"2200C", "Invalid use of escape character"},
                    {"2200D", "Invalid escape octet"},
                    {"2200F", "Zero length character std::string"},
                    {"2200G", "Most specific type mismatch"},
                    {"22010", "Invalid indicator parameter value"},
                    {"22011", "Substd::string error"},
                    {"22012", "Division by zero"},
                    {"22015", "Interval field overflow"},
                    {"22018", "Invalid character value for cast specification"},
                    {"22019", "Invalid escape character"},
                    {"2201B", "Invalid regular expression"},
                    {"2201E", "Invalid argument for logarithm"},
                    {"2201F", "Invalid argument for power function"},
                    {"2201G", "Invalid argument for width bucket function"},
                    {"22020", "Invalid limit value"},
                    {"22021", "Character not in repertoire"},
                    {"22022", "Indicator overflow"},
                    {"22023", "Invalid parameter value"},
                    {"22024", "Unterminated c std::string"},
                    {"22025", "Invalid escape sequence"},
                    {"22026", "String data, length mismatch"},
                    {"22027", "Trim error"},
                    {"2202E", "Array subscript error"},
                    {"22P01", "Floating point exception"},
                    {"22P02", "Invalid text representation"},
                    {"22P03", "Invalid binary representation"},
                    {"22P04", "Bad copy file format"},
                    {"22P05", "Untranslatable character"},
                    {"23000", "Integrity constraint violation"},
                    {"23001", "Restrict violation"},
                    {"23502", "Not null violation"},
                    {"23503", "Foreign key violation"},
                    {"23505", "Unique violation"},
                    {"23514", "Check violation"},
                    {"24000", "Invalid cursor state"},
                    {"25000", "Invalid transaction state"},
                    {"25001", "Active sql transaction"},
                    {"25002", "Branch transaction already active"},
                    {"25003", "Inappropriate access mode for branch transaction"},
                    {"25004", "Inappropriate isolation level for branch transaction"},
                    {"25005", "No active sql transaction for branch transaction"},
                    {"25006", "Read only sql transaction"},
                    {"25007", "Schema and data statement mixing not supported"},
                    {"25008", "Held cursor requires same isolation level"},
                    {"25P01", "No active sql transaction"},
                    {"25P02", "In failed sql transaction"},
                    {"25S01", "Transaction state"},
                    {"25S02", "Transaction is still active"},
                    {"25S03", "Transaction is rolled back"},
                    {"26000", "Invalid sql statement name"},
                    {"27000", "Triggered data change violation"},
                    {"28000", "Invalid authorization specification"},
                    {"2B000", "Dependent privilege descriptors still exist"},
                    {"2BP01", "Dependent objects still exist"},
                    {"2D000", "Invalid transaction termination"},
                    {"2F000", "Sql routine exception"},
                    {"2F002", "Modifying sql data not permitted"},
                    {"2F003", "Prohibited sql statement attempted"},
                    {"2F004", "Reading sql data not permitted"},
                    {"2F005", "Function executed no return statement"},
                    {"34000", "Invalid cursor name"},
                    {"38000", "External routine exception"},
                    {"38001", "Containing sql not permitted"},
                    {"38002", "Modifying sql data not permitted"},
                    {"38003", "Prohibited sql statement attempted"},
                    {"38004", "Reading sql data not permitted"},
                    {"39000", "External routine invocation exception"},
                    {"39001", "Invalid sqlstate returned"},
                    {"39004", "Null value not allowed"},
                    {"39P01", "Trigger protocol violated"},
                    {"39P02", "Srf protocol violated"},
                    {"3B000", "Savepoint exception"},
                    {"3B001", "Invalid savepoint specification"},
                    {"3C000", "Duplicate cursor name"},
                    {"3D000", "Invalid catalog name"},
                    {"3F000", "Invalid schema name"},
                    {"40000", "Transaction rollback"},
                    {"40001", "Serialization failure"},
                    {"40002", "Transaction integrity constraint violation"},
                    {"40003", "Statement completion unknown"},
                    {"40P01", "Deadlock detected"},
                    {"42000", "Syntax error or access violation"},
                    {"42501", "Insufficient privilege"},
                    {"42601", "Syntax error"},
                    {"42602", "Invalid name"},
                    {"42611", "Invalid column definition"},
                    {"42622", "Name too long"},
                    {"42701", "Duplicate column"},
                    {"42702", "Ambiguous column"},
                    {"42703", "Undefined column"},
                    {"42704", "Undefined object"},
                    {"42710", "Duplicate object"},
                    {"42712", "Duplicate alias"},
                    {"42723", "Duplicate function"},
                    {"42725", "Ambiguous function"},
                    {"42803", "Grouping error"},
                    {"42804", "Datatype mismatch"},
                    {"42809", "Wrong object type"},
                    {"42830", "Invalid foreign key"},
                    {"42846", "Cannot coerce"},
                    {"42883", "Undefined function"},
                    {"42939", "Reserved name"},
                    {"42P01", "Undefined table"},
                    {"42P02", "Undefined parameter"},
                    {"42P03", "Duplicate cursor"},
                    {"42P04", "Duplicate database"},
                    {"42P05", "Duplicate prepared statement"},
                    {"42P06", "Duplicate schema"},
                    {"42P07", "Duplicate table"},
                    {"42P08", "Ambiguous parameter"},
                    {"42P09", "Ambiguous alias"},
                    {"42P10", "Invalid column reference"},
                    {"42P11", "Invalid cursor definition"},
                    {"42P12", "Invalid database definition"},
                    {"42P13", "Invalid function definition"},
                    {"42P14", "Invalid prepared statement definition"},
                    {"42P15", "Invalid schema definition"},
                    {"42P16", "Invalid table definition"},
                    {"42P17", "Invalid object definition"},
                    {"42P18", "Indeterminate datatype"},
                    {"42S01", "Base table or view already exists"},
                    {"42S02", "Base table or view not found"},
                    {"42S11", "Index already exists"},
                    {"42S12", "Index not found"},
                    {"42S21", "Column already exists"},
                    {"42S22", "Column not found"},
                    {"44000", "WITH CHECK OPTION violation"},
                    {"53000", "Insufficient resources"},
                    {"53100", "Disk full"},
                    {"53200", "Out of memory"},
                    {"53300", "Too many connections"},
                    {"54000", "Program limit exceeded"},
                    {"54001", "Statement too complex"},
                    {"54011", "Too many columns"},
                    {"54023", "Too many arguments"},
                    {"55000", "Object not in prerequisite state"},
                    {"55006", "Object in use"},
                    {"55P02", "Cant change runtime param"},
                    {"55P03", "Lock not available"},
                    {"57000", "Operator intervention"},
                    {"57014", "Query canceled"},
                    {"57P01", "Admin shutdown"},
                    {"57P02", "Crash shutdown"},
                    {"57P03", "Cannot connect now"},
                    {"58030", "Io error"},
                    {"58P01", "Undefined file"},
                    {"58P02", "Duplicate file"},
                    {"F0000", "Config file error"},
                    {"F0001", "Lock file exists"},
                    {"HY000", "General error"},
                    {"HY001", "Memory allocation error"},
                    {"HY003", "Invalid application buffer type"},
                    {"HY004", "Invalid SQL data type"},
                    {"HY007", "Associated statement is not prepared"},
                    {"HY008", "Operation canceled"},
                    {"HY009", "Invalid use of null pointer"},
                    {"HY010", "Function sequence error"},
                    {"HY011", "Attribute cannot be set now"},
                    {"HY012", "Invalid transaction operation code"},
                    {"HY013", "Memory management error"},
                    {"HY014", "Limit on the number of handles exceeded"},
                    {"HY015", "No cursor name available"},
                    {"HY016", "Cannot modify an implementation row descriptor"},
                    {"HY017", "Invalid use of an automatically allocated descriptor handle"},
                    {"HY018", "Server declined cancel request"},
                    {"HY019", "Non-character and non-binary data sent in pieces"},
                    {"HY020", "Attempt to concatenate a null value"},
                    {"HY021", "Inconsistent descriptor information"},
                    {"HY024", "Invalid attribute value"},
                    {"HY090", "Invalid std::string or buffer length"},
                    {"HY091", "Invalid descriptor field identifier"},
                    {"HY092", "Invalid attribute/option identifier"},
                    {"HY093", "Invalid parameter number"},
                    {"HY095", "Function type out of range"},
                    {"HY096", "Invalid information type"},
                    {"HY097", "Column type out of range"},
                    {"HY098", "Scope type out of range"},
                    {"HY099", "Nullable type out of range"},
                    {"HY100", "Uniqueness option type out of range"},
                    {"HY101", "Accuracy option type out of range"},
                    {"HY103", "Invalid retrieval code"},
                    {"HY104", "Invalid precision or scale value"},
                    {"HY105", "Invalid parameter type"},
                    {"HY106", "Fetch type out of range"},
                    {"HY107", "Row value out of range"},
                    {"HY109", "Invalid cursor position"},
                    {"HY110", "Invalid driver completion"},
                    {"HY111", "Invalid bookmark value"},
                    {"HYC00", "Optional feature not implemented"},
                    {"HYT00", "Timeout expired"},
                    {"HYT01", "Connection timeout expired"},
                    {"IM001", "Driver does not support this function"},
                    {"IM002", "Data source name not found and no default driver specified"},
                    {"IM003", "Specified driver could not be loaded"},
                    {"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"},
                    {"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"},
                    {"IM006", "Driver's SQLSetConnectAttr failed"},
                    {"IM007", "No data source or driver specified; dialog prohibited"},
                    {"IM008", "Dialog failed"},
                    {"IM009", "Unable to load translation DLL"},
                    {"IM010", "Data source name too long"},
                    {"IM011", "Driver name too long"},
                    {"IM012", "DRIVER keyword syntax error"},
                    {"IM013", "Trace file error"},
                    {"IM014", "Invalid name of File DSN"},
                    {"IM015", "Corrupt file data source"},
                    {"P0000", "Plpgsql error"},
                    {"P0001", "Raise exception"},
                    {"XX000", "Internal error"},
                    {"XX001", "Data corrupted"},
                    {"XX002", "Index corrupted"}
            };
            std::unordered_map<std::string, std::string>::const_iterator info_iter = err_hash.find(state);
            if (info_iter != err_hash.end()) {
                return info_iter->second;
            }
            return "";
        }

        int data_object_statement::executer() {
            data_object::raise_impl_error(this->_dbh, nullptr, "IM001", "driver does not implement executer");
            return 0;
        }

        int data_object_statement::fetcher(fetch_orientation ori, long offset) {
            data_object::raise_impl_error(this->_dbh, nullptr, "IM001", "driver does not implement fetcher");
            return 0;
        }

        int data_object_statement::describer(int colno) {
            data_object::raise_impl_error(this->_dbh, nullptr, "IM001", "driver does not implement describer");
            return 0;
        }

        int data_object_statement::get_col(int colno, std::string &ptr, int &caller_frees) {
            data_object::raise_impl_error(this->_dbh, nullptr, "IM001", "driver does not implement get_col");
            return 0;
        }

        int data_object_statement::param_hook(bound_param_data &attr, param_event val) {
            return 0;
        }

        int data_object_statement::dispatch_param_event(param_event event_type) {
            for (auto &&item : this->_bound_param) {
                bound_param_data &param = item.second;
                if (!this->param_hook(param, event_type)) {
                    return 0;
                    break;
                }
            }
            for (auto &&item : this->_bound_columns) {
                bound_param_data &param = item.second;
                if (!this->param_hook(param, event_type)) {
                    return 0;
                }
            }
            return 1;
        }

        int data_object_statement::describe_columns() {
            int col;
            for (col = 0; col < this->_columns.size(); col++) {
                if (!this->describer(col)) {
                    return 0;
                }
                if (this->_dbh->_native_case != this->_dbh->_desired_case &&
                    this->_dbh->_desired_case != CASE_NATURAL) {
                    std::string s = this->_columns[col].name;
                    switch (this->_dbh->_desired_case) {
                        case CASE_UPPER:
                            boost::algorithm::to_upper(s);
                            break;
                        case CASE_LOWER:
                            boost::algorithm::to_lower(s);
                            break;
                        default:;
                    }
                }
                if (!this->_bound_columns.empty()) {
                    struct bound_param_data *param;
                    auto iter = this->_bound_columns.find(this->_columns[col].name);
                    param = &iter->second;
                    if (param != nullptr) {
                        param->paramno = col;
                    }
                }
            }
            return 1;
        }

        int data_object_statement::scan(scanner *s) {
            char *cursor = s->cur;
            s->tok = cursor;
            {
                unsigned char yych;
                if ((s->end - cursor) < 2) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case 0x00:
                        goto yy2;
                    case '"':
                        goto yy3;
                    case '\'':
                        goto yy5;
                    case '(':
                    case ')':
                    case '*':
                    case '+':
                    case ',':
                    case '.':
                        goto yy9;
                    case '-':
                        goto yy10;
                    case '/':
                        goto yy11;
                    case ':':
                        goto yy6;
                    case '?':
                        goto yy7;
                    default:
                        goto yy12;
                }
                yy2:
                cursor = s->ptr;
                goto yy4;
                yy3:
                yych = *(s->ptr = ++cursor);
                if (yych >= 0x01) {
                    goto yy37;
                }
                yy4:
                {
                    {
                        s->cur = s->tok + 1;
                        return PARSER_TEXT;
                    }
                }
                yy5:
                yych = *(s->ptr = ++cursor);
                if (yych <= 0x00) {
                    goto yy4;
                }
                goto yy32;
                yy6:
                yych = *++cursor;
                switch (yych) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'A':
                    case 'B':
                    case 'C':
                    case 'D':
                    case 'E':
                    case 'F':
                    case 'G':
                    case 'H':
                    case 'I':
                    case 'J':
                    case 'K':
                    case 'L':
                    case 'M':
                    case 'N':
                    case 'O':
                    case 'P':
                    case 'Q':
                    case 'R':
                    case 'S':
                    case 'T':
                    case 'U':
                    case 'V':
                    case 'W':
                    case 'X':
                    case 'Y':
                    case 'Z':
                    case '_':
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                    case 'i':
                    case 'j':
                    case 'k':
                    case 'l':
                    case 'm':
                    case 'n':
                    case 'o':
                    case 'p':
                    case 'q':
                    case 'r':
                    case 's':
                    case 't':
                    case 'u':
                    case 'v':
                    case 'w':
                    case 'x':
                    case 'y':
                    case 'z':
                        goto yy26;
                    case ':':
                        goto yy29;
                    default:
                        goto yy4;
                }
                yy7:
                ++cursor;
                switch ((yych = *cursor)) {
                    case '?':
                        goto yy23;
                    default:
                        goto yy8;
                }
                yy8:
                {
                    {
                        s->cur = cursor;
                        return PARSER_BIND_POS;
                    }
                }
                yy9:
                yych = *++cursor;
                goto yy4;
                yy10:
                yych = *++cursor;
                switch (yych) {
                    case '-':
                        goto yy21;
                    default:
                        goto yy4;
                }
                yy11:
                yych = *(s->ptr = ++cursor);
                switch (yych) {
                    case '*':
                        goto yy15;
                    default:
                        goto yy4;
                }
                yy12:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case 0x00:
                    case '"':
                    case '\'':
                    case '(':
                    case ')':
                    case '*':
                    case '+':
                    case ',':
                    case '-':
                    case '.':
                    case '/':
                    case ':':
                    case '?':
                        goto yy14;
                    default:
                        goto yy12;
                }
                yy14:
                {
                    {
                        s->cur = cursor;
                        return PARSER_TEXT;
                    }
                }
                yy15:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case '*':
                        goto yy17;
                    default:
                        goto yy15;
                }
                yy17:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case '*':
                        goto yy17;
                    case '/':
                        goto yy19;
                    default:
                        goto yy15;
                }
                yy19:
                ++cursor;
                yy20:
                {
                    {
                        s->cur = cursor;
                        return PARSER_TEXT;
                    }
                }
                yy21:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case '\n':
                    case '\r':
                        goto yy20;
                    default:
                        goto yy21;
                }
                yy23:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case '?':
                        goto yy23;
                    default:
                        goto yy25;
                }
                yy25:
                {
                    {
                        s->cur = cursor;
                        return PARSER_TEXT;
                    }
                }
                yy26:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'A':
                    case 'B':
                    case 'C':
                    case 'D':
                    case 'E':
                    case 'F':
                    case 'G':
                    case 'H':
                    case 'I':
                    case 'J':
                    case 'K':
                    case 'L':
                    case 'M':
                    case 'N':
                    case 'O':
                    case 'P':
                    case 'Q':
                    case 'R':
                    case 'S':
                    case 'T':
                    case 'U':
                    case 'V':
                    case 'W':
                    case 'X':
                    case 'Y':
                    case 'Z':
                    case '_':
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                    case 'i':
                    case 'j':
                    case 'k':
                    case 'l':
                    case 'm':
                    case 'n':
                    case 'o':
                    case 'p':
                    case 'q':
                    case 'r':
                    case 's':
                    case 't':
                    case 'u':
                    case 'v':
                    case 'w':
                    case 'x':
                    case 'y':
                    case 'z':
                        goto yy26;
                    default:
                        goto yy28;
                }
                yy28:
                {
                    {
                        s->cur = cursor;
                        return PARSER_BIND;
                    }
                }
                yy29:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                switch (yych) {
                    case ':':
                        goto yy29;
                    default:
                        goto yy25;
                }
                yy31:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                yy32:
                switch (yych) {
                    case 0x00:
                        goto yy2;
                    case '\'':
                        goto yy34;
                    case '\\':
                        goto yy33;
                    default:
                        goto yy31;
                }
                yy33:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                if (yych <= 0x00) {
                    goto yy2;
                }
                goto yy31;
                yy34:
                ++cursor;
                {
                    {
                        s->cur = cursor;
                        return PARSER_TEXT;
                    }
                }
                yy36:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                yy37:
                switch (yych) {
                    case 0x00:
                        goto yy2;
                    case '"':
                        goto yy39;
                    case '\\':
                        goto yy38;
                    default:
                        goto yy36;
                }
                yy38:
                ++cursor;
                if (s->end <= cursor) {
                    s->cur = cursor;
                    return 4;
                }
                yych = *cursor;
                if (yych <= 0x00) {
                    goto yy2;
                }
                goto yy36;
                yy39:
                ++cursor;
                {
                    {
                        s->cur = cursor;
                        return PARSER_TEXT;
                    }
                }
            }
        }

        std::vector<std::string> data_object_statement::error_info() {
            int error_expected_count = 3;
            std::vector<std::string> return_value;
            return_value.push_back(this->_error_code);
            this->_dbh->fetch_err(this, return_value);
            if (return_value.size() == 1) {
            }
            const int error_count = return_value.size();
            if (error_expected_count > error_count) {
                int error_count_diff = error_expected_count - error_count;
                for (int current_index = 0; current_index < error_count_diff; current_index++) {
                    return_value.push_back("");
                }
            }
            return return_value;
        }

        int data_object_statement::set_attribute_func(long attr, driver_option &val) {
            data_object::raise_impl_error(this->_dbh, this, "IM001",
                                          "This driver doesn't support setting attributes");
            return false;
        }

        bool data_object_statement::set_attribute(long attribute, driver_option value) {
            this->_error_code = "000000";
            if (this->set_attribute_func(attribute, value)) {
                return true;
            } else {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
            }
            return false;
        }

        int data_object_statement::generic_stmt_attr_get(driver_option &return_value, attribute_type attr) {
            switch (attr) {
                case attribute_type::ATTR_EMULATE_PREPARES:
                    return_value = (this->_supports_placeholders == PLACEHOLDER_NONE);
                    return 1;
                default:
                    return 0;
            }
            return 0;
        }

        int data_object_statement::generic_stmt_attr_get(driver_option &return_value, long attr) {
            return generic_stmt_attr_get(return_value, attr);
        }

        int data_object_statement::get_attribute(long attr, driver_option &val) {
            driver_option return_value;
            if (!this->generic_stmt_attr_get(return_value, (attribute_type) attr)) {
                data_object::raise_impl_error(this->_dbh, this, "IM001",
                                              "This driver doesn't support getting attributes");
                val = return_value;
                return -1;
            } else {
                return 0;
            }
            return 1;
        }

        driver_option data_object_statement::get_attribute(long attribute) {
            driver_option return_value;
            this->_error_code = "000000";
            switch (this->get_attribute(attribute, return_value)) {
                case -1:
                    if (this->_error_code != "000000") {
                        data_object::handle_error(*this->_dbh, *this);
                    }
                    return return_value;
                case 0:
                    if (!this->generic_stmt_attr_get(return_value, attribute)) {
                        data_object::raise_impl_error(this->_dbh, this, "IM001",
                                                      "driver doesn't support getting that attribute");
                        return "";
                    }
                    return return_value;
                default:
                    return return_value;
            }
        }

        int data_object_statement::get_column_meta(long colno, column_data &return_value) {
            data_object::raise_impl_error(this->_dbh, this, "IM001", "driver doesn't support meta data");
            return false;
        }

        column_data data_object_statement::get_column_meta(long column) {
            const long colno = column;
            column_data return_value;
            if (colno < 0) {
                data_object::raise_impl_error(this->_dbh, this, "42P10", "column number must be non-negative");
                return {};
            }
            this->_error_code = "000000";
            if (!this->get_column_meta(colno, return_value)) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
                return {};
            }
            return return_value;
        }

        int data_object_statement::do_next_rowset() {
            this->_columns.clear();
            if (!this->next_rowset()) {
                this->_executed = false;
                return 0;
            }
            this->describe_columns();
            return 1;
        }

        int data_object_statement::next_rowset_func() {
            data_object::raise_impl_error(this->_dbh, this, "IM001", "driver does not support multiple rowsets");
            return false;
        }

        bool data_object_statement::next_rowset() {
            if (!this->_dbh) {
                return false;
            }
            this->_error_code = "000000";
            if (!this->next_rowset_func()) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
                return false;
            }
            return true;
        }

        int data_object_statement::cursor_closer() {
            do {
                while (this->fetcher(FETCH_ORI_NEXT, 0)) {
                }
                if (!this->do_next_rowset()) {
                    break;
                }
            } while (1);
            this->_executed = false;
            return true;
        }

        bool data_object_statement::close_cursor() {
            if (!this->_dbh) {
                return false;
            }
            this->_error_code = "000000";
            if (!this->cursor_closer()) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this->_dbh, *this);
                }
                return false;
            }
            this->_executed = false;
            return true;
        }

        ///////////////////////////////////////////////////////////////
        //                    CONNECTION DEFINITION                  //
        ///////////////////////////////////////////////////////////////
        void data_object::data_object_factory(std::string data_source,
                                              std::string username,
                                              std::string password,
                                              std::unordered_map<attribute_type, driver_option> options) {
            bool is_persistent = 0;
            std::string alt_dsn(512, ' ');
            int call_factory = 1;
            size_t colon_pos = data_source.find(':');
            if (colon_pos != std::string::npos) {
                this->_driver_name = data_source.substr(0, colon_pos);
            } else {
                this->_driver_name = "data_object";
            }
            if (!options.empty()) {
                std::string hashkey;
                data_object *pdbh = nullptr;
                if (options.count(attribute_type::ATTR_PERSISTENT)) {
                    driver_option v = options[attribute_type::ATTR_PERSISTENT];
                    if (v.which() == 1 && !std::regex_match(boost::get<std::string>(v), std::regex("[\\d\\+-\\.]+")) &&
                        !boost::get<std::string>(v).empty()) {
                        hashkey = "DBH:DSN=" + data_source + ":" + username + ":" + password + ":" +
                                  boost::get<std::string>(v);
                        is_persistent = 1;
                    } else {
                        is_persistent = boost::get<int>(v) ? 1 : 0;
                        hashkey = "DBH:DSN=" + data_source + ":" + username + ":" + password;
                    }
                }
                if (is_persistent) {
                    std::unordered_map<std::string, data_object *>::iterator it = persistent_list.find(hashkey);
                    if (it != persistent_list.end()) {
                        pdbh = it->second;
                        const bool connection_is_alive = !pdbh || (pdbh && !pdbh->check_liveness());
                        if (connection_is_alive) {
                            if (pdbh) {
                                pdbh->_refcount--;
                            } else {
                                persistent_list.erase(it);
                            }
                        }
                    }
                    if (pdbh) {
                        call_factory = 0;
                    } else {
                        this->_refcount = 1;
                        this->_is_persistent = 1;
                        this->_persistent_id = hashkey;
                    }
                }
                if (pdbh) {
                    pdbh->_refcount++;
                    *this = *pdbh;
                }
            }
            if (call_factory) {
                if (colon_pos != std::string::npos) {
                    this->_data_source = data_source.substr(colon_pos + 1);
                } else {
                    this->_data_source = data_source;
                }
                this->_username = username;
                this->_password = password;
            }
            auto iter = options.find(ATTR_AUTOCOMMIT);
            if (iter == options.end()) {
                this->_auto_commit = 1;
            } else {
                this->_auto_commit = iter->second.get_int();
            }
            if (this->_data_source.empty()) {
                throw std::runtime_error("Improper dns data source");
            }
            bool continue_constructing = false;
            if (call_factory && this->handle_factory(options)) {
                if (is_persistent) {
                    persistent_list[this->_persistent_id] = this;
                }
                continue_constructing = true;
            }
            if (!call_factory || continue_constructing) {
                if (!options.empty()) {
                    driver_option attr_value;
                    for (auto &&option : options) {
                        this->set_attribute_func(option.first, option.second);
                    }
                }
                return;
            }
        }

        void data_object::raise_impl_error(data_object *dbh,
                                           data_object_statement *stmt,
                                           const std::string sqlstate, // new code
                                           const std::string supp) {
            std::string err = dbh->_error_code;
            std::string message = "";
            std::string msg;
            if (stmt) {
                err = stmt->_error_code;
            }
            if (!sqlstate.empty()) {
                err = sqlstate;
            }
            msg = data_object_statement::sqlstate_state_to_description(err);
            if (msg.empty()) {
                msg = "<<Unknown error>>";
            }
            std::stringstream ss;
            if (!supp.empty()) {
                ss << boost::format("SQLSTATE[%s]: %s: %s") % err % msg % supp;
            } else {
                ss << boost::format("SQLSTATE[%s]: %s") % err % msg;
            }
            message = ss.str();
            if (stmt) {
                stmt->_error_code = err;
                stmt->_error_info = msg;
                stmt->_error_supp = message;
            }
            dbh->_error_code = err;
            dbh->_error_info = msg;
            dbh->_error_supp = message;
        }

        bool data_object::begin_transaction() {
            if (this->_in_txn) {
                throw std::runtime_error("There is already an active transaction");
                return false;
            }
            if (this->begin()) {
                this->_in_txn = 1;
                return true;
            }
            if (this->_error_code != "000000") {
                data_object::handle_error(*this);
            }
            return false;
        }

        bool data_object::commit() {
            if (!this->_in_txn) {
                throw std::runtime_error("There is no active transaction");
                return false;
            }
            if (this->commit_func()) {
                this->_in_txn = 0;
                return true;
            }
            if (this->_error_code != "000000") {
                data_object::handle_error(*this);
            }
            return false;
        }

        bool data_object::roll_back() {
            if (!this->_in_txn) {
                throw std::runtime_error("There is no active transaction");
                return false;
            }
            if (this->rollback()) {
                this->_in_txn = false;
                return true;
            }
            if (this->_error_code != "000000") {
                data_object::handle_error(*this);
            }
            return false;
        }

        bool data_object::in_transaction() {
            return (this->in_transaction_func());
        }

        int data_object::set_attribute(error_mode value) {
            switch (value) {
                case ERRMODE_SILENT:
                case ERRMODE_WARNING:
                case ERRMODE_EXCEPTION:
                    this->_error_mode = value;
                    return true;
                default:
                    data_object::raise_impl_error(this, nullptr, "HY000", "invalid error mode");
                    if (this->_error_code != "000000") {
                        data_object::handle_error(*this);
                    }
                    return false;
            }
            return false;
        }

        int data_object::set_attribute(case_conversion value) {
            switch (value) {
                case CASE_NATURAL:
                case CASE_UPPER:
                case CASE_LOWER:
                    this->_desired_case = value;
                    return true;
                default:
                    data_object::raise_impl_error(this, nullptr, "HY000", "invalid case folding mode");
                    if (this->_error_code != "000000") {
                        data_object::handle_error(*this);
                    }
                    return false;
            }
            return false;
        }

        int data_object::set_attribute(null_handling value) {
            this->_oracle_nulls = value;
            return true;
        }

        int data_object::set_attribute(attribute_type value, bool on_off) {
            if (value == ATTR_STRINGIFY_FETCHES) {
                this->_stringify = on_off;
                return true;
            }
            return false;
        }

        int data_object::set_attribute(attribute_type value, driver_option on_off) {
            if (value == ATTR_STRINGIFY_FETCHES) {
                this->_stringify = on_off.get_int();
                return true;
            }
            return false;
        }

        driver_option data_object::get_attribute(attribute_type attribute) {
            driver_option o;
            switch (attribute) {
                case ATTR_PERSISTENT:
                    return driver_option(this->_is_persistent);
                case ATTR_CASE:
                    return driver_option(this->_desired_case);
                case ATTR_ORACLE_NULLS:
                    return driver_option(this->_oracle_nulls);
                case ATTR_ERRMODE:
                    return driver_option(this->_error_mode);
                case ATTR_AUTOCOMMIT:
                    return driver_option(this->_auto_commit);
                default:
                    break;
            }
            driver_option return_value;
            switch (this->get_attribute(attribute, return_value)) {
                case -1:
                    if (this->_error_code != "000000") {
                        data_object::handle_error(*this);
                    }
                    return false;
                case 0:
                    data_object::raise_impl_error(this, nullptr, "IM001", "driver does not support that attribute");
                    return false;
                default:
                    return false;
            }
            return false;
        }

        long data_object::exec(std::string query) {
            const std::string &statement = query;
            long ret;
            if (query.empty()) {
                data_object::raise_impl_error(this, nullptr, "HY000", "trying to execute an empty query");
                return false;
            }
            this->_error_code = "000000";
            if (this->_query_stmt) {
                this->_query_stmt = nullptr;
            }
            ret = this->doer(statement);
            if (ret == -1) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this);
                }
                return false;
            } else {
                return (ret);
            }
        }

        std::string data_object::last_insert_id(std::string seqname) {
            const std::string &name = seqname;
            this->_error_code = "000000";
            if (this->_query_stmt) {
                this->_query_stmt = nullptr;
            }
            std::string id = this->last_id(name);
            if (id.empty()) {
                if (this->_error_code != "000000") {
                    data_object::handle_error(*this);
                }
                return "";
            } else {
                return id;
            }
        }

        std::string data_object::error_code() {
            if (this->_query_stmt) {
                return this->_query_stmt->_error_code;
            }
            if (this->_error_code.empty()) {
                return "";
            }
            return this->_error_code;
        }

        std::vector<std::string> data_object::error_info() {
            int error_count;
            int error_count_diff = 0;
            int error_expected_count = 3;
            std::vector<std::string> return_value;
            if (this->_query_stmt) {
                return_value.push_back(this->_query_stmt->_error_code);
                if (this->_query_stmt->_error_code != "000000") {
                    goto fill_array;
                }
            } else {
                return_value.push_back(this->_error_code);
                if (this->_error_code != "000000") {
                    goto fill_array;
                }
            }
            this->fetch_err(this->_query_stmt.get(), return_value);
            fill_array:
            error_count = return_value.size();
            if (error_expected_count > error_count) {
                error_count_diff = error_expected_count - error_count;
                for (int current_index = 0; current_index < error_count_diff; current_index++) {
                    return_value.push_back("");
                }
            }
            return return_value;
        }

        std::string data_object::quote(std::string str, param_type paramtype) {
            std::string qstr;
            this->_error_code = "000000";
            if (this->_query_stmt) {
                this->_query_stmt = nullptr;
            }
            if (this->quoter(str, qstr, paramtype)) {
                return qstr;
            }
            if (this->_error_code != "000000") {
                data_object::handle_error(*this);
            }
            return "";
        }

        std::string data_object::addcslashes(std::string str, std::string what) {
            char c;
            std::string new_str;
            new_str.reserve(str.size() * 0.2);
            for (int source = 0; source < str.length(); ++source) {
                c = str[source];
                if (what.find(c) != std::string::npos) {
                    if ((unsigned char) c < 32 || (unsigned char) c > 126) {
                        new_str.push_back('\\');
                        switch (c) {
                            case '\n':
                                new_str.push_back('n');
                                break;
                            case '\t':
                                new_str.push_back('t');
                                break;
                            case '\r':
                                new_str.push_back('r');
                                break;
                            case '\a':
                                new_str.push_back('a');
                                break;
                            case '\v':
                                new_str.push_back('v');
                                break;
                            case '\b':
                                new_str.push_back('b');
                                break;
                            case '\f':
                                new_str.push_back('f');
                                break;
                            default:
                                std::stringstream ss;
                                ss << boost::format("%03o") % (unsigned char) c;
                                new_str += ss.str();
                        }
                        continue;
                    }
                    new_str.push_back('\\');
                }
                new_str.push_back(c);
            }
            return new_str;
        }
    }
}
#endif //WPP_DATA_OBJECT_H
