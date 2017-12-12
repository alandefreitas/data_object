//
// Created by Alan de Freitas on 28/11/17.
//
#ifndef WPP_SQLITE_DRIVER_H
#define WPP_SQLITE_DRIVER_H

#include <stdlib.h>
#include <sqlite3.h>
#include "../data_object.h"

namespace wpp {
    namespace db {
        ///////////////////////////////////////////////////////////////
        //                       DECLARATIONS                        //
        ///////////////////////////////////////////////////////////////
        class sqlite_data_object;

        struct sqlite_error_info {
            const char *file;
            int line;
            unsigned int errcode;
            char *errmsg;
        };

        class sqlite_statement
                : public data_object_statement {
            public:
                friend sqlite_data_object;

                ~sqlite_statement() {
                    if (this->_stmt) {
                        sqlite3_finalize(this->_stmt);
                    }
                }

                virtual int executer() override;

                virtual int fetcher(fetch_orientation ori, long offset) override;

                virtual int describer(int colno) override;

                virtual int get_col(int colno, std::string &ptr, int &caller_frees) override;

                virtual int param_hook(bound_param_data &attr, param_event val) override;

                virtual int get_column_meta(long colno, column_data &return_value) override;

                virtual int cursor_closer() override;

            private:
                sqlite_data_object *_H;
                sqlite3_stmt *_stmt;
                unsigned _pre_fetched:1;
                unsigned _done:1;
        };

        class sqlite_data_object
                : public data_object_crtp<sqlite_data_object, sqlite_statement> {
            public:
                friend class sqlite_statement;

                sqlite_data_object(std::string data_source = ":memory:",
                                   std::string username = "",
                                   std::string passwd = "",
                                   std::unordered_map<attribute_type, driver_option> options = {}) :
                        data_object_crtp<sqlite_data_object, sqlite_statement>(data_source, username, passwd, options) {
                    this->data_object_factory(data_source, username, passwd, options);
                };

                ~sqlite_data_object() {
                    sqlite_error_info *einfo = &this->_einfo;
                    if (this->_db) {
                        #ifdef HAVE_SQLITE3_CLOSE_V2
                        sqlite3_close_v2(this->_db);
                        #else
                        sqlite3_close(this->_db);
                        #endif
                        this->_db = nullptr;
                    }
                }

                virtual int handle_factory(std::unordered_map<attribute_type, driver_option> options = {}) override;

                virtual int
                preparer(const std::string sql,
                         std::shared_ptr<sqlite_statement> &stmt,
                         std::unordered_map<attribute_type, driver_option> driver_options = {}) override {
                    int i;
                    const char *tail;
                    stmt->_H = this;
                    stmt->_supports_placeholders = PLACEHOLDER_POSITIONAL | PLACEHOLDER_NAMED;
                    if (driver_options.find(ATTR_CURSOR) != driver_options.end()) {
                        int x = driver_options[ATTR_CURSOR];
                        if ((int) CURSOR_FWDONLY != x) {
                            this->_einfo.errcode = SQLITE_ERROR;
                            sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                            return 0;
                        }
                    }
                    i = sqlite3_prepare_v2(this->_db, sql.c_str(), sql.size(), &stmt->_stmt, &tail);
                    if (i == SQLITE_OK) {
                        return 1;
                    }
                    sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                    return 0;
                };

                virtual long doer(const std::string sql) override {
                    char *errmsg = nullptr;
                    if (sqlite3_exec(this->_db, sql.c_str(), nullptr, nullptr, &errmsg) != SQLITE_OK) {
                        sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                        if (errmsg) {
                            sqlite3_free(errmsg);
                        }
                        return -1;
                    } else {
                        return sqlite3_changes(this->_db);
                    }
                }

                virtual int begin() override {
                    char *errmsg = NULL;
                    if (sqlite3_exec(this->_db, "BEGIN", NULL, NULL, &errmsg) != SQLITE_OK) {
                        sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                        if (errmsg) {
                            sqlite3_free(errmsg);
                        }
                        return 0;
                    }
                    return 1;
                }

                virtual int commit_func() override {
                    char *errmsg = NULL;
                    if (sqlite3_exec(this->_db, "COMMIT", NULL, NULL, &errmsg) != SQLITE_OK) {
                        sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                        if (errmsg) {
                            sqlite3_free(errmsg);
                        }
                        return 0;
                    }
                    return 1;
                }

                virtual int rollback() override {
                    char *errmsg = NULL;
                    if (sqlite3_exec(this->_db, "ROLLBACK", NULL, NULL, &errmsg) != SQLITE_OK) {
                        sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                        if (errmsg) {
                            sqlite3_free(errmsg);
                        }
                        return 0;
                    }
                    return 1;
                }

                virtual int set_attribute_func(long attr, const driver_option &val) override {
                    switch (attr) {
                        case ATTR_TIMEOUT:
                            sqlite3_busy_timeout(this->_db, (val.get_int()) * 1000);
                            return 1;
                    }
                    return 0;
                }

                virtual std::string last_id(const std::string name) override {
                    return std::to_string(sqlite3_last_insert_rowid(this->_db));
                }

                virtual int fetch_err(const data_object_statement *stmt, std::vector<std::string> &info) override {
                    sqlite_error_info *einfo = &this->_einfo;
                    if (einfo->errcode) {
                        info.resize(3);
                        info[0] = std::to_string(einfo->errcode);
                        info[1] = einfo->errmsg;
                    }
                    return 1;
                }

                // pdo_sqlite_get_attribute, get_attribute; pdo_dbh_get_attr_func;
                virtual int get_attribute(long attr, driver_option &val) override {
                    switch (attr) {
                        case ATTR_CLIENT_VERSION:
                        case ATTR_SERVER_VERSION:
                            val = std::string(sqlite3_libversion());
                            break;
                        default:
                            return 0;
                    }
                    return 1;
                }

            protected:
                sqlite3 *_db;
                sqlite_error_info _einfo;

                static int sqlite_error(sqlite_data_object *dbh, sqlite_statement *stmt, const char *file, int line);
        };

        int sqlite_statement::executer() {
            if (this->_executed && !this->_done) {
                sqlite3_reset(this->_stmt);
            }
            this->_done = 0;
            switch (sqlite3_step(this->_stmt)) {
                case SQLITE_ROW:
                    this->_pre_fetched = 1;
                    this->_columns.resize(sqlite3_data_count(this->_stmt));
                    return 1;
                case SQLITE_DONE:
                    this->_columns.resize(sqlite3_column_count(this->_stmt));
                    this->_row_count = sqlite3_changes(this->_H->_db);
                    sqlite3_reset(this->_stmt);
                    this->_done = 1;
                    return 1;
                case SQLITE_ERROR:
                    sqlite3_reset(this->_stmt);
                case SQLITE_MISUSE:
                case SQLITE_BUSY:
                default:
                    sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                    return 0;
            }
        }

        int
        sqlite_data_object::sqlite_error(sqlite_data_object *dbh, sqlite_statement *stmt, const char *file, int line) {
            std::string &pdo_err = stmt ? stmt->_error_code : dbh->_error_code;
            sqlite_error_info &einfo = dbh->_einfo;
            einfo.errcode = sqlite3_errcode(dbh->_db);
            einfo.file = file;
            einfo.line = line;
            if (einfo.errcode != SQLITE_OK) {
                einfo.errmsg = (char *) sqlite3_errmsg(dbh->_db);
            } else { /* no error */
                pdo_err = "000000";
                return 0;
            }
            switch (einfo.errcode) {
                case SQLITE_NOTFOUND:
                    pdo_err = "42S02";
                    break;
                case SQLITE_INTERRUPT:
                    pdo_err = "01002";
                    break;
                case SQLITE_NOLFS:
                    pdo_err = "HYC00";
                    break;
                case SQLITE_TOOBIG:
                    pdo_err = "22001";
                    break;
                case SQLITE_CONSTRAINT:
                    pdo_err = "23000";
                    break;
                case SQLITE_ERROR:
                default:
                    pdo_err = "HY000";
                    break;
            }
            return einfo.errcode;
        }

        int sqlite_statement::fetcher(fetch_orientation ori, long offset) {
            int i;
            if (!this->_stmt) {
                return 0;
            }
            if (this->_pre_fetched) {
                this->_pre_fetched = 0;
                return 1;
            }
            if (this->_done) {
                return 0;
            }
            i = sqlite3_step(this->_stmt);
            switch (i) {
                case SQLITE_ROW:
                    return 1;
                case SQLITE_DONE:
                    this->_done = 1;
                    sqlite3_reset(this->_stmt);
                    return 0;
                case SQLITE_ERROR:
                    sqlite3_reset(this->_stmt);
                default:
                    sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                    return 0;
            }
        }

        int sqlite_statement::describer(int colno) {
            const char *str;
            if (colno >= sqlite3_column_count(this->_stmt)) {
                sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                return 0;
            }
            str = sqlite3_column_name(this->_stmt, colno);
            this->_columns[colno].name = str;
            this->_columns[colno].maxlen = 0xffffffff;
            this->_columns[colno].precision = 0;
            switch (sqlite3_column_type(this->_stmt, colno)) {
                case SQLITE_INTEGER:
                case SQLITE_FLOAT:
                case SQLITE3_TEXT:
                case SQLITE_BLOB:
                case SQLITE_NULL:
                default:
                    this->_columns[colno].param_type = PARAM_STR;
                    break;
            }
            return 1;
        }

        int sqlite_statement::get_col(int colno, std::string &result, int &caller_frees) {
            char *char_result;
            if (!this->_stmt) {
                return 0;
            }
            if (colno >= sqlite3_data_count(this->_stmt)) {
                /* error invalid column */
                sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                return 0;
            }
            switch (sqlite3_column_type(this->_stmt, colno)) {
                case SQLITE_NULL:
                    result = "";
                    return 1;
                case SQLITE_BLOB:
                    char_result = (char *) sqlite3_column_blob(this->_stmt, colno);
                    result = char_result;
                    return 1;
                default:
                    char_result = (char *) sqlite3_column_text(this->_stmt, colno);
                    result = char_result;
                    return 1;
            }
        }

        int sqlite_statement::param_hook(bound_param_data &param, param_event event_type) {
            switch (event_type) {
                case PARAM_EVT_EXEC_PRE:
                    if (this->_executed && !this->_done) {
                        sqlite3_reset(this->_stmt);
                        this->_done = 1;
                    }
                    if (param.is_param) {
                        if (param.paramno == -1) {
                            param.paramno = sqlite3_bind_parameter_index(this->_stmt, param.name.c_str()) - 1;
                        }
                        switch (param.param_type) {
                            case PARAM_NULL:
                                if (sqlite3_bind_null(this->_stmt, param.paramno + 1) == SQLITE_OK) {
                                    return 1;
                                }
                                sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                                return 0;
                            case PARAM_INT:
                            case PARAM_BOOL:
                                if (std::numeric_limits<long>::max() > 2147483647) {
                                    if (SQLITE_OK ==
                                        sqlite3_bind_int64(this->_stmt, param.paramno + 1, *(long *) param.parameter)) {
                                        return 1;
                                    }
                                } else {
                                    if (SQLITE_OK ==
                                        sqlite3_bind_int(this->_stmt, param.paramno + 1, *(long *) param.parameter)) {
                                        return 1;
                                    }
                                }
                                sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                                return 0;
                            case PARAM_FLOAT:
                                if (SQLITE_OK ==
                                    sqlite3_bind_double(this->_stmt, param.paramno + 1, *(double *) param.parameter)) {
                                    return 1;
                                }
                                return 0;
                            case PARAM_LOB:
                                return 0;
                            default:
                                if (param.parameter == nullptr) {
                                    if (sqlite3_bind_null(this->_stmt, param.paramno + 1) == SQLITE_OK) {
                                        return 1;
                                    }
                                } else {
                                    std::string *string_data = (std::string *) param.parameter;
                                    if (SQLITE_OK == sqlite3_bind_text(this->_stmt, param.paramno + 1,
                                                                       string_data->c_str(),
                                                                       string_data->size(),
                                                                       SQLITE_STATIC)) {
                                        return 1;
                                    }
                                }
                                sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                                return 0;
                        }
                    }
                    break;
                default:;
            }
            return 1;
        }

        int sqlite_statement::get_column_meta(long colno, column_data &return_value) {
            const char *str;
            if (!this->_stmt) {
                return 0;
            }
            if (colno >= sqlite3_data_count(this->_stmt)) {
                sqlite_data_object::sqlite_error(this->_H, this, __FILE__, __LINE__);
                return 0;
            }
            switch (sqlite3_column_type(this->_stmt, colno)) {
                case SQLITE_NULL:
                    return_value.native_type = "null";
                    break;
                case SQLITE_FLOAT:
                    return_value.native_type = "double";
                    break;
                case SQLITE_BLOB:
                    return_value.native_type = "blob";
                    return_value.flags.push_back("blob");
                case SQLITE_TEXT:
                    return_value.native_type = "string";
                    break;
                case SQLITE_INTEGER:
                    return_value.native_type = "integer";
                    break;
            }
            str = sqlite3_column_table_name(this->_stmt, colno);
            if (str) {
                return_value.table = str;
            }
            return 1;
        }

        int sqlite_statement::cursor_closer() {
            sqlite3_reset(this->_stmt);
            return 1;
        }

        int sqlite_data_object::handle_factory(std::unordered_map<attribute_type, driver_option> driver_options) {
            int i, ret = 0;
            long timeout = 60, flags;
            char *filename;
            this->_einfo.errcode = 0;
            this->_einfo.errmsg = NULL;
            filename = &this->_data_source[0u];
            if (!filename) {
                std::runtime_error("open_basedir prohibits opening " + this->_data_source);
            } else {
                flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
                #if SQLITE_VERSION_NUMBER >= 3005000
                i = sqlite3_open_v2(filename, &this->_db, flags, NULL);
                #else
                i = sqlite3_open(filename, &this->_db);
                #endif
                if (i != SQLITE_OK) {
                    sqlite_data_object::sqlite_error(this, nullptr, __FILE__, __LINE__);
                } else {
                    if (!driver_options.empty()) {
                        auto iter = driver_options.find(ATTR_TIMEOUT);
                        if (iter != driver_options.end()) {
                            timeout = iter->second.get_int();
                        }
                    }
                    sqlite3_busy_timeout(this->_db, timeout * 1000);
                    this->_alloc_own_columns = 1;
                    this->_max_escaped_char_length = 2;
                    ret = 1;
                }
            }
            return ret;
        }
        ///////////////////////////////////////////////////////////////
        //                 TYPE ALIAS WITHOUT TEMPLATE               //
        ///////////////////////////////////////////////////////////////
        using sqlite = sqlite_data_object;
    }
}
#endif //WPP_SQLITE_DRIVER_H
