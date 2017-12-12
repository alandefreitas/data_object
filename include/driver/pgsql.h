//
// Created by Alan de Freitas on 28/11/17.
//
#ifndef WPP_PGSQL_DRIVER_H
#define WPP_PGSQL_DRIVER_H

#include <stdlib.h>
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>
#include "pg_config.h" /* needed for PG_VERSION */
#include "../data_object.h"

namespace wpp {
    namespace db {
        ///////////////////////////////////////////////////////////////
        //                       DECLARATIONS                        //
        ///////////////////////////////////////////////////////////////
        class pgsql_data_object;

        enum pgsql_attribute_type {
            PGSQL_ATTR_DISABLE_PREPARES = attribute_type::ATTR_DRIVER_SPECIFIC,
        };
        struct pgsql_column {
            std::string def;
            long intval;
            Oid pgsql_type;
            bool boolval;
        };
        struct pgsql_lob_self {
            pgsql_data_object *dbh;
            PGconn *conn;
            int lfd;
            Oid oid;
        };

        class pgsql_statement
                : public data_object_statement {
            public:
                friend pgsql_data_object;

                ~pgsql_statement();

                virtual int executer() override;

                virtual int fetcher(fetch_orientation ori, long offset) override;

                virtual int describer(int colno) override;

                virtual int get_col(int colno, std::string &ptr, int &caller_frees) override;

                virtual int param_hook(bound_param_data &attr, param_event val) override;

                //virtual int set_attribute(long attr, std::string &val) override;
                //virtual int get_attribute(long attr, std::string &val) override;
                virtual int get_column_meta(long colno, column_data &return_value) override;

                //virtual int next_rowset_func() override;
                virtual int cursor_closer() override;

            private:
                static std::string translate_oid_to_table(Oid oid, PGconn *conn);

                // Statement handle
                std::string _cursor_name;
                std::string _stmt_name;
                std::string _query;
                std::vector<std::string> _param_values;
                // Useful format for posgresql
                std::vector<int> _param_lengths;
                std::vector<int> _param_formats;
                std::vector<Oid> _param_types;
                int _current_row;
                bool _is_prepared{false};
                // Connection
                // this is not a smart point because it won't be deallocated
                pgsql_data_object *_H{nullptr};
                // Results
                PGresult *_result{nullptr};
                // PGSQL columns
                std::vector<pgsql_column> _cols;

                int get_cursor_result();

                int prepare();

                int execute_prepared();

                int execute_with_param();

                int execute_plain_query();

                int check_status(ExecStatusType &status);

                int update_row_and_column_count(ExecStatusType status);

            public:
                const std::string BOOLLABEL = "bool";
                const std::string BYTEALABEL = "bytea";
                const std::string DATELABEL = "date";
                const std::string INT2LABEL = "int2";
                const std::string INT4LABEL = "int4";
                const std::string INT8LABEL = "int8";
                const std::string TEXTLABEL = "text";
                const std::string TIMESTAMPLABEL = "timestamp";
                const std::string VARCHARLABEL = "varchar";
                const static Oid BOOLOID = 16;
                const static Oid BYTEAOID = 17;
                const static Oid DATEOID = 1082;
                const static Oid INT2OID = 21;
                const static Oid INT4OID = 23;
                const static Oid INT8OID = 20;
                const static Oid OIDOID = 26;
                const static Oid TEXTOID = 25;
                const static Oid TIMESTAMPOID = 1114;
                const static Oid VARCHAROID = 1043;
        };

        class pgsql_data_object
                : public data_object_crtp<pgsql_data_object, pgsql_statement> {
            public:
                friend class pgsql_statement;

                pgsql_data_object(std::string data_source,
                                  std::string username,
                                  std::string passwd,
                                  std::unordered_map<attribute_type, driver_option> options = {}) :
                        data_object_crtp<pgsql_data_object, pgsql_statement>(data_source, username, passwd, options) {
                    this->data_object_factory(data_source, username, passwd, options);
                };

                ~pgsql_data_object();

                virtual int handle_factory(std::unordered_map<attribute_type, driver_option> options = {}) override;

                virtual int
                preparer(const std::string sql,
                         std::shared_ptr<pgsql_statement> &stmt,
                         std::unordered_map<attribute_type, driver_option> driver_options = {}) override;

                virtual long doer(const std::string sql) override;

                virtual bool quoter(std::string unquoted,
                                    std::string &quoted,
                                    param_type paramtype) override;

                virtual int begin() override;

                virtual int commit_func() override;

                virtual int rollback() override;

                virtual int set_attribute_func(long attr, const driver_option &val) override;

                virtual std::string last_id(const std::string name) override;

                virtual int fetch_err(const data_object_statement *stmt, std::vector<std::string> &info) override;

                virtual int get_attribute(long attr, driver_option &val) override;

                virtual int check_liveness() override;

                virtual int in_transaction_func() override;

            protected:
                // Auxiliary functions
                void clear_result_set();

                bool open_connection();

                bool close_connection();

                static std::string escape_credentials(const std::string str) {
                    return addcslashes(str, "\\'");
                }

                int transaction_cmd(const std::string cmd);

                static int pgsql_error(pgsql_data_object *dbh,
                                       pgsql_statement *stmt,
                                       int errcode,
                                       const std::string sqlstate,
                                       const std::string msg,
                                       const std::string file,
                                       int line);

                std::shared_ptr<pgsql_lob_self> create_lob_stream(int lfd, Oid oid);

                static std::string trim_message(std::string &message);

                // Data Members
                PGresult *_result_set;
                // Error info
                struct error_info {
                    std::string file;
                    int line;
                    unsigned int errcode;
                    std::string errmsg;
                };
                // Database handle
                PGconn *_server;
                unsigned _attached:1;
                unsigned _reserved:31;
                struct error_info _einfo;
                Oid _pgoid;
                unsigned int _stmt_counter;
                bool _emulate_prepares;
                bool _disable_native_prepares;
                bool _disable_prepares;
                typedef struct {
                    Oid oid;
                } pgsql_bound_param;
                // Number of pgsql objects
                static int _counter;
        };

        int pgsql_data_object::_counter = 0;

        ///////////////////////////////////////////////////////////////
        //                    STATEMENT DEFINITIONS                  //
        ///////////////////////////////////////////////////////////////
        pgsql_statement::~pgsql_statement() {
            if (this->_result) {
                PQclear(this->_result);
                this->_result = (nullptr);
            }
            if (!this->_stmt_name.empty()) {
                if (this->_is_prepared /* && server_obj_usable */) {
                    std::string q = "";
                    PGresult *res;
                    std::stringstream ss;
                    ss << boost::format("DEALLOCATE %s") % this->_stmt_name;
                    q = ss.str();
                    res = PQexec(this->_H->_server, q.c_str());
                    if (res) {
                        PQclear(res);
                    }
                }
                this->_stmt_name = "";
            }
            this->_param_lengths.clear();
            this->_param_values.clear();
            this->_param_formats.clear();
            this->_param_types.clear();
            this->_query.clear();
            if (!this->_cursor_name.empty()) {
                std::string q = nullptr;
                PGresult *res;
                std::stringstream ss;
                ss << boost::format("CLOSE %s") % this->_cursor_name;
                q = ss.str();
                res = PQexec(this->_H->_server, q.c_str());
                if (res) {
                    PQclear(res);
                }
                this->_cursor_name = nullptr;
            }
            if (!this->_cols.empty()) {
                this->_cols.clear();
            }
        }

        int pgsql_statement::get_cursor_result() {
            ExecStatusType status;
            std::string q = nullptr;
            if (this->_is_prepared) {
                q = "CLOSE " + this->_cursor_name;
                this->_result = PQexec(this->_H->_server, q.c_str());
            }
            q = "DECLARE " + this->_cursor_name + " SCROLL CURSOR WITH HOLD FOR " + this->_active_query_string;
            this->_result = PQexec(this->_H->_server, q.c_str());
            /* check if declare failed */
            status = PQresultStatus(this->_result);
            if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                pgsql_data_object::pgsql_error((pgsql_data_object *) this->_dbh, this, status,
                                               PQresultErrorField(this->_result, PG_DIAG_SQLSTATE), "",
                                               __FILE__,
                                               __LINE__);
                return 0;
            }
            this->_is_prepared = 1;
            q = "FETCH FORWARD 0 FROM " + this->_cursor_name;
            this->_result = PQexec(this->_H->_server, q.c_str());
            return 1;
        }

        int pgsql_statement::prepare() {
            ExecStatusType status;
            bool stmt_retry;
            do {
                stmt_retry = false;
                this->_result = PQprepare(this->_H->_server, this->_stmt_name.c_str(), this->_query.c_str(),
                                          this->_bound_param.size(),
                                          this->_param_types.data());
                status = PQresultStatus(this->_result);
                switch (status) {
                    case PGRES_COMMAND_OK:
                    case PGRES_TUPLES_OK:
                        /* it worked */
                        this->_is_prepared = 1;
                        PQclear(this->_result);
                        break;
                    default: {
                        std::string sqlstate = PQresultErrorField(this->_result, PG_DIAG_SQLSTATE);
                        const bool statement_already_existed = sqlstate == "42P05";
                        if (statement_already_existed) {
                            std::string buf;
                            PGresult *res;
                            buf = "DEALLOCATE " + this->_stmt_name;
                            res = PQexec(this->_H->_server, buf.c_str());
                            if (res) {
                                PQclear(res);
                            }
                            stmt_retry = true;
                        } else {
                            pgsql_data_object::pgsql_error((pgsql_data_object *) this->_dbh, this, status, sqlstate,
                                                           "", __FILE__, __LINE__);
                            return 0;
                        }
                    }
                }
            } while (stmt_retry);
            return 1;
        }

        int pgsql_statement::execute_prepared() {
            const char **char_param = new const char *[this->_param_values.size()];
            for (int i = 0; i < this->_param_values.size(); ++i) {
                char_param[i] = this->_param_values[i].c_str();
            }
            this->_result = PQexecPrepared(this->_H->_server, this->_stmt_name.c_str(),
                                           this->_bound_param.size(),
                                           char_param,
                                           this->_param_lengths.data(),
                                           this->_param_formats.data(),
                                           0);
            return 1;
        }

        int pgsql_statement::execute_with_param() {
            const char **char_param = new const char *[this->_param_values.size()];
            for (int i = 0; i < this->_param_values.size(); ++i) {
                char_param[i] = this->_param_values[i].c_str();
            }
            this->_result = PQexecParams(this->_H->_server, this->_query.c_str(),
                                         this->_bound_param.size(),
                                         this->_param_types.data(),
                                         (const char **) char_param,
                                         this->_param_lengths.data(),
                                         this->_param_formats.data(),
                                         0);
            return 1;
        }

        int pgsql_statement::execute_plain_query() {
            this->_result = PQexec(this->_H->_server, this->_active_query_string.c_str());
            return 1;
        }

        int pgsql_statement::check_status(ExecStatusType &status) {
            status = PQresultStatus(this->_result);
            if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                pgsql_data_object::pgsql_error((pgsql_data_object *) this->_dbh, this, status,
                                               PQresultErrorField(this->_result, PG_DIAG_SQLSTATE),
                                               "", __FILE__, __LINE__);
                return 0;
            }
            return 1;
        }

        int pgsql_statement::update_row_and_column_count(ExecStatusType status) {
            if (!this->_executed && (this->_cols.empty())) {
                this->_cols.resize(PQnfields(this->_result));
                this->_columns.resize(this->_cols.size());
            }
            if (status == PGRES_COMMAND_OK) {
                this->_row_count = std::atol(PQcmdTuples(this->_result));
                this->_H->_pgoid = PQoidValue(this->_result);
            } else {
                this->_row_count = (long) PQntuples(this->_result);
            }
            return 1;
        }

        int pgsql_statement::executer() {
            if (this->_result) {
                PQclear(this->_result);
                this->_result = nullptr;
            }
            this->_current_row = 0;
            const bool statement_has_cursor = !this->_cursor_name.empty();
            if (statement_has_cursor) {
                if (!this->get_cursor_result()) {
                    return 0;
                }
            } else if (!this->_stmt_name.empty()) {
                if (!this->_is_prepared) {
                    if (!this->prepare()) {
                        return 0;
                    }
                } else {
                }
                if (!this->execute_prepared()) {
                    return 0;
                }
            } else if (this->_supports_placeholders == PLACEHOLDER_NAMED) {
                if (!this->execute_with_param()) {
                    return 0;
                }
            } else {
                if (!this->execute_plain_query()) {
                    return 0;
                }
            }
            ExecStatusType status;
            if (!this->check_status(status)) {
                return 0;
            }
            this->update_row_and_column_count(status);
            return 1;
        }

        int pgsql_statement::fetcher(fetch_orientation ori, long offset) {
            if (!this->_cursor_name.empty()) {
                std::string ori_str = nullptr;
                std::string q = nullptr;
                ExecStatusType status;
                switch (ori) {
                    case FETCH_ORI_NEXT:
                        ori_str = "NEXT";
                        break;
                    case FETCH_ORI_PRIOR:
                        ori_str = "BACKWARD";
                        break;
                    case FETCH_ORI_FIRST:
                        ori_str = "FIRST";
                        break;
                    case FETCH_ORI_LAST:
                        ori_str = "LAST";
                        break;
                    case FETCH_ORI_ABS:
                        ori_str = "ABSOLUTE " + std::to_string(offset);
                        break;
                    case FETCH_ORI_REL:
                        ori_str = "RELATIVE " + std::to_string(offset);
                        break;
                    default:
                        return 0;
                }
                q = "FETCH " + ori_str + " FROM " + this->_cursor_name;
                this->_result = PQexec(this->_H->_server, q.c_str());
                status = PQresultStatus(this->_result);
                if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                    pgsql_data_object::pgsql_error((pgsql_data_object *) this->_dbh, this, status,
                                                   PQresultErrorField(this->_result, PG_DIAG_SQLSTATE), "",
                                                   __FILE__,
                                                   __LINE__);
                    return 0;
                }
                if (PQntuples(this->_result)) {
                    this->_current_row = 1;
                    return 1;
                } else {
                    return 0;
                }
            } else {
                if (this->_current_row < this->_row_count) {
                    this->_current_row++;
                    return 1;
                } else {
                    return 0;
                }
            }
        }

        int pgsql_statement::describer(int colno) {
            //pgsql_statement *S = (pgsql_statement*)this->_driver_data;
            std::vector<column_data> &cols = this->_columns;
            struct pdo_bound_param_data *param;
            std::string str;
            if (!this->_result) {
                return 0;
            }
            str = PQfname(this->_result, colno);
            cols[colno].name = str;
            cols[colno].maxlen = PQfsize(this->_result, colno);
            cols[colno].precision = PQfmod(this->_result, colno);
            this->_cols[colno].pgsql_type = PQftype(this->_result, colno);
            switch (this->_cols[colno].pgsql_type) {
                case pgsql_statement::BOOLOID:
                    cols[colno].param_type = PARAM_BOOL;
                    break;
                case pgsql_statement::OIDOID: {
                    const std::unordered_map<std::string, bound_param_data>::const_iterator param = this->_bound_columns.find(
                            cols[colno].name);
                    if (param != this->_bound_columns.end()) {
                        if (param->second.param_type == PARAM_LOB) {
                            cols[colno].param_type = PARAM_LOB;
                            break;
                        }
                    }
                    cols[colno].param_type = PARAM_INT;
                    break;
                }
                case pgsql_statement::INT2OID:
                case pgsql_statement::INT4OID:
                    cols[colno].param_type = PARAM_INT;
                    break;
                case pgsql_statement::INT8OID:
                    if (sizeof(long) >= 8) {
                        cols[colno].param_type = PARAM_INT;
                    } else {
                        cols[colno].param_type = PARAM_STR;
                    }
                    break;
                case pgsql_statement::BYTEAOID:
                    cols[colno].param_type = PARAM_LOB;
                    break;
                default:
                    cols[colno].param_type = PARAM_STR;
            }
            return 1;
        }

        int pgsql_statement::get_col(int colno, std::string &ptr, int &caller_frees) {
            std::vector<column_data> &cols = this->_columns;
            if (!this->_result) {
                return 0;
            }
            if (PQgetisnull(this->_result, this->_current_row - 1, colno)) {
                ptr.clear();
            } else {
                ptr = PQgetvalue(this->_result, this->_current_row - 1, colno);
                switch (cols[colno].param_type) {
                    case PARAM_INT:
                        this->_cols[colno].intval = std::stol(ptr);
                        ptr = std::to_string(this->_cols[colno].intval);
                        break;
                    case PARAM_BOOL:
                        this->_cols[colno].boolval = (ptr[0] == 't' || ptr[0] == 'T') ? 1 : 0;
                        ptr = this->_cols[colno].boolval ? "true" : "false";
                        break;
                    case PARAM_NULL:
                    case PARAM_STR:
                    default:
                        break;
                }
            }
            return 1;
        }

        int pgsql_statement::param_hook(bound_param_data &param, param_event event_type) {
            const bool named_placeholders = this->_supports_placeholders == PLACEHOLDER_NAMED && param.is_param;
            if (named_placeholders) {
                switch (event_type) {
                    case PARAM_EVT_FREE:
                        break;
                    case PARAM_EVT_NORMALIZE: // from $1, $2... to 0, 1 etc....
                        if (!param.name.empty()) {
                            const bool it_has_pgsql_format = param.name[0] == '$';
                            if (it_has_pgsql_format) {
                                param.paramno = std::stol(param.name.substr(1));
                            } else {
                                auto iter = this->_bound_param_map.find(param.name);
                                const bool quoted_name_is_defined = iter != this->_bound_param_map.end();
                                if (quoted_name_is_defined) {
                                    const std::string quoted_name = iter->second;
                                    param.paramno = std::stol(quoted_name.substr(1));
                                    param.paramno--;
                                } else {
                                    pgsql_data_object::raise_impl_error(this->_dbh, this, "HY093", param.name);
                                    return 0;
                                }
                            }
                        }
                        break;
                    case PARAM_EVT_ALLOC:
                        if (this->_bound_param_map.empty()) {
                            return 1;
                        }
                        if (this->_bound_param.size() < param.paramno) {
                            pgsql_data_object::raise_impl_error(this->_dbh, this, "HY093",
                                                                "parameter was not defined");
                            return 0;
                        }
                    case PARAM_EVT_EXEC_POST:
                    case PARAM_EVT_FETCH_PRE:
                    case PARAM_EVT_FETCH_POST:
                        return 1;
                    case PARAM_EVT_EXEC_PRE:
                        if (this->_bound_param_map.empty()) {
                            return 1;
                        }
                        if (this->_param_values.empty()) {
                            this->_param_values.resize(this->_bound_param.size());
                            this->_param_lengths.resize(this->_bound_param.size());
                            this->_param_formats.resize(this->_bound_param.size());
                            this->_param_types.resize(this->_bound_param.size());
                        }
                        if (param.paramno >= 0) {
                            std::string parameter = byte_to_string(param.parameter_typeinfo, param.parameter);
                            switch (param.param_type) {
                                case PARAM_NULL:
                                    this->_param_values[param.paramno] = "";
                                    this->_param_lengths[param.paramno] = 0;
                                    break;
                                case PARAM_BOOL:
                                    this->_param_values[param.paramno] = (parameter[0] == 't') ? "t" : "f";
                                    this->_param_lengths[param.paramno] = 1;
                                    this->_param_formats[param.paramno] = 0;
                                    break;
                                case PARAM_LOB:
                                    return 0;
                                default:
                                    this->_param_values[param.paramno] = parameter;
                                    this->_param_lengths[param.paramno] = parameter.length();
                                    this->_param_formats[param.paramno] = 0;
                            }
                            if (param.param_type == PARAM_LOB) {
                                this->_param_types[param.paramno] = 0;
                                this->_param_formats[param.paramno] = 1;
                            } else {
                                this->_param_types[param.paramno] = 0;
                            }
                        }
                        break;
                }
            } else if (param.is_param) {
                if (param.param_type == PARAM_BOOL) {
                    const std::string s = ((*((std::string *) param.parameter))[0] == 't' ||
                                           (*((std::string *) param.parameter))[0] == '0') ? "t" : "f";
                    param.param_type = PARAM_STR;
                    param.parameter_data.reset(new std::string(s));
                    param.parameter = (void *) &param.parameter_data;
                }
            }
            return 1;
        }

        int pgsql_statement::get_column_meta(long colno, column_data &return_value) {
            PGresult *res;
            std::string q = "";
            ExecStatusType status;
            if (!this->_result) {
                return 0;
            }
            if (colno >= this->_columns.size()) {
                return 0;
            }
            return_value.native_type = this->_cols[colno].pgsql_type;
            Oid table_oid = PQftable(this->_result, colno);
            return_value.native_table_id = table_oid;
            std::string table_name = pgsql_statement::translate_oid_to_table(table_oid, this->_H->_server);
            return_value.table = table_name;
            switch (this->_cols[colno].pgsql_type) {
                case pgsql_statement::BOOLOID:
                    return_value.native_type = BOOLLABEL;
                    break;
                case pgsql_statement::BYTEAOID:
                    return_value.native_type = BYTEALABEL;
                    break;
                case pgsql_statement::INT8OID:
                    return_value.native_type = INT8LABEL;
                    break;
                case pgsql_statement::INT2OID:
                    return_value.native_type = INT2LABEL;
                    break;
                case pgsql_statement::INT4OID:
                    return_value.native_type = INT4LABEL;
                    break;
                case pgsql_statement::TEXTOID:
                    return_value.native_type = TEXTLABEL;
                    break;
                case pgsql_statement::VARCHAROID:
                    return_value.native_type = VARCHARLABEL;
                    break;
                case pgsql_statement::DATEOID:
                    return_value.native_type = DATELABEL;
                    break;
                case pgsql_statement::TIMESTAMPOID:
                    return_value.native_type = TIMESTAMPLABEL;
                    break;
                default:
                    std::stringstream ss;
                    ss << boost::format("SELECT TYPNAME FROM PG_TYPE WHERE OID=%u") % this->_cols[colno].pgsql_type;
                    q = ss.str();
                    res = PQexec(this->_H->_server, q.c_str());
                    status = PQresultStatus(res);
                    if (status == PGRES_TUPLES_OK && 1 == PQntuples(res)) {
                        return_value.native_type = PQgetvalue(res, 0, 0);
                    }
                    PQclear(res);
            }
            return 1;
        }

        int pgsql_statement::cursor_closer() {
            this->_cols.clear();
            return 1;
        }

        std::string pgsql_statement::translate_oid_to_table(Oid oid, PGconn *conn) {
            PGresult *tmp_res;
            std::string querystr = "";
            std::stringstream ss;
            ss << boost::format("SELECT RELNAME FROM PG_CLASS WHERE OID=%d") % oid;
            querystr = ss.str();
            if ((tmp_res = PQexec(conn, querystr.c_str())) == nullptr || PQresultStatus(tmp_res) != PGRES_TUPLES_OK) {
                if (tmp_res) {
                    PQclear(tmp_res);
                }
                return "";
            }
            char *table_name = PQgetvalue(tmp_res, 0, 0);
            if (table_name == nullptr) {
                PQclear(tmp_res);
                return "";
            }
            PQclear(tmp_res);
            return table_name;
        }

        /////////////////////////////////////////////////////////////////
        ////                   CONNECTION DEFINITIONS                  //
        /////////////////////////////////////////////////////////////////
        int pgsql_data_object::handle_factory(std::unordered_map<attribute_type, driver_option> driver_options) {
            int ret = 0;
            std::string conn_str;
            std::string tmp_user;
            std::string tmp_pass;
            long connect_timeout = 30;
            this->_counter++;
            this->_driver_name = "pgsql";
            this->_einfo.errcode = 0;
            this->_einfo.errmsg = "";
            size_t from = 0;
            size_t pos = this->_data_source.find(';', from);
            while (pos != std::string::npos) {
                this->_data_source[pos] = ' ';
                from = pos + 1;
                pos = this->_data_source.find(';', from);
            }
            auto iter = driver_options.find(ATTR_TIMEOUT);
            if (iter != driver_options.end()) {
                connect_timeout = iter->second.get_int();
            } else {
                connect_timeout = 30;
            }
            tmp_user = escape_credentials(this->_username);
            tmp_pass = escape_credentials(this->_password);
            if (!tmp_user.empty() && !tmp_pass.empty()) {
                conn_str =
                        this->_data_source + " user='" + tmp_user + "' password='" + tmp_pass + "' connect_timeout=" +
                        std::to_string(connect_timeout);
            } else if (!tmp_user.empty()) {
                conn_str = this->_data_source + " user='" + tmp_user + "' connect_timeout=" +
                           std::to_string(connect_timeout);
            } else if (!tmp_pass.empty()) {
                conn_str = this->_data_source + " password='" + tmp_pass + "' connect_timeout=" +
                           std::to_string(connect_timeout);
            } else {
                conn_str = this->_data_source + " connect_timeout=" + std::to_string(connect_timeout);
            }
            this->_server = PQconnectdb(conn_str.c_str());
            if (PQstatus(this->_server) != CONNECTION_OK) {
                pgsql_data_object::pgsql_error(this, nullptr, PGRES_FATAL_ERROR, "08006", "", __FILE__, __LINE__);
                goto cleanup;
            }
            this->_attached = 1;
            this->_pgoid = -1;
            this->_alloc_own_columns = 1;
            this->_max_escaped_char_length = 2;
            ret = 1;
            cleanup:
            if (!ret) {
                this->~pgsql_data_object();
            }
            return ret;
        }

        pgsql_data_object::~pgsql_data_object() {
            if (this->_server) {
                PQfinish(this->_server);
            }
        }

        int pgsql_data_object::preparer(std::string sql,
                                        std::shared_ptr<pgsql_statement> &stmt,
                                        std::unordered_map<attribute_type, driver_option> driver_options) {
            int ret;
            std::string nsql;
            int emulate = 0;
            int execute_only = 0;
            stmt->_H = this;
            int scrollable;
            auto iter = driver_options.find(ATTR_CURSOR);
            if (iter != driver_options.end()) {
                scrollable = iter->second.get_int() == cursor_type::CURSOR_SCROLL;
            } else {
                scrollable = false;
            }
            if (scrollable) {
                stmt->_cursor_name.clear();
                std::stringstream ss;
                ss << boost::format("pgsql_%04x_crsr_%08x") % this->_counter % ++this->_stmt_counter;
                stmt->_cursor_name = ss.str();
                emulate = 1;
            } else if (!driver_options.empty()) {
                if ((driver_options.count(ATTR_EMULATE_PREPARES) ? driver_options[ATTR_EMULATE_PREPARES].get_int()
                                                                 : this->_emulate_prepares) == 1) {
                    emulate = 1;
                }
                if ((driver_options.count((wpp::db::attribute_type) PGSQL_ATTR_DISABLE_PREPARES)
                     ? driver_options[(wpp::db::attribute_type) PGSQL_ATTR_DISABLE_PREPARES].get_int()
                     : this->_disable_prepares) == 1) {
                    execute_only = 1;
                }
            } else {
                emulate = this->_disable_native_prepares || this->_emulate_prepares;
                execute_only = this->_disable_prepares;
            }
            if (!emulate && PQprotocolVersion(this->_server) > 2) {
                stmt->_supports_placeholders = PLACEHOLDER_NAMED;
                stmt->_named_rewrite_template = "$%d";
                ret = stmt->parse_params(sql, nsql);
                if (ret == 1) {
                    sql = nsql;
                } else if (ret == -1) {
                    this->_error_code = stmt->_error_code;
                    return 0;
                }
                if (!execute_only) {
                    std::stringstream ss;
                    ss << boost::format("pgsql_%04x_stmt_%08x") % this->_counter % ++this->_stmt_counter;
                    stmt->_stmt_name = ss.str();
                }
                if (!nsql.empty()) {
                    stmt->_query = nsql;
                } else {
                    stmt->_query = sql;
                }
                return 1;
            }
            stmt->_supports_placeholders = PLACEHOLDER_NONE;
            return 1;
        };

        long pgsql_data_object::doer(const std::string sql) {
            PGresult *res;
            long ret = 1;
            ExecStatusType qs;
            if (!(res = PQexec(this->_server, sql.c_str()))) {
                pgsql_data_object::pgsql_error(this, nullptr, PGRES_FATAL_ERROR, nullptr, "", __FILE__, __LINE__);
                return -1;
            }
            qs = PQresultStatus(res);
            if (qs != PGRES_COMMAND_OK && qs != PGRES_TUPLES_OK) {
                pgsql_data_object::pgsql_error(this, nullptr, qs, PQresultErrorField(res, PG_DIAG_SQLSTATE), "",
                                               __FILE__, __LINE__);
                PQclear(res);
                return -1;
            }
            this->_pgoid = PQoidValue(res);
            if (qs == PGRES_COMMAND_OK) {
                ret = std::atol(PQcmdTuples(res));
            } else {
                ret = 0;
            }
            PQclear(res);
            return ret;
        }

        bool pgsql_data_object::quoter(std::string unquoted,
                                       std::string &quoted,
                                       param_type paramtype) {
            std::basic_string<unsigned char> escaped;
            size_t tmp_len;
            switch (paramtype) {
                case PARAM_LOB:
                    escaped = PQescapeByteaConn(this->_server, (unsigned char *) unquoted.data(), unquoted.size(),
                                                &tmp_len);
                    quoted.resize(tmp_len + 2);
                    std::copy(escaped.begin(), escaped.end(), quoted.begin() + 1);
                    quoted.front() = '\'';
                    quoted.back() = '\'';
                    break;
                default:
                    char *quoted_char;
                    size_t quotedlen = PQescapeStringConn(this->_server, quoted_char, unquoted.data(),
                                                          unquoted.length(), nullptr);
                    quoted = "'" + std::string(quoted_char) + "'";
            }
            return 1;
        }

        int pgsql_data_object::begin() {
            return this->transaction_cmd("BEGIN");
        }

        int pgsql_data_object::commit_func() {
            int ret = this->transaction_cmd("COMMIT");
            if (!ret) {
                this->_in_txn = this->in_transaction_func();
            }
            return ret;
        }

        int pgsql_data_object::rollback() {
            return this->transaction_cmd("ROLLBACK");
        }

        int pgsql_data_object::set_attribute_func(long attr, const driver_option &val) {
            return 1;
        }

        std::string pgsql_data_object::last_id(const std::string name) {
            std::string id;
            PGresult *res;
            ExecStatusType status;
            if (name.empty()) {
                res = PQexec(this->_server, "SELECT LASTVAL()");
            } else {
                const char *q[1];
                q[0] = name.c_str();
                res = PQexecParams(this->_server, "SELECT CURRVAL($1)", 1, nullptr, q, nullptr, nullptr, 0);
            }
            status = PQresultStatus(res);
            if (res && (status == PGRES_TUPLES_OK)) {
                id = (std::string) PQgetvalue(res, 0, 0);
            } else {
                pgsql_data_object::pgsql_error(this, nullptr, status, PQresultErrorField(res, PG_DIAG_SQLSTATE), "",
                                               __FILE__, __LINE__);
            }
            if (res) {
                PQclear(res);
            }
            return id;
        }

        int pgsql_data_object::fetch_err(const data_object_statement *stmt, std::vector<std::string> &info) {
            return 1;
        }

        int pgsql_data_object::get_attribute(long attr, driver_option &val) {
            driver_option return_value;
            switch (attr) {
                case ATTR_EMULATE_PREPARES:
                    return_value = this->_emulate_prepares;
                    break;
                case PGSQL_ATTR_DISABLE_PREPARES:
                    return_value = this->_disable_prepares;
                    break;
                case ATTR_CLIENT_VERSION:
                    return_value = PG_VERSION;
                    break;
                case ATTR_SERVER_VERSION:
                    if (PQprotocolVersion(this->_server) >= 3) { /* PostgreSQL 7.4 or later */
                        return_value = (char *) PQparameterStatus(this->_server, "server_version");
                    } else /* emulate above via a query */
                    {
                        PGresult *res = PQexec(this->_server, "SELECT VERSION()");
                        if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
                            return_value = (std::string) PQgetvalue(res, 0, 0);
                        }
                        if (res) {
                            PQclear(res);
                        }
                    }
                    break;
                case ATTR_CONNECTION_STATUS:
                    switch (PQstatus(this->_server)) {
                        case CONNECTION_STARTED:
                            return_value = "Waiting for connection to be made.";
                            break;
                        case CONNECTION_MADE:
                        case CONNECTION_OK:
                            return_value = "Connection OK; waiting to send.";
                            break;
                        case CONNECTION_AWAITING_RESPONSE:
                            return_value = "Waiting for a response from the server.";
                            break;
                        case CONNECTION_AUTH_OK:
                            return_value = "Received authentication; waiting for backend start-up to finish.";
                            break;
                        #ifdef CONNECTION_SSL_STARTUP
                        case CONNECTION_SSL_STARTUP:
                        return_value = "Negotiating SSL encryption.";
                    break;
                        #endif
                        case CONNECTION_SETENV:
                            return_value = "Negotiating environment-driven parameter settings.";
                            break;
                        case CONNECTION_BAD:
                        default:
                            return_value = "Bad connection.";
                            break;
                    }
                    break;
                case ATTR_SERVER_INFO: {
                    int spid = PQbackendPID(this->_server);
                    return_value = "PID: " + std::to_string(spid) + "; Client Encoding: " +
                                   (char *) PQparameterStatus(this->_server, "client_encoding") + "; Is Superuser: " +
                                   (char *) PQparameterStatus(this->_server, "is_superuser") +
                                   "; Session Authorization: " +
                                   (char *) PQparameterStatus(this->_server, "session_authorization") +
                                   "; Date Style: " + (char *) PQparameterStatus(this->_server, "DateStyle");
                }
                    break;
                default:
                    return 0;
            }
            return 1;
        }

        int pgsql_data_object::check_liveness() {
            if (PQstatus(this->_server) == CONNECTION_BAD) {
                PQreset(this->_server);
            }
            return (PQstatus(this->_server) == CONNECTION_OK) ? 1 : 0;
        }

        int pgsql_data_object::in_transaction_func() {
            return PQtransactionStatus(this->_server) > PQTRANS_IDLE;
        }

        int pgsql_data_object::pgsql_error(pgsql_data_object *dbh,
                                           pgsql_statement *stmt,
                                           int errcode,
                                           const std::string sqlstate,
                                           const std::string msg,
                                           const std::string file,
                                           int line) {
            std::string &pdo_err = stmt ? stmt->_error_code : dbh->_error_code;
            error_info &einfo = dbh->_einfo;
            std::string errmsg = PQerrorMessage(dbh->_server);
            einfo.errcode = errcode;
            einfo.file = file;
            einfo.line = line;
            if (!einfo.errmsg.empty()) {
                einfo.errmsg.clear();
            }
            if (sqlstate.empty() || (sqlstate.length()) >= 6) {
                pdo_err = "HY000";
            } else {
                pdo_err = sqlstate;
            }
            if (!msg.empty()) {
                einfo.errmsg = msg;
            } else if (!errmsg.empty()) {
                einfo.errmsg = pgsql_data_object::trim_message(errmsg);
            }
            return errcode;
        }

        std::string pgsql_data_object::trim_message(std::string &message) {
            int i = message.length() - 1;
            std::string tmp;
            if (i > 1 && (message[i - 1] == '\r' || message[i - 1] == '\n') && message[i] == '.') {
                --i;
            }
            while (i > 0 && (message[i] == '\r' || message[i] == '\n')) {
                --i;
            }
            ++i;
            tmp.reserve(i + 1);
            tmp = message.substr(0, i);
            tmp.back() = '\0';
            return tmp;
        }

        int pgsql_data_object::transaction_cmd(const std::string cmd) {
            PGresult *res;
            int ret = 1;
            res = PQexec(this->_server, cmd.c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                pgsql_data_object::pgsql_error(this, nullptr, PQresultStatus(res),
                                               PQresultErrorField(res, PG_DIAG_SQLSTATE), "", __FILE__, __LINE__);
                ret = 0;
            }
            PQclear(res);
            return ret;
        }

        std::shared_ptr<pgsql_lob_self> pgsql_data_object::create_lob_stream(int lfd, Oid oid) {
            std::shared_ptr<pgsql_lob_self> self(new pgsql_lob_self);
            self->dbh = this;
            self->lfd = lfd;
            self->oid = oid;
            self->conn = this->_server;
            if (self) {
                return self;
            }
            return nullptr;
        }
        ///////////////////////////////////////////////////////////////
        //                 TYPE ALIAS WITHOUT TEMPLATE               //
        ///////////////////////////////////////////////////////////////
        using pgsql = pgsql_data_object;
    }
}
#endif //WPP_PGSQL_DRIVER_H
