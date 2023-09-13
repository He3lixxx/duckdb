#include "arrow/arrow_test_helper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "fmt/format.h"
#include "tpch/dbgen/include/dbgen/dbgen.hpp"

#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

std::string FormatDuration(const std::chrono::duration<double> &duration) {
	return std::to_string(duration.count()) + "s";
}

template <typename FUNC_T>
std::chrono::duration<double> Measure(FUNC_T &&func) {
	auto start_time = std::chrono::steady_clock::now();

	func();

	auto end_time = std::chrono::steady_clock::now();
	return end_time - start_time;
}

template <typename FUNC_T>
void MeasureAndPrintDuration(FUNC_T &&func, const std::string &step_description) {
	std::cout << step_description << "... " << std::flush;
	auto time_taken = Measure(std::forward<FUNC_T>(func));
	std::cout << "done (" << FormatDuration(time_taken) << ")" << std::endl;
}

std::vector<std::string> table_names = {"nation",   "region", "part",     "supplier", "partsupp",
                                        "customer", "orders", "lineitem", "nation"};
std::vector<std::string> queries(22);
constexpr size_t ITERATIONS_PER_QUERY = 5;

void RunQueries(duckdb::Connection *con) {
	for (size_t query_index = 0; query_index < queries.size(); ++query_index) {
		std::string &query = queries[query_index];
		std::string query_name = duckdb_fmt::format("q{:02}", query_index + 1);

		for (size_t iteration = 0; iteration < ITERATIONS_PER_QUERY; ++iteration) {
			std::unique_ptr<duckdb::MaterializedQueryResult> result;
			MeasureAndPrintDuration([&]() { result = con->Query(query); }, query_name);
			if (result->HasError()) {
				result->ThrowError();
			}
		}
	}
}

void BenchmarkNative(duckdb::Connection *con) {
	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("CREATE VIEW {0} AS SELECT * FROM {0}_generated;", table));
	}

	RunQueries(con);

	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("DELETE VIEW {0};", table));
	}
}

void BenchmarkParquet(duckdb::Connection *con) {
	MeasureAndPrintDuration(
	    [&]() {
		    for (const auto &table : table_names) {
			    con->Query(duckdb_fmt::format("COPY {0}_generated TO './{0}.parquet' (FORMAT PARQUET);", table));
			    con->Query(
			        duckdb_fmt::format("CREATE VIEW {0} AS SELECT * FROM read_parquet('./{0}.parquet');", table));
		    }
	    },
	    "setup");

	RunQueries(con);

	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("DELETE VIEW {0};", table));
	}
}

void BenchmarkArrow(duckdb::Connection *con) {
	// None of this is documented, its just copied together from ArrowTestHelper::RunArrowComparison and adbc::Ingest

	for (const auto &table : table_names) {
		auto duckdb_rows = con->Query(duckdb_fmt::format("SELECT * FROM {}_generated", table));

		// Convert duckdb rows to arrow using duckdb test helpers
		auto client_config = duckdb::ClientConfig::GetConfig(*con->context);
		duckdb::ArrowOptions arrow_option(con->context->db->config.options.arrow_offset_size,
		                                  client_config.ExtractTimezone());
		auto types = duckdb_rows->types;
		auto names = duckdb_rows->names;

		// TODO: Memory cleanup
		duckdb::ArrowTestFactory *factory = new duckdb::ArrowTestFactory(std::move(types), std::move(names),
		                                                                 std::move(duckdb_rows), true, arrow_option);

		// construct the arrow scan
		auto params = duckdb::ArrowTestHelper::ConstructArrowScan(reinterpret_cast<uintptr_t>(factory), true);
		con->TableFunction("arrow_scan", params)->CreateView(table, true, true);
	}

	// TODO: Crashes: Probably we can't reuse the Arrow-Stream?
	// Are we even sure data is read from an arrow format here? Can't really find a place in the code where it's
	// actually materialized as arrow
	RunQueries(con);

	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("DELETE VIEW {0};", table));
	}
}

struct CustomTableFunctionInput : public duckdb::TableFunctionInput {
};

struct CustomTableFunctionBindData : public duckdb::FunctionData {
	duckdb::DataChunk *table;

	// Taken from
	// https://github.com/duckdblabs/postgres_scanner/blob/cd043b49cdc9e0d3752535b8333c9433e1007a48/postgres_scanner.cpp#L62
	duckdb::unique_ptr<FunctionData> Copy() const override {
		throw duckdb::NotImplementedException("");
	}
	bool Equals(const FunctionData &other) const override {
		throw duckdb::NotImplementedException("");
	}
};

duckdb::DataChunk my_custom_global_table;

static duckdb::unique_ptr<duckdb::FunctionData> CustomTableFunctionBind(duckdb::ClientContext &context,
                                                                     duckdb::TableFunctionBindInput &input,
                                                                     duckdb::vector<duckdb::LogicalType> &return_types,
                                                                     duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CustomTableFunctionBindData>();
	// TODO -- irgendwie als Argument / UserData reingeben?
	bind_data->table = &my_custom_global_table;

	// ExTODO -- multiple tables / DataChunks.
	names.push_back("Meine Spalte 1");
	names.push_back("Ein anderer Name");
	return_types.push_back(duckdb::LogicalType::BIGINT);
	return_types.push_back(duckdb::LogicalType::BIGINT);

	return std::move(bind_data);
}

static void CustomTableFunctionImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                    duckdb::DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CustomTableFunctionBindData>();
	output.Reference(*bind_data.table);
}

void BenchmarkCustomTableFunction(duckdb::Connection *con) {
	// ExTODO: Pro Table: Eine map<std::string, duckdb::DataChunk>, befüllen mit den Daten aus DuckDB `_generated`
	// tables, columnwise In der Table-Function müssen wir dann den Vector des DataChunks so ändern, dass er unsere
	// Daten referenziert Dann sollten wir zero-copy-scan haben

	con->BeginTransaction();

	// Erstmal einfachere Variante: Einfach nur zwei int-columns, mit random data
	my_custom_global_table.Initialize(*con->context, {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT});

	std::mt19937_64 rng(std::random_device {}());
	for (duckdb::Vector &column_data : my_custom_global_table.data) {
		// Total wild, dass man weder den Vector noch den Chunk nach seiner size / capacity fragen kann
		for (size_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
			column_data.SetValue(i, static_cast<int64_t>(rng()));
		}
	}

	// DuckDB Table-Function registrieren.
	duckdb::TableFunction custom_table_function(/*name=*/"custom_table_fn",
	                                            /*arguments=*/ {duckdb::LogicalType::VARCHAR},
	                                            /*function=*/CustomTableFunctionImpl, /*bind=*/CustomTableFunctionBind);

	// Copied from duckdb_register_table_function
	auto &catalog = duckdb::Catalog::GetSystemCatalog(*con->context);
	duckdb::CreateTableFunctionInfo function_info(custom_table_function);
	catalog.CreateTableFunction(*con->context, function_info);

	// Views für die Tabellennamen erzeugen
	for (const auto &table : table_names) {
		duckdb::vector<duckdb::Value> params = {table};
		con->TableFunction("custom_table_fn", params)->CreateView(table);
	}

	// ExTODO: Geht Filter Pushdown? Wenn ja, wie?

	con->Commit();

	std::cout << "$ SHOW TABLES" << std::endl;
	con->Query("SHOW TABLES")->Print();
	std::cout << "$ SELECT COUNT(*) FROM custom_table_fn('test')" << std::endl;
	con->Query("SELECT COUNT(*) FROM custom_table_fn('test')")->Print();

	con->Query("SELECT COUNT(*) FROM lineitem")->Print();
	// ExTODO: RunQueries(con);

	con->BeginTransaction();
	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("DELETE VIEW {0};", table));
	}
	con->Commit();
}

int main() {
	duckdb::DuckDB db;
	duckdb::Connection con(db);

	MeasureAndPrintDuration(
	    [&]() {
		    for (int i = 1; i <= 22; ++i) {
			    queries[i - 1] = tpch::DBGenWrapper::GetQuery(i);
		    }
	    },
	    "getting queries");

	double scale_factor = 1;
#ifdef DEBUG
	scale_factor = 0.01;
#endif

	std::cout << "\n\nCustom Table Function" << std::endl;
	BenchmarkCustomTableFunction(&con);
	return 0; // ExTODO

	std::string dbgen_query = duckdb_fmt::format("CALL dbgen(sf={}, suffix=\"_generated\")", scale_factor);
	MeasureAndPrintDuration([&]() { con.Query(dbgen_query); }, duckdb_fmt::format("generating data ({})", dbgen_query));

	std::cout << "\n\nNative execution (views)" << std::endl;
	BenchmarkNative(&con);
	std::cout << "\n\nArrow in-memory representation" << std::endl;
	BenchmarkArrow(&con);
	std::cout << "\n\nParquet files" << std::endl;
	BenchmarkParquet(&con);
}
