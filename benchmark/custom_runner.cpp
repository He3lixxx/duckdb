#include "arrow/arrow_test_helper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/table_function.hpp"
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

std::vector<std::string> table_names = {"nation",   "region",   "part",   "supplier",
                                        "partsupp", "customer", "orders", "lineitem"};
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
		con->Query(duckdb_fmt::format("DROP VIEW {0};", table));
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
		con->Query(duckdb_fmt::format("DROP VIEW {0};", table));
	}
}

void BenchmarkArrow(duckdb::Connection *con) {
	// None of this is documented, its just copied together from ArrowTestHelper::RunArrowComparison and adbc::Ingest
	// Doesn't quite work currently. Error indicates that the arrow data seems to have some streaming semantics and
	// can not be consumed multiple times
	// Are we even sure data is read from an arrow format here? Can't really find a place in the code where it's
	// actually materialized as arrow

	for (const auto &table : table_names) {
		auto duckdb_rows = con->Query(duckdb_fmt::format("SELECT * FROM {}_generated", table));

		// Convert duckdb rows to arrow using duckdb test helpers
		auto client_config = duckdb::ClientConfig::GetConfig(*con->context);
		duckdb::ArrowOptions arrow_option(con->context->db->config.options.arrow_offset_size,
		                                  client_config.ExtractTimezone());
		auto types = duckdb_rows->types;
		auto names = duckdb_rows->names;

		duckdb::ArrowTestFactory *factory = new duckdb::ArrowTestFactory(std::move(types), std::move(names),
		                                                                 std::move(duckdb_rows), true, arrow_option);

		// construct the arrow scan
		auto params = duckdb::ArrowTestHelper::ConstructArrowScan(reinterpret_cast<uintptr_t>(factory), true);
		con->TableFunction("arrow_scan", params)->CreateView(table, true, true);
	}

	RunQueries(con);

	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("DROP VIEW {0};", table));
	}
}

struct CustomTable {
	duckdb::vector<duckdb::string> column_names;
	duckdb::vector<duckdb::LogicalType> column_types;

	duckdb::vector<duckdb::unique_ptr<duckdb::DataChunk>> data_chunks;
};

std::unordered_map<std::string, CustomTable> tables;

struct CustomTableFunctionLocalState : public duckdb::LocalTableFunctionState {
	size_t chunk_offset = 0;
};

static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
CustomTableFunctionInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                             duckdb::GlobalTableFunctionState *global_state) {
	auto state = duckdb::make_uniq<CustomTableFunctionLocalState>();

	return std::move(state);
}

struct CustomTableFunctionBindData : public duckdb::FunctionData {
	CustomTable *table;

	duckdb::unique_ptr<FunctionData> Copy() const override {
		throw duckdb::NotImplementedException("CustomTableFunctionBindData::Copy");
	}
	bool Equals(const FunctionData &other) const override {
		if (const CustomTableFunctionBindData *other_cast = dynamic_cast<const CustomTableFunctionBindData *>(&other)) {
			return other_cast->table == table;
		}
		return false;
	}
};

static duckdb::unique_ptr<duckdb::FunctionData>
CustomTableFunctionBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                        duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<duckdb::string> &names) {
	auto bind_data = duckdb::make_uniq<CustomTableFunctionBindData>();

	std::string requested_table_name = input.inputs[0].GetValue<std::string>();
	CustomTable &table = tables[requested_table_name];
	bind_data->table = &table;

	for (auto &name : table.column_names) {
		names.push_back(name);
	}

	for (auto &type : table.column_types) {
		return_types.push_back(type);
	}

	return std::move(bind_data);
}

static void CustomTableFunctionImpl(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                    duckdb::DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CustomTableFunctionBindData>();
	auto &local_state = data_p.local_state->Cast<CustomTableFunctionLocalState>();

	if (local_state.chunk_offset >= bind_data.table->data_chunks.size()) {
		return;
	}

	output.Reference(*bind_data.table->data_chunks[local_state.chunk_offset++]);
}

void BenchmarkCustomTableFunction(duckdb::Connection *con) {
	con->BeginTransaction();

	for (const auto &table_name : table_names) {
		CustomTable &custom_table = tables[table_name];

		duckdb::unique_ptr<duckdb::MaterializedQueryResult> data =
		    con->Query(duckdb_fmt::format("SELECT * FROM {}_generated", table_name));

		custom_table.column_types = data->types;
		custom_table.column_names = data->names;

		// TODO: Should we use ColumnDataCollection, duckdb's wrapper around multiple data chunks?
		while (auto result_chunk = data->Fetch()) {
			custom_table.data_chunks.emplace_back(duckdb::make_uniq<duckdb::DataChunk>());
			auto &table_chunk = custom_table.data_chunks.back();

			D_ASSERT(result_chunk->size() <= STANDARD_VECTOR_SIZE);
			table_chunk->Initialize(*con->context, result_chunk->GetTypes());
			result_chunk->Copy(*table_chunk);
		}
	}

	// DuckDB Table-Function registrieren.
	duckdb::TableFunction custom_table_function(/*name=*/"custom_table_fn",
	                                            /*arguments=*/ {duckdb::LogicalType::VARCHAR},
	                                            /*function=*/CustomTableFunctionImpl, /*bind=*/CustomTableFunctionBind,
	                                            /*init_global=*/nullptr, /*init_local=*/CustomTableFunctionInitLocal);

	// Copied from duckdb_register_table_function
	auto &catalog = duckdb::Catalog::GetSystemCatalog(*con->context);
	duckdb::CreateTableFunctionInfo function_info(custom_table_function);
	catalog.CreateTableFunction(*con->context, function_info);

	// Views für die Tabellennamen erzeugen
	for (const auto &table : table_names) {
		duckdb::vector<duckdb::Value> params = {table};
		con->TableFunction("custom_table_fn", params)->CreateView(table);
	}

	// TODO: Bringt Filter Pushdown etwas? (Ansatz: TableFunction hat `filter_pushdown` Attribut)

	con->Commit();

	RunQueries(con);

	for (const auto &table : table_names) {
		con->Query(duckdb_fmt::format("DROP VIEW {0};", table));
	}

	for (auto &table_name_table_pair : tables) {
		for (auto &chunk : table_name_table_pair.second.data_chunks) {
			chunk->Destroy();
		}
	}
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

	std::string dbgen_query = duckdb_fmt::format("CALL dbgen(sf={}, suffix=\"_generated\")", scale_factor);
	MeasureAndPrintDuration([&]() { con.Query(dbgen_query); }, duckdb_fmt::format("generating data ({})", dbgen_query));

	std::cout << "\n\nCustom Table Function" << std::endl;
	BenchmarkCustomTableFunction(&con);

	std::cout << "\n\nNative execution (views)" << std::endl;
	BenchmarkNative(&con);

	// std::cout << "\n\nArrow in-memory representation" << std::endl;
	// BenchmarkArrow(&con);

	std::cout << "\n\nParquet files" << std::endl;
	BenchmarkParquet(&con);
}
