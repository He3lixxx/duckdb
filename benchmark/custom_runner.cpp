#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "fmt/format.h"
#include "tpch/dbgen/include/dbgen/dbgen.hpp"

#include <chrono>
#include <iostream>
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
	// TODO
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

	std::cout << "\n\nNative execution (views)" << std::endl;
	BenchmarkNative(&con);
	std::cout << "\n\nArrow in-memory representation" << std::endl;
	BenchmarkArrow(&con);
	std::cout << "\n\nParquet files" << std::endl;
	BenchmarkParquet(&con);
}
