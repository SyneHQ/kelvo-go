//go:build duckbridge && duckdb_arrow && cgo

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
#include "shim.h"
#include "duckdb.hpp"
#include "duckdb.h"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include <cstring>
#include <new>

using namespace duckdb;

namespace {
struct FactoryState { uint64_t handle; };

bool SupportsPushdown(const FunctionData &data, idx_t column) {
	const auto &arrow = data.Cast<ArrowScanFunctionData>();
	if (column >= arrow.all_types.size()) { return false; }
	// Advertise only the exact scalar types understood by the Go contract.
	// DuckDB keeps other predicates above the scan instead of handing us a
	// required predicate that a source cannot safely implement (collations,
	// floating-point, decimal and timezone semantics are engine-local).
	switch (arrow.all_types[column].id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT: return true;
	default: return false;
	}
}

string JsonString(const string &value) {
	string out = "\"";
	const char hex[] = "0123456789abcdef";
	for (unsigned char c : value) {
		if (c == '"' || c == '\\') { out += '\\'; out += char(c); }
		else if (c < 0x20) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
		else { out += char(c); }
	}
	return out + "\"";
}

string Unsupported() { throw NotImplementedException("Native source pushed filter is unsupported"); }

string ConstantType(const Value &value) {
	if (value.IsNull()) { return Unsupported(); }
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN: return "bool";
	case LogicalTypeId::TINYINT: return "int8";
	case LogicalTypeId::SMALLINT: return "int16";
	case LogicalTypeId::INTEGER: return "int32";
	case LogicalTypeId::BIGINT: return "int64";
	case LogicalTypeId::UTINYINT: return "uint8";
	case LogicalTypeId::USMALLINT: return "uint16";
	case LogicalTypeId::UINTEGER: return "uint32";
	case LogicalTypeId::UBIGINT: return "uint64";
	default: return Unsupported();
	}
}

string FilterJSON(const TableFilter &filter, const string &column, idx_t depth) {
	if (depth > 32) { return Unsupported(); }
	switch (filter.filter_type) {
	case TableFilterType::OPTIONAL_FILTER:
		// The pinned enum explicitly defines this wrapper as nonmandatory.
		return "";
	case TableFilterType::IS_NULL:
		return "{\"kind\":\"is_null\",\"column\":" + JsonString(column) + "}";
	case TableFilterType::IS_NOT_NULL:
		return "{\"kind\":\"is_not_null\",\"column\":" + JsonString(column) + "}";
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &comparison = filter.Cast<ConstantFilter>();
		string op;
		switch (comparison.comparison_type) {
		case ExpressionType::COMPARE_EQUAL: op = "eq"; break;
		case ExpressionType::COMPARE_NOTEQUAL: op = "ne"; break;
		case ExpressionType::COMPARE_LESSTHAN: op = "lt"; break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: op = "le"; break;
		case ExpressionType::COMPARE_GREATERTHAN: op = "gt"; break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = "ge"; break;
		default: return Unsupported();
		}
		return "{\"kind\":\"comparison\",\"column\":" + JsonString(column) + ",\"op\":" + JsonString(op) +
		       ",\"type\":" + JsonString(ConstantType(comparison.constant)) + ",\"value\":" + JsonString(comparison.constant.ToString()) + "}";
	}
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR: {
		const auto &conjunction = static_cast<const ConjunctionFilter &>(filter);
		string children;
		idx_t child_count = 0;
		for (auto &child : conjunction.child_filters) {
			auto json = FilterJSON(*child, column, depth + 1);
			if (json.empty()) {
				// An optional child in OR cannot be dropped: it represents true.
				if (filter.filter_type == TableFilterType::CONJUNCTION_OR) { return Unsupported(); }
				continue;
			}
			if (!children.empty()) { children += ","; }
			children += json;
			child_count++;
			if (children.size() > 1 << 20) { return Unsupported(); }
		}
		if (children.empty()) { return ""; }
		if (child_count == 1) { return children; }
		return string("{\"kind\":\"") + (filter.filter_type == TableFilterType::CONJUNCTION_AND ? "and" : "or") + "\",\"children\":[" + children + "]}";
	}
	default: return Unsupported();
	}
}

string PlanJSON(ArrowStreamParameters &parameters) {
	string json = "{\"columns\":[";
	for (idx_t i = 0; i < parameters.projected_columns.columns.size(); i++) {
		if (i) { json += ","; }
		json += JsonString(parameters.projected_columns.columns[i]);
	}
	json += "],\"filters\":[";
	bool first = true;
	if (parameters.filters) {
		for (auto &entry : parameters.filters->filters) {
			auto name = parameters.projected_columns.projection_map.find(entry.first);
			if (name == parameters.projected_columns.projection_map.end()) { return Unsupported(); }
			auto filter = FilterJSON(*entry.second, name->second, 0);
			if (filter.empty()) { continue; }
			if (!first) { json += ","; }
			first = false;
			json += filter;
			if (json.size() > 1 << 20) { return Unsupported(); }
		}
	}
	return json + "]}";
}

unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t pointer, ArrowStreamParameters &parameters) {
	auto factory = reinterpret_cast<FactoryState *>(pointer);
	string plan;
	try { plan = PlanJSON(parameters); }
	catch (...) { kelvo_go_fail(factory->handle, 1); throw; }
	auto stream = make_uniq<ArrowArrayStreamWrapper>();
	stream->number_of_rows = -1;
	std::memset(&stream->arrow_array_stream, 0, sizeof(ArrowArrayStream));
	if (kelvo_go_produce(factory->handle, &plan[0], plan.size(), &stream->arrow_array_stream)) {
		throw InvalidInputException("Native source could not produce an Arrow stream");
	}
	return stream;
}

void Schema(ArrowArrayStream *pointer, ArrowSchema &schema) {
	auto factory = reinterpret_cast<FactoryState *>(pointer);
	std::memset(&schema, 0, sizeof(schema));
	if (kelvo_go_factory_schema(factory->handle, &schema)) {
		throw InvalidInputException("Native source schema is unavailable");
	}
}

int StreamSchema(ArrowArrayStream *stream, ArrowSchema *schema) {
	std::memset(schema, 0, sizeof(*schema));
	return kelvo_go_stream_schema(reinterpret_cast<uintptr_t>(stream->private_data), schema);
}
int StreamNext(ArrowArrayStream *stream, ArrowArray *array) {
	std::memset(array, 0, sizeof(*array));
	return kelvo_go_next(reinterpret_cast<uintptr_t>(stream->private_data), array);
}
const char *StreamError(ArrowArrayStream *) { return "Native source Arrow stream failed"; }
void StreamRelease(ArrowArrayStream *stream) {
	if (!stream->release) { return; }
	auto handle = reinterpret_cast<uintptr_t>(stream->private_data);
	stream->release = nullptr;
	stream->private_data = nullptr;
	kelvo_go_stream_release(handle);
}

struct PinnedArray {
	void (*release)(ArrowArray *);
	void *private_data;
	uint64_t handle;
};
void ArrayRelease(ArrowArray *array) {
	if (!array->release) { return; }
	auto pinned = static_cast<PinnedArray *>(array->private_data);
	array->release = pinned->release;
	array->private_data = pinned->private_data;
	array->release(array);
	kelvo_go_unpin(pinned->handle);
	delete pinned;
}
} // namespace

extern "C" const char *kelvo_runtime_version(void) { return duckdb_library_version(); }
extern "C" void *kelvo_factory_create(uint64_t handle) { return new (std::nothrow) FactoryState{handle}; }
extern "C" void kelvo_factory_destroy(void *factory) { delete static_cast<FactoryState *>(factory); }
extern "C" int kelvo_factory_register(void *factory, void *connection, const char *schema, const char *name) {
	try {
		auto conn = reinterpret_cast<Connection *>(connection);
		auto state = reinterpret_cast<FactoryState *>(factory);
		const string function_name = "kelvo_arrow_scan_" + std::to_string(state->handle);
		// Reuse the pinned Arrow scanner with Kelvo's actual filter capability.
		// A private per-factory function leaves DuckDB's built-in arrow_scan
		// untouched and lives in the disposable query database.
		ExtensionLoader loader(DatabaseInstance::GetDatabase(*conn->context), "kelvo");
		auto scan = loader.GetTableFunction("arrow_scan").functions.GetFunctionByOffset(0);
		scan.name = function_name;
		scan.supports_pushdown_type = SupportsPushdown;
		loader.RegisterFunction(scan);
		auto relation = conn->TableFunction(function_name, {Value::POINTER(reinterpret_cast<uintptr_t>(factory)), Value::POINTER(reinterpret_cast<uintptr_t>(Produce)), Value::POINTER(reinterpret_cast<uintptr_t>(Schema))});
		relation->CreateView(schema, name, false, false);
		return 0;
	} catch (...) { return 1; }
}
extern "C" void kelvo_schema_zero(void *schema) { std::memset(schema, 0, sizeof(ArrowSchema)); }
extern "C" void kelvo_array_zero(void *array) { std::memset(array, 0, sizeof(ArrowArray)); }
extern "C" void kelvo_stream_init(void *pointer, uint64_t handle) {
	auto stream = static_cast<ArrowArrayStream *>(pointer);
	std::memset(stream, 0, sizeof(*stream));
	stream->get_schema = StreamSchema;
	stream->get_next = StreamNext;
	stream->get_last_error = StreamError;
	stream->release = StreamRelease;
	stream->private_data = reinterpret_cast<void *>(static_cast<uintptr_t>(handle));
}
extern "C" int kelvo_array_pin_release(void *pointer, uint64_t handle) {
	auto array = static_cast<ArrowArray *>(pointer);
	auto pinned = new (std::nothrow) PinnedArray{array->release, array->private_data, handle};
	if (!pinned) { return 1; }
	array->release = ArrayRelease;
	array->private_data = pinned;
	return 0;
}
