// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
#pragma once
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
const char *kelvo_runtime_version(void);
enum kelvo_predicate_type {
	KELVO_PREDICATE_BOOL = 1,
	KELVO_PREDICATE_INT8,
	KELVO_PREDICATE_INT16,
	KELVO_PREDICATE_INT32,
	KELVO_PREDICATE_INT64,
	KELVO_PREDICATE_UINT8,
	KELVO_PREDICATE_UINT16,
	KELVO_PREDICATE_UINT32,
	KELVO_PREDICATE_UINT64,
	KELVO_PREDICATE_DATE32
};
struct kelvo_predicate_column {
	uint32_t ordinal;
	uint8_t scalar_type;
};
void *kelvo_factory_create(uint64_t handle, uint32_t schema_columns,
                          const struct kelvo_predicate_column *columns, size_t count);
void kelvo_factory_destroy(void *factory);
int kelvo_factory_register(void *factory, void *connection, const char *schema, const char *name);
void kelvo_schema_zero(void *schema);
void kelvo_array_zero(void *array);
void kelvo_stream_init(void *stream, uint64_t handle);
int kelvo_array_pin_release(void *array, uint64_t handle);
int kelvo_go_factory_schema(uint64_t handle, void *schema);
int kelvo_go_produce(uint64_t handle, char *plan, size_t length, void *stream);
int kelvo_go_stream_schema(uint64_t handle, void *schema);
int kelvo_go_next(uint64_t handle, void *array);
void kelvo_go_stream_release(uint64_t handle);
void kelvo_go_unpin(uint64_t handle);
void kelvo_go_fail(uint64_t handle, int code);
#ifdef __cplusplus
}
#endif
