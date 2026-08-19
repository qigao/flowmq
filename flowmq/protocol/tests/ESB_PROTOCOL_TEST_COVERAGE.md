# FlowMQ ESB Protocol Test Coverage

## Test Suite: `test_flowmq_protocol_esb`

### Coverage Summary

Total test cases: **11**

### Test Cases

1. **Pattern Validation** (`validates ESB patterns`)
   - ✓ Valid ESB patterns (12-21)
   - ✓ Invalid patterns (0, base patterns 1-11, out of range)

2. **Pattern Compatibility** (`checks ESB pattern compatibility`)
   - ✓ SCATTER ↔ GATHER
   - ✓ SAGA_COORDINATOR ↔ SAGA_PARTICIPANT
   - ✓ STREAM_PRODUCER ↔ STREAM_CONSUMER
   - ✓ STREAM_CONSUMER ↔ STREAM_CONSUMER (consumer group)
   - ✓ PRIORITY_PRODUCER ↔ PRIORITY_CONSUMER
   - ✓ CB_CLIENT ↔ CB_SERVICE
   - ✓ Cross-pattern incompatibility

3. **SCATTER/GATHER Encoding** (`encodes and decodes scatter/gather frame`)
   - ✓ SCATTER_REQUEST frame
   - ✓ expected_responses field
   - ✓ aggregation_policy field
   - ✓ Round-trip encode/decode

4. **Partial Response** (`encodes and decodes partial response frame`)
   - ✓ PARTIAL_RESPONSE frame
   - ✓ partial_index field

5. **STREAM Encoding** (`encodes and decodes stream frame with consumer group`)
   - ✓ STREAM_PUBLISH frame
   - ✓ partition_id field
   - ✓ offset field (uint64_t)
   - ✓ consumer_group field (borrowed view)

6. **SAGA Encoding** (`encodes and decodes saga frame`)
   - ✓ SAGA_EXECUTE frame
   - ✓ saga_id field (uint64_t)
   - ✓ saga_step field
   - ✓ saga_state field

7. **Priority Queue** (`encodes and decodes priority queue frame`)
   - ✓ PRIORITY_PUBLISH frame
   - ✓ priority field (0-255)

8. **Circuit Breaker** (`encodes and decodes circuit breaker frame`)
   - ✓ CB_STATUS frame
   - ✓ cb_state field
   - ✓ failure_count field

9. **Size Validation** (`rejects frame size exceeding max_frame_size`)
   - ✓ Frame size limit enforcement

10. **Boundary Validation** (`rejects invalid expected_responses count`)
    - ✓ MAX_FANOUT_COUNT enforcement

11. **Empty Frame** (`handles empty ESB frame (no TLV fields)`)
    - ✓ Frame with no ESB-specific fields
    - ✓ Zero-initialized fields after decode

## TLV Field Coverage

| Field Type | Encode | Decode | Validation |
|------------|--------|--------|------------|
| EXPECTED_RESPONSES | ✓ | ✓ | ✓ (≤1024) |
| AGGREGATION_POLICY | ✓ | ✓ | ✓ |
| PARTIAL_INDEX | ✓ | ✓ | ✓ |
| SAGA_ID | ✓ | ✓ | ✓ |
| SAGA_STEP | ✓ | ✓ | ✓ (≤256) |
| SAGA_STATE | ✓ | ✓ | ✓ |
| PARTITION_ID | ✓ | ✓ | ✓ |
| OFFSET | ✓ | ✓ | ✓ |
| CONSUMER_GROUP | ✓ | ✓ | ✓ (≤255 bytes) |
| PRIORITY | ✓ | ✓ | ✓ (0-255) |
| CB_STATE | ✓ | ✓ | ✓ |
| FAILURE_COUNT | ✓ | ✓ | ✓ |

## Error Handling Coverage

- ✓ NULL pointer validation
- ✓ Invalid pattern rejection
- ✓ Frame size overflow (TURBO_EMSGSIZE)
- ✓ Boundary violation (TURBO_EPROTO)
- ✓ Unknown TLV type (forward compatibility - skip)
- ✓ TLV length mismatch (TURBO_EPROTO)

## Integration with Base Protocol

- ✓ Extends flowmq_protocol_frame_t without breaking ABI
- ✓ TLV payload embedded in base frame payload
- ✓ Cleanup propagates to base frame
- ✓ Borrowed views lifetime tied to base frame

## Next Steps

1. Run tests: `ctest --preset win-dev-user -R test_flowmq_protocol_esb --output-on-failure`
2. Verify integration with flowmq_pattern.c
3. Performance benchmark for TLV encode/decode overhead
4. Fuzz testing for malformed TLV input
