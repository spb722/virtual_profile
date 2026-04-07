#!/bin/bash
# =============================================================================
# TRACK 3 — GEO + SNAPSHOT END-TO-END TESTS (Resolver API — port 8001)
# =============================================================================
# These test the FULL pipeline: NL → classifier → agent → KPI mapper → template
# Validates: correct sub_type routing, N value, and final parent_condition output
#
# WHAT TO CHECK FOR EACH TEST:
#   - Track classification  : should say Track 3 (SNAPSHOT)
#   - sub_type              : should match the group label in each test
#   - N value               : should match the number in the input sentence
#   - parent_condition      : should match the expected template shown in each test
# =============================================================================

BASE="http://localhost:8001/resolve"

# =============================================================================
# GROUP 1 — geo_last_n_days
# Expected parent_condition:
#   dpi_geo_location_event_date >= CurrentTime-{N}DAYS
#   AND dpi_geo_location_region ${operator} ${value}
#   AND COUNT_ALL(GEO_LOCATION_MSISDN) >= 1
# =============================================================================

echo "======================================================================"
echo "GEO TEST 1a: geo_last_n_days — 30 days"
echo "Expected sub_type: geo_last_n_days | N: 30"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who have been detected in region northoman at least once in the last 30 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "GEO TEST 1b: geo_last_n_days — 7 days"
echo "Expected sub_type: geo_last_n_days | N: 7"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who have been detected in region northoman at least once in the last 7 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "GEO TEST 1c: geo_last_n_days — 90 days"
echo "Expected sub_type: geo_last_n_days | N: 90"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who have been detected in region northoman at least once in the last 90 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "GEO TEST 1d: geo_last_n_days — yesterday (N should resolve to 1)"
echo "Expected sub_type: geo_last_n_days | N: 1"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who were found in region northoman yesterday"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 2 — geo_last_n_months
# Expected parent_condition:
#   dpi_geo_location_event_date >= CurrentMonth-{N}MONTHS
#   AND dpi_geo_location_region ${operator} ${value}
#   AND COUNT_ALL(GEO_LOCATION_MSISDN) >= 1
# =============================================================================

echo "======================================================================"
echo "GEO TEST 2a: geo_last_n_months — 2 months"
echo "Expected sub_type: geo_last_n_months | N: 2"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who have been detected in region northoman at least once in the last 2 months"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "GEO TEST 2b: geo_last_n_months — 6 months"
echo "Expected sub_type: geo_last_n_months | N: 6"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who were present in region northoman at least once in the last 6 months"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 3 — geo_current
# Expected parent_condition:
#   {lon_col} <> NULL AND {lat_col} <> NULL AND {geo_name_col} ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "GEO TEST 3a: geo_current — current location check"
echo "Expected sub_type: geo_current | N: null"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers whose current location is region northoman"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "GEO TEST 3b: geo_current — currently present"
echo "Expected sub_type: geo_current | N: null"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers currently present in region northoman"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 4 — snapshot_max_check
# Expected parent_condition:
#   {id_col} <> NULL AND MAX({ref_col}) > 0
# =============================================================================

echo "======================================================================"
echo "SNAPSHOT TEST 4a: snapshot_max_check — prepaid voice revenue"
echo "Expected sub_type: snapshot_max_check | N: null"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who have prepaid voice revenue on event date >= 500 and MAX prepaid voice revenue is greater than zero"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "SNAPSHOT TEST 4b: snapshot_max_check — HBB ID"
echo "Expected sub_type: snapshot_max_check | N: null"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers with HBB add-on component >= 500 who have a valid HBB ID and MAX HBB ID is not null"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 5 — snapshot_by_date_boundary
# Expected parent_condition:
#   {date_col} = CurrentTime-{N}DAYS
#   AND {id_col} ${operator} ${value}
#   AND COUNT_ALL({count_col}) = 0
# =============================================================================

echo "======================================================================"
echo "SNAPSHOT TEST 5a: snapshot_by_date_boundary — yesterday"
echo "Expected sub_type: snapshot_by_date_boundary | N: 1"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who activated HBB add-on component >= 500 yesterday and have no active fixed line"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "SNAPSHOT TEST 5b: snapshot_by_date_boundary — one day ago"
echo "Expected sub_type: snapshot_by_date_boundary | N: 1"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who activated HBB add-on component >= 500 one day ago and have no active fixed line"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 6 — snapshot_by_id with separate join key
# Expected parent_condition:
#   HBBID = ${HBBID} AND HBBAddon_Inact_Date ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "SNAPSHOT TEST 6a: snapshot_by_id — separate join key (HBBID)"
echo "Expected sub_type: snapshot_by_id | id_col: HBBID"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers with HBB ID matching the specified ID and HBB add-on deactivation date >= 500"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "SNAPSHOT TEST 6b: snapshot_by_id — separate join key (HBB_ID variant)"
echo "Expected sub_type: snapshot_by_id | id_col: HBB_ID"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers with the specified HBB ID and HBB add-on deactivation date >= 500"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 7 — snapshot_by_id normal (no separate join key)
# Expected parent_condition:
#   {value_col} ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "SNAPSHOT TEST 7a: snapshot_by_id — plain attribute check"
echo "Expected sub_type: snapshot_by_id | id_col: null"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers whose prepaid voice revenue on event date >= 500"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "SNAPSHOT TEST 7b: snapshot_by_id — plain date attribute check"
echo "Expected sub_type: snapshot_by_id | id_col: null"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers whose HBB add-on deactivation date >= 500"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 8 — Classifier boundary cases
# These confirm the Track 1 vs Track 3 disambiguation is working correctly
# =============================================================================

echo "======================================================================"
echo "BOUNDARY TEST 8a: should be Track 3 — presence check with time window"
echo "Expected: Track 3 (SNAPSHOT)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who appeared in region northoman in the last 15 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "BOUNDARY TEST 8b: should be Track 3 — ever found"
echo "Expected: Track 3 (SNAPSHOT)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers who were ever found in region northoman in the last 60 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "BOUNDARY TEST 8c: should be Track 1 — count IS the metric"
echo "Expected: Track 1 (TIME_SERIES) — must NOT go to Track 3"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers whose visit count to region northoman in the last 30 days is greater than 10"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "BOUNDARY TEST 8d: should be Track 1 — total detections as metric"
echo "Expected: Track 1 (TIME_SERIES) — must NOT go to Track 3"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customers whose total number of detections in region northoman in the last 30 days >= 5"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "ALL TRACK 3 TESTS COMPLETE"
echo "======================================================================"
echo ""
echo "QUICK PASS/FAIL CHECKLIST:"
echo "  Group 1 (1a-1d) : track=3, sub_type=geo_last_n_days,         N matches input"
echo "  Group 2 (2a-2b) : track=3, sub_type=geo_last_n_months,       N matches input"
echo "  Group 3 (3a-3b) : track=3, sub_type=geo_current,             N=null"
echo "  Group 4 (4a-4b) : track=3, sub_type=snapshot_max_check,      N=null"
echo "  Group 5 (5a-5b) : track=3, sub_type=snapshot_by_date_boundary, N=1"
echo "  Group 6 (6a-6b) : track=3, sub_type=snapshot_by_id,          id_col present"
echo "  Group 7 (7a-7b) : track=3, sub_type=snapshot_by_id,          id_col=null"
echo "  Group 8 (8a-8b) : track=3 SNAPSHOT  — presence check cases"
echo "  Group 8 (8c-8d) : track=1 TIME_SERIES — must NOT be Track 3"