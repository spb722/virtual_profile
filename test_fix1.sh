#!/bin/bash
# =============================================================================
# SESSION FIX TESTS — End-to-end tests for all changes made this session
# (Resolver API — port 8001)
# =============================================================================
# WHAT TO CHECK FOR EACH TEST:
#   - Track classification  : must match the expected track
#   - parent_condition      : must match the expected pattern shown in each test
# =============================================================================

BASE="http://localhost:8001/resolve"

# =============================================================================
# GROUP 1 — geo_current with region text column (Profile_Cdr_Region)
# Fix: template_current_region used when region_col present (no lat/lon 500)
# Expected parent_condition:
#   Profile_Cdr_Region <> NULL AND Profile_Cdr_Region ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "TEST 1a: geo_current — current geographic location is oman"
echo "Expected: Track 3 | Profile_Cdr_Region <> NULL AND Profile_Cdr_Region \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customer'\''s current geographic location is oman"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 1b: geo_current — current region check"
echo "Expected: Track 3 | Profile_Cdr_Region <> NULL AND Profile_Cdr_Region \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Customer current region is northoman"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 2 — Track 2 attr_check: direct profile attribute comparison
# Fix: classifier rule 2 updated — plain attribute threshold → Track 2 not Track 3
# Expected parent_condition:
#   <col> <> NULL AND <col> ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "TEST 2a: attr_check — network age numeric threshold"
echo "Expected: Track 2 | AON <> NULL AND AON \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "network age > 12 months"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 2b: attr_check — account balance numeric threshold"
echo "Expected: Track 2 | <col> <> NULL AND <col> \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "account balance >= 500"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 2c: attr_check — categorical tariff plan check"
echo "Expected: Track 2 | <col> <> NULL AND <col> \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "tariff plan is prepaid"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 3 — Classifier: subscription + placeholder window → Track 5
# Fix: example added to classifier prompt — X/N overrides rule 7b
# =============================================================================

echo "======================================================================"
echo "TEST 3a: subscribed + placeholder X days → Track 5 (NOT Track 2)"
echo "Expected: Track 5 (PARAMETERIZED)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "customer is subscribed to 1234 in the last X days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 3b: not subscribed + placeholder N days → Track 5 (NOT Track 2)"
echo "Expected: Track 5 (PARAMETERIZED)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "customer is not subscribed to product 500 in the last N days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 3c: subscribed + fixed 30 days → Track 2 (fixed number, not placeholder)"
echo "Expected: Track 2 (STATIC_FLAG)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "customer is subscribed to product 500 in the last 30 days"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 4 — Track 1 IN LIST + COUNT_ALL (filtered_count.template_range)
# Fix: filter_values wired through payload → template_range now reachable
# Expected parent_condition:
#   {date_col} >= CurrentTime-{N}DAYS
#   AND {filter_col} IN LIST ({filter_values})
#   AND COUNT_ALL({kpi_col}) ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "TEST 4a: product ID list + date window + COUNT_ALL"
echo "Expected: Track 1 | SUBSCRIPTIONS_EVENT_DATE >= CurrentTime-30DAYS AND SUBSCRIPTIONS_Product_Id IN LIST (123;125) AND COUNT_ALL(...) \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Select customers who purchased product 123 or product 125 in the last 30 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 4b: device type list + date window + COUNT_ALL"
echo "Expected: Track 1 | <date_col> >= CurrentTime-90DAYS AND <type_col> IN LIST (keypad;smartphone;iphone) AND COUNT_ALL(...) \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Find the number of customers using keypad, smartphone, or iphone devices in the last 90 days"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 5 — Track 1 IN LIST + SUM/AVG (filtered_count.template_range_agg)
# Fix: new template_range_agg added — IN LIST + date + variable aggregation
# Expected parent_condition:
#   {filter_col} IN LIST ({filter_values})
#   AND {date_col} >= CurrentTime-{N}DAYS
#   AND {agg}({kpi_col}) ${operator} ${value}
# =============================================================================

echo "======================================================================"
echo "TEST 5a: subscription state list + date window + SUM metric"
echo "Expected: Track 1 | SubscriptionState IN LIST (active;inactive) AND <date_col> >= CurrentTime-30DAYS AND SUM(SMS_Offnet_Revenue) \${operator} \${value}"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "total sms offnet revenue of active or inactive subscribers over the past 30 days"
}' | python3 -m json.tool
echo ""

# =============================================================================
# GROUP 6 — Track 2 campaign_present_fixed_days
# Fix: new sub_type + template for promo SENT (presence) in fixed N days
# Expected parent_condition:
#   L_PROMO_SENT_DATE >= CurrentTime-{N}DAYS
#   AND LC_ACTION_TYPE IN LIST (Promotion;PROMOTION;promotion)
#   AND L_ACTION_KEY ${operator} ${value}
#   AND COUNT_ALL(L_AGG_MSISDN) > 0
# =============================================================================

echo "======================================================================"
echo "TEST 6a: promo sent in last 4 days (fixed window)"
echo "Expected: Track 2 | L_PROMO_SENT_DATE >= CurrentTime-4DAYS AND LC_ACTION_TYPE IN LIST (Promotion;PROMOTION;promotion) AND L_ACTION_KEY \${operator} \${value} AND COUNT_ALL(L_AGG_MSISDN) > 0"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "count of customers who have been sent a promotion in the last 4 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "TEST 6b: promo sent in last 7 days (fixed window)"
echo "Expected: Track 2 | L_PROMO_SENT_DATE >= CurrentTime-7DAYS AND LC_ACTION_TYPE IN LIST (Promotion;PROMOTION;promotion) AND L_ACTION_KEY \${operator} \${value} AND COUNT_ALL(L_AGG_MSISDN) > 0"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "customers who received a promotion in the last 7 days"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "ALL SESSION FIX TESTS COMPLETE"
echo "======================================================================"
echo ""
echo "QUICK PASS/FAIL CHECKLIST:"
echo "  Group 1 (1a-1b) : track=3, geo_current, region_col path — no 500 error"
echo "  Group 2 (2a-2c) : track=2, attr_check — direct attribute comparison, not Track 3"
echo "  Group 3 (3a-3b) : track=5, PARAMETERIZED — X/N placeholder overrides rule 7b"
echo "  Group 3 (3c)    : track=2, STATIC_FLAG  — fixed number stays Track 2"
echo "  Group 4 (4a-4b) : track=1, IN LIST + COUNT_ALL → template_range"
echo "  Group 5 (5a)    : track=1, IN LIST + SUM/AVG  → template_range_agg"
echo "  Group 6 (6a-6b) : track=2, campaign_present_fixed_days — promo sent fixed N days"
