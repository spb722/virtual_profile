#!/bin/bash
# =============================================================================
# FIX 1 END-TO-END TESTS (Resolver API — port 8001)
# =============================================================================
# These test the FULL pipeline: NL → classifier → agent → KPI mapper → template
# The key thing being validated: does the LLM agent correctly extract groupby_entity?
#
# NOTE: These require both services running AND the VP verification API reachable.
# If KPI mapping fails for Airtel KPIs, the test will 422 — that's a separate issue
# (KPI catalog coverage), not a groupby issue.
# =============================================================================

BASE="http://localhost:8001/resolve"

echo "======================================================================"
echo "E2E TEST 1: Simple groupby — count per subscriber"
echo "Target VP: CATEGORY_COUNT_CHECK"
echo "Expected agent output: groupby_entity='subscriber'"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Count of action keys grouped by subscriber"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 2: Groupby product — refill type count"
echo "Target VP: EXPIRY_DAYS"
echo "Expected agent output: groupby_entity='product'"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Refill type count grouped by product, compared against date"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 3: Multi-column groupby — product + description"
echo "Target VP: Post_Expiry_ESB_Description_Count_1"
echo "Expected agent output: groupby_entity='product_description'"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Refill ID and ESB description not null, refill type count grouped by product and description"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 4: Today + groupby — SSO check absent"
echo "Target VP: RECHARGE_CHECK_SSO"
echo "Expected agent output: groupby_entity='subscriber'"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Action key not triggered today for SSO subscriber, counted per subscriber"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 5: Today + groupby — activation present"
echo "Target VP: RECHGARGE_ACTIVATION_CHECK"
echo "Expected agent output: groupby_entity='subscriber'"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Action key activated today with SSO subscriber present, at least one per subscriber"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 6: Track 5 + multi-col groupby"
echo "Target VP: L_PROMO_SENT"
echo "Expected agent output: groupby_entity='action_date'"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Promo sent for action key in last X days, grouped by action key and date"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 7: NO groupby — should produce null groupby_entity"
echo "Target VP: ACTION_COUNT (works today, no groupby needed)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Action key count is zero"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "E2E TEST 8: NO groupby — existing working VP"
echo "Target VP: MO1 (simple flag check)"
echo "======================================================================"
curl -s -X POST $BASE -H "Content-Type: application/json" -d '{
  "condition": "Service type is not null"
}' | python3 -m json.tool
echo ""

echo "======================================================================"
echo "ALL E2E TESTS COMPLETE"
echo "======================================================================"
echo ""
echo "WHAT TO CHECK:"
echo "  - Tests 1-6: parent_condition should contain __groupby_ suffix"
echo "  - Tests 7-8: parent_condition should NOT contain __groupby_"
echo "  - If any test returns 422 with 'No KPI/profile found': that's a KPI"
echo "    mapper gap (VP verification API doesn't know the Airtel KPI), not"
echo "    a groupby issue. File that separately."