"""
vp_verify_mock_api.py
---------------------
Comprehensive mock of the VP verification API used by kpi_mapper.py.
Covers all KPI columns referenced across the 283 Omantel VP test cases.

IMPORTANT: The "kpi" field in the response is used DIRECTLY as kpi_col by
kpi_mapper.py, so it MUST be the actual column name (e.g. COMMON_Total_Revenue),
NOT a display name.

Run:
  uvicorn vp_verify_mock_api:app --host 0.0.0.0 --port 5678 --reload

Point the resolver at it:
  VP_VERIFY_URL=http://localhost:5678/webhook/VP_verify
"""

import logging
from fastapi import FastAPI
from pydantic import BaseModel

logger = logging.getLogger(__name__)

app = FastAPI(
    title="VP Verify Mock API",
    description="Local mock of the KPI verification service — all Omantel VP test cases.",
    version="2.0.0",
)


class VerifyRequest(BaseModel):
    conditions: list[str]
    check: bool = False


# =============================================================================
# MASTER KPI MAP
# =============================================================================
# Keys   : lowercase phrases the LLM agent would extract as the "kpi" field.
# Values : { kpi: <actual column name>, table_name: <YAML table>, datatype }
#
# The "kpi" value MUST exactly match column names in vp_template_engine.yaml
# and the original Omantel rules.
# =============================================================================

MOCK_KPI_MAP = {

    # ─── COMMON_Seg_Fct — Revenue ───────────────────────────────────────────
    "total revenue":                    {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "revenue":                          {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total revenue of customer":        {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "monthly revenue":                  {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "average monthly revenue":          {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "arpu":                             {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "average revenue":                  {"kpi": "COMMON_Total_Revenue",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "data revenue":                     {"kpi": "COMMON_Data_Revenue",               "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total data revenue":               {"kpi": "COMMON_Data_Revenue",               "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing call revenue":            {"kpi": "COMMON_OG_Call_Revenue",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "og call revenue":                  {"kpi": "COMMON_OG_Call_Revenue",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total outgoing call revenue":      {"kpi": "COMMON_OG_Call_Revenue",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "call revenue":                     {"kpi": "COMMON_OG_Call_Revenue",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "sms revenue":                      {"kpi": "COMMON_OG_Sms_Revenue",             "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing sms revenue":             {"kpi": "COMMON_OG_Sms_Revenue",             "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total sms revenue":                {"kpi": "COMMON_OG_Sms_Revenue",             "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total outgoing sms revenue":       {"kpi": "COMMON_OG_Sms_Revenue",             "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd call revenue":                 {"kpi": "COMMON_OG_IDD_Call_Revenue",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total idd call revenue":           {"kpi": "COMMON_OG_IDD_Call_Revenue",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd revenue":                      {"kpi": "Common_Total_IDD_Revenue",          "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total idd revenue":                {"kpi": "Common_Total_IDD_Revenue",          "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd sms revenue":                  {"kpi": "COMMON_OG_IDD_Sms_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming call revenue":             {"kpi": "COMMON_OG_Roam_Call_Revenue",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total roaming call revenue":       {"kpi": "COMMON_OG_Roam_Call_Revenue",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roam call revenue":                {"kpi": "COMMON_OG_Roam_Call_Revenue",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid voice revenue":            {"kpi": "COMMON_Prepay_Voice_Revenue",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid voice revenue":      {"kpi": "COMMON_Prepay_Voice_Revenue",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid data revenue":             {"kpi": "COMMON_Prepay_Data_Revenue",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid data revenue":       {"kpi": "COMMON_Prepay_Data_Revenue",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid sms revenue":              {"kpi": "COMMON_Prepay_Sms_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid sms revenue":        {"kpi": "COMMON_Prepay_Sms_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid vas revenue":              {"kpi": "COMMON_Prepay_Vas_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid vas revenue":        {"kpi": "COMMON_Prepay_Vas_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid product revenue":          {"kpi": "COMMON_Prepay_Product_Revenue",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid product revenue":    {"kpi": "COMMON_Prepay_Product_Revenue",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid gift revenue":             {"kpi": "COMMON_Prepay_Gft_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid gift revenue":       {"kpi": "COMMON_Prepay_Gft_Revenue",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid mt revenue":               {"kpi": "COMMON_Prepay_Mt_Revenue",          "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid mt revenue":         {"kpi": "COMMON_Prepay_Mt_Revenue",          "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "prepaid omip revenue":             {"kpi": "COMMON_Prepay_Omip_Revenue",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total prepaid omip revenue":       {"kpi": "COMMON_Prepay_Omip_Revenue",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},

    # ─── COMMON_Seg_Fct — PayG Revenue ──────────────────────────────────────
    "local payg data revenue":              {"kpi": "COMMON_Data_Local_PayG_Revenue",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local pay-as-you-go data revenue":     {"kpi": "COMMON_Data_Local_PayG_Revenue",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming payg data revenue":            {"kpi": "Common_Data_Roam_PayG_Revenue",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming pay-as-you-go data revenue":   {"kpi": "Common_Data_Roam_PayG_Revenue",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd payg call revenue":                {"kpi": "COMMON_OG_IDD_PayG_Call_Revenue",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd pay-as-you-go call revenue":       {"kpi": "COMMON_OG_IDD_PayG_Call_Revenue",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local payg call revenue":              {"kpi": "COMMON_OG_Local_PayG_Call_Revenue", "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local pay-as-you-go call revenue":     {"kpi": "COMMON_OG_Local_PayG_Call_Revenue", "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming payg call revenue":            {"kpi": "COMMON_OG_Roam_PayG_Call_Revenue",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming pay-as-you-go call revenue":   {"kpi": "COMMON_OG_Roam_PayG_Call_Revenue",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},

    # ─── COMMON_Seg_Fct — Volume / Usage ────────────────────────────────────
    "data volume":                      {"kpi": "COMMON_Data_Volume",                "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total data volume":                {"kpi": "COMMON_Data_Volume",                "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "data bundle volume":               {"kpi": "COMMON_Data_Bundle_Volume",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total data bundle volume":         {"kpi": "COMMON_Data_Bundle_Volume",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "free data volume":                 {"kpi": "COMMON_Data_Free_Volume",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "total free data volume":           {"kpi": "COMMON_Data_Free_Volume",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "pay-as-you-go data volume":        {"kpi": "COMMON_Data_PayG_Volume",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "payg data volume":                 {"kpi": "COMMON_Data_PayG_Volume",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming bundle data volume":       {"kpi": "COMMON_DATA_ROAM_BUNDLE_VOLUME",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming payg data volume":         {"kpi": "COMMON_DATA_ROAM_PAYG_VOLUME",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming pay-as-you-go data volume": {"kpi": "COMMON_DATA_ROAM_PAYG_VOLUME",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "daytime data volume":              {"kpi": "COMMON_DAY_DATA_VOLUME",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "night-time data volume":           {"kpi": "COMMON_NIGHT_DATA_VOLUME",          "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "night data volume":                {"kpi": "COMMON_NIGHT_DATA_VOLUME",          "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "off-peak data volume":             {"kpi": "COMMON_OFF_PEAK_DATA_VOLUME",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "peak data volume":                 {"kpi": "COMMON_PEAK_DATA_VOLUME",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "streaming volume":                 {"kpi": "COMMON_STREAMING_VOLUME",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "streaming data volume":            {"kpi": "COMMON_STREAMING_VOLUME",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "streaming session count":          {"kpi": "COMMON_STREAMING_SESSION_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "whatsapp volume":                  {"kpi": "COMMON_WHATSAPP_VOLUME",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "whatsapp data volume":             {"kpi": "COMMON_WHATSAPP_VOLUME",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "tiktok volume":                    {"kpi": "COMMON_TIKTOK_VOLUME",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "tiktok data volume":               {"kpi": "COMMON_TIKTOK_VOLUME",              "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "facebook volume":                  {"kpi": "COMMON_FACEBOOK_VOLUME",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "facebook data volume":             {"kpi": "COMMON_FACEBOOK_VOLUME",            "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "instagram volume":                 {"kpi": "COMMON_INSTAGRAM_VOLUME",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "instagram data volume":            {"kpi": "COMMON_INSTAGRAM_VOLUME",           "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "web browsing volume":              {"kpi": "COMMON_WEB_BROWSING_VOLUME",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "web browsing data volume":         {"kpi": "COMMON_WEB_BROWSING_VOLUME",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "email volume":                     {"kpi": "COMMON_EMAIL_VOLUME",               "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "email data volume":                {"kpi": "COMMON_EMAIL_VOLUME",               "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "email session count":              {"kpi": "COMMON_EMAIL_SESSION_COUNT",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "gaming volume":                    {"kpi": "COMMON_GAME_VOLUME",                "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "gaming data volume":               {"kpi": "COMMON_GAME_VOLUME",                "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "gaming session count":             {"kpi": "COMMON_GAME_SESSION_COUNT",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},

    # ─── COMMON_Seg_Fct — MOU / Call & SMS counts ───────────────────────────
    "outgoing call minutes":            {"kpi": "Common_OG_Call_MOU",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "og call mou":                      {"kpi": "Common_OG_Call_MOU",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "incoming call minutes":            {"kpi": "Common_IC_Call_MOU",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "ic call mou":                      {"kpi": "Common_IC_Call_MOU",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing call count":              {"kpi": "COMMON_OG_CALL_COUNT",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "incoming call count":              {"kpi": "COMMON_IC_CALL_COUNT",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing sms count":               {"kpi": "COMMON_OG_SMS_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "incoming sms count":               {"kpi": "COMMON_IC_SMS_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing bundle call count":       {"kpi": "COMMON_OG_BUNDLE_CALL_COUNT",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing bundle call minutes":     {"kpi": "COMMON_OG_BUNDLE_CALL_MOU",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "outgoing bundle sms count":        {"kpi": "COMMON_OG_BUNDLE_SMS_COUNT",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "free outgoing call count":         {"kpi": "COMMON_OG_FREE_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "free outgoing call minutes":       {"kpi": "COMMON_OG_FREE_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "free outgoing sms count":          {"kpi": "COMMON_OG_FREE_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "payg call count":                  {"kpi": "COMMON_OG_PAYG_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "pay-as-you-go call count":         {"kpi": "COMMON_OG_PAYG_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "payg call minutes":                {"kpi": "COMMON_OG_PAYG_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "pay-as-you-go call minutes":       {"kpi": "COMMON_OG_PAYG_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "payg sms count":                   {"kpi": "COMMON_OG_PAYG_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "pay-as-you-go sms count":          {"kpi": "COMMON_OG_PAYG_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "daytime voice usage":              {"kpi": "COMMON_OG_Day_voice_usage",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "night-time voice usage":           {"kpi": "COMMON_OG_NIGHT_VOICE_USAGE",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "night voice usage":                {"kpi": "COMMON_OG_NIGHT_VOICE_USAGE",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "peak voice usage":                 {"kpi": "COMMON_OG_PEAK_VOICE_USAGE",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},

    # ─── COMMON_Seg_Fct — IDD breakdown ─────────────────────────────────────
    "idd call count":                   {"kpi": "COMMON_OG_IDD_CALL_COUNT",         "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd ibp call count":               {"kpi": "COMMON_OG_IDD_IBP_CALL_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd ibp call minutes":             {"kpi": "COMMON_OG_IDD_IBP_CALL_MOU",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd bundle call count":            {"kpi": "COMMON_OG_IDD_Bundle_Call_Count",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd bundle call minutes":          {"kpi": "COMMON_OG_IDD_BUNDLE_CALL_MOU",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd bundle sms count":             {"kpi": "COMMON_OG_IDD_BUNDLE_SMS_COUNT",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd free sms count":               {"kpi": "COMMON_OG_IDD_FREE_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd gcc call count":               {"kpi": "COMMON_OG_IDD_GCC_CALL_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd gcc call minutes":             {"kpi": "COMMON_OG_IDD_GCC_CALL_MOU",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd payg call count":              {"kpi": "COMMON_OG_IDD_PAYG_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd pay-as-you-go call count":     {"kpi": "COMMON_OG_IDD_PAYG_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd payg call minutes":            {"kpi": "COMMON_OG_IDD_PAYG_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd pay-as-you-go call minutes":   {"kpi": "COMMON_OG_IDD_PAYG_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd payg sms count":               {"kpi": "COMMON_OG_IDD_PAYG_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd pay-as-you-go sms count":      {"kpi": "COMMON_OG_IDD_PAYG_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd vf call count":                {"kpi": "COMMON_OG_IDD_VF_CALL_COUNT",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "idd vf call minutes":              {"kpi": "COMMON_OG_IDD_VF_CALL_MOU",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},

    # ─── COMMON_Seg_Fct — Roaming breakdown ─────────────────────────────────
    "roaming bundle call count":          {"kpi": "COMMON_OG_ROAM_BUNDLE_CALL_COUNT",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming bundle call minutes":        {"kpi": "COMMON_OG_ROAM_BUNDLE_CALL_MOU",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming bundle sms count":           {"kpi": "COMMON_OG_ROAM_BUNDLE_SMS_COUNT",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming free sms count":             {"kpi": "COMMON_OG_ROAM_FREE_SMS_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming gcc call count":             {"kpi": "COMMON_OG_ROAM_GCC_CALL_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming gcc call minutes":           {"kpi": "COMMON_OG_ROAM_GCC_CALL_MOU",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming ibp call count":             {"kpi": "COMMON_OG_ROAM_IBP_CALL_COUNT",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming ibp call minutes":           {"kpi": "COMMON_OG_ROAM_IBP_CALL_MOU",       "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming payg call count":            {"kpi": "COMMON_OG_ROAM_PAYG_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming pay-as-you-go call count":   {"kpi": "COMMON_OG_ROAM_PAYG_CALL_COUNT",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming payg call minutes":          {"kpi": "COMMON_OG_ROAM_PAYG_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming pay-as-you-go call minutes": {"kpi": "COMMON_OG_ROAM_PAYG_CALL_MOU",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming payg sms count":             {"kpi": "COMMON_OG_Roam_PayG_Sms_Count",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming pay-as-you-go sms count":    {"kpi": "COMMON_OG_Roam_PayG_Sms_Count",     "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming vf call count":              {"kpi": "COMMON_OG_ROAM_VF_CALL_COUNT",      "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "roaming vf call minutes":            {"kpi": "COMMON_OG_ROAM_VF_CALL_MOU",        "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local bundle off-net call minutes":  {"kpi": "COMMON_OG_LOCAL_BUNDLE_OFFNET_CALL_MOU", "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local bundle offnet call minutes":   {"kpi": "COMMON_OG_LOCAL_BUNDLE_OFFNET_CALL_MOU", "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local bundle on-net call minutes":   {"kpi": "COMMON_OG_LOCAL_BUNDLE_ONNET_CALL_MOU",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local bundle onnet call minutes":    {"kpi": "COMMON_OG_LOCAL_BUNDLE_ONNET_CALL_MOU",  "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local off-net call minutes":         {"kpi": "COMMON_OG_Local_Offnet_Call_MOU",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local offnet call minutes":          {"kpi": "COMMON_OG_Local_Offnet_Call_MOU",   "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local on-net call minutes":          {"kpi": "COMMON_OG_LOCAL_ONNET_CALL_MOU",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},
    "local onnet call minutes":           {"kpi": "COMMON_OG_LOCAL_ONNET_CALL_MOU",    "table_name": "COMMON_Seg_Fct", "datatype": "numeric"},

    # ─── Recharge_Seg_Fact ──────────────────────────────────────────────────
    "recharge amount":              {"kpi": "RECHARGE_Denomination",  "table_name": "Recharge_Seg_Fact", "datatype": "numeric"},
    "recharge denomination":        {"kpi": "RECHARGE_Denomination",  "table_name": "Recharge_Seg_Fact", "datatype": "numeric"},
    "recharge revenue":             {"kpi": "RECHARGE_Denomination",  "table_name": "Recharge_Seg_Fact", "datatype": "numeric"},
    "total recharge amount":        {"kpi": "RECHARGE_Denomination",  "table_name": "Recharge_Seg_Fact", "datatype": "numeric"},
    "average recharge amount":      {"kpi": "RECHARGE_Denomination",  "table_name": "Recharge_Seg_Fact", "datatype": "numeric"},
    "average recharge":             {"kpi": "RECHARGE_Denomination",  "table_name": "Recharge_Seg_Fact", "datatype": "numeric"},

    # ─── BILL_PAYMENT ───────────────────────────────────────────────────────
    "bill payment amount":          {"kpi": "BILL_PAYMENT_SUMMARY_AMOUNT", "table_name": "BILL_PAYMENT", "datatype": "numeric"},
    "bill payment summary amount":  {"kpi": "BILL_PAYMENT_SUMMARY_AMOUNT", "table_name": "BILL_PAYMENT", "datatype": "numeric"},
    "total bill payment amount":    {"kpi": "BILL_PAYMENT_SUMMARY_AMOUNT", "table_name": "BILL_PAYMENT", "datatype": "numeric"},

    # ─── BILL_EVENT ─────────────────────────────────────────────────────────
    "bill payment records":         {"kpi": "BILL_IS_PAID_MSISDN_PRO",      "table_name": "BILL_EVENT", "datatype": "numeric"},
    "bill paid count":              {"kpi": "BILL_IS_PAID_MSISDN_PRO",      "table_name": "BILL_EVENT", "datatype": "numeric"},
    "bill paid":                    {"kpi": "BILL_IS_PAID_MSISDN_PRO",      "table_name": "BILL_EVENT", "datatype": "numeric"},
    "paid bill event":              {"kpi": "BILL_IS_PAID_MSISDN_PRO",      "table_name": "BILL_EVENT", "datatype": "numeric"},
    "bill payment count":           {"kpi": "BILL_IS_PAID_MSISDN_PRO",      "table_name": "BILL_EVENT", "datatype": "numeric"},
    "bill payments":                {"kpi": "BILL_IS_PAID_MSISDN_PROFILE",  "table_name": "BILL_EVENT", "datatype": "numeric"},
    "total bill payments":          {"kpi": "BILL_IS_PAID_MSISDN_PROFILE",  "table_name": "BILL_EVENT", "datatype": "numeric"},

    # ─── Subscriptions ──────────────────────────────────────────────────────
    "product":                      {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "product id":                   {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "product subscription":         {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "subscription":                 {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "subscribed to product":        {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "not subscribed to product":    {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "active product subscriptions": {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},
    "product subscriptions":        {"kpi": "SUBSCRIPTIONS_Product_Id",  "table_name": "Subscriptions", "datatype": "categorical"},

    # ─── VAS_SUBSCRIPTIONS ──────────────────────────────────────────────────
    "vas subscription":             {"kpi": "VAS_CNT_OF_PID",  "table_name": "VAS_SUBSCRIPTIONS", "datatype": "numeric"},
    "vas product":                  {"kpi": "VAS_CNT_OF_PID",  "table_name": "VAS_SUBSCRIPTIONS", "datatype": "numeric"},

    # ─── LIFECYCLE_BONUS ────────────────────────────────────────────────────
    "bonus action key":             {"kpi": "L_ACTION_KEY",    "table_name": "LIFECYCLE_BONUS", "datatype": "categorical"},
    "bonus for action key":         {"kpi": "L_ACTION_KEY",    "table_name": "LIFECYCLE_BONUS", "datatype": "categorical"},
    "bonus sent":                   {"kpi": "L_ACTION_KEY",    "table_name": "LIFECYCLE_BONUS", "datatype": "categorical"},

    # ─── LIFECYCLE_PROMO ────────────────────────────────────────────────────
    "action key":                   {"kpi": "L_ACTION_KEY",     "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "promotion action key":         {"kpi": "L_ACTION_KEY",     "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "promo action key":             {"kpi": "L_ACTION_KEY",     "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "promotion for action key":     {"kpi": "L_ACTION_KEY",     "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "promo sent":                   {"kpi": "L_ACTION_KEY",     "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "promotion":                    {"kpi": "L_ACTION_KEY",     "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "segment name":                 {"kpi": "LC_SEGMENT_NAME",  "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "promotion segment":            {"kpi": "LC_SEGMENT_NAME",  "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},
    "bonus segment":                {"kpi": "LC_SEGMENT_NAME",  "table_name": "LIFECYCLE_PROMO", "datatype": "categorical"},

    # ─── DPI_App_Usage ──────────────────────────────────────────────────────
    "streaming data usage":         {"kpi": "dpi_app_usage_usage", "table_name": "DPI_App_Usage", "datatype": "numeric"},
    "streaming usage":              {"kpi": "dpi_app_usage_usage", "table_name": "DPI_App_Usage", "datatype": "numeric"},
    "app data usage":               {"kpi": "dpi_app_usage_usage", "table_name": "DPI_App_Usage", "datatype": "numeric"},
    "app data":                     {"kpi": "dpi_app_usage_usage", "table_name": "DPI_App_Usage", "datatype": "numeric"},
    "youtube usage":                {"kpi": "dpi_app_usage_usage", "table_name": "DPI_App_Usage", "datatype": "numeric"},
    "social media usage":           {"kpi": "dpi_app_usage_usage", "table_name": "DPI_App_Usage", "datatype": "numeric"},

    # ─── DPI_Geo_Location ───────────────────────────────────────────────────
    "geo location":                 {"kpi": "dpi_geo_location_region", "table_name": "DPI_Geo_Location", "datatype": "categorical"},
    "region":                       {"kpi": "dpi_geo_location_region", "table_name": "DPI_Geo_Location", "datatype": "categorical"},
    "geographic location":          {"kpi": "dpi_geo_location_region", "table_name": "DPI_Geo_Location", "datatype": "categorical"},

    # ─── HBB ────────────────────────────────────────────────────────────────
    "hbb fixed line":               {"kpi": "HBB_Fixed_Line_NRP",   "table_name": "HBB", "datatype": "numeric"},
    "fixed broadband line":         {"kpi": "HBB_Fixed_Line_NRP",   "table_name": "HBB", "datatype": "numeric"},
    "hbb fixed line nrp":           {"kpi": "HBB_Fixed_Line_NRP",   "table_name": "HBB", "datatype": "numeric"},
    "hbb fixed line count":         {"kpi": "HBB_Fixed_Line_NRP",   "table_name": "HBB", "datatype": "numeric"},
    "hbb add-on component":         {"kpi": "HBB_AddOn_Comp_Id",    "table_name": "HBB", "datatype": "categorical"},
    "hbb addon component":          {"kpi": "HBB_AddOn_Comp_Id",    "table_name": "HBB", "datatype": "categorical"},
    "hbb addon":                    {"kpi": "HBB_AddOn_Comp_Id",    "table_name": "HBB", "datatype": "categorical"},
    "hbb add-on deactivation date": {"kpi": "HBB_AddOn_Inact_Date", "table_name": "HBB", "datatype": "date"},
    "hbb addon deactivation date":  {"kpi": "HBB_AddOn_Inact_Date", "table_name": "HBB", "datatype": "date"},
    "hbb id":                       {"kpi": "HBBID",                 "table_name": "HBB", "datatype": "categorical"},
    "hbbid":                        {"kpi": "HBBID",                 "table_name": "HBB", "datatype": "categorical"},

    # ─── AUDIENCE_SEGMENT ───────────────────────────────────────────────────
    "segment id":                   {"kpi": "AS_SEGMENT_ID",        "table_name": "AUDIENCE_SEGMENT", "datatype": "categorical"},
    "audience segment id":          {"kpi": "AS_SEGMENT_ID",        "table_name": "AUDIENCE_SEGMENT", "datatype": "categorical"},
    "segment":                      {"kpi": "AS_SEGMENT_ID",        "table_name": "AUDIENCE_SEGMENT", "datatype": "categorical"},
    "execution counter":            {"kpi": "AS_EXECUTION_COUNTER", "table_name": "AUDIENCE_SEGMENT", "datatype": "numeric"},
    "segment execution counter":    {"kpi": "AS_EXECUTION_COUNTER", "table_name": "AUDIENCE_SEGMENT", "datatype": "numeric"},

    # ─── REDEEMED_POINTS ────────────────────────────────────────────────────
    "redeemed points":              {"kpi": "REDEEMED_REDEEM_POINTS", "table_name": "REDEEMED_POINTS", "datatype": "numeric"},
    "redeemed loyalty points":      {"kpi": "REDEEMED_REDEEM_POINTS", "table_name": "REDEEMED_POINTS", "datatype": "numeric"},
    "loyalty points redeemed":      {"kpi": "REDEEMED_REDEEM_POINTS", "table_name": "REDEEMED_POINTS", "datatype": "numeric"},

    # ─── 360_PROFILE ────────────────────────────────────────────────────────
    "nbo product id":               {"kpi": "CUST_360_NBO_PRODUCTID", "table_name": "360_PROFILE", "datatype": "categorical"},
    "next best offer product id":   {"kpi": "CUST_360_NBO_PRODUCTID", "table_name": "360_PROFILE", "datatype": "categorical"},
    "next best offer":              {"kpi": "CUST_360_NBO_PRODUCTID", "table_name": "360_PROFILE", "datatype": "categorical"},

    # ─── CAMPAIGN_WHITELIST ─────────────────────────────────────────────────
    "whitelist":                    {"kpi": "WHITELIST_MSISDN_NP",  "table_name": "CAMPAIGN_WHITELIST", "datatype": "categorical"},
    "whitelist msisdn":             {"kpi": "WHITELIST_MSISDN_NP",  "table_name": "CAMPAIGN_WHITELIST", "datatype": "categorical"},

    # ─── Instant_cdr_group ──────────────────────────────────────────────────
    "recharge amount since promo":  {"kpi": "I_RECHARGE_AMOUNT",  "table_name": "Instant_cdr_group", "datatype": "numeric"},
    "instant recharge amount":      {"kpi": "I_RECHARGE_AMOUNT",  "table_name": "Instant_cdr_group", "datatype": "numeric"},
    "post-promo recharge":          {"kpi": "I_RECHARGE_AMOUNT",  "table_name": "Instant_cdr_group", "datatype": "numeric"},

    # ─── NATIVE ─────────────────────────────────────────────────────────────
    "native action key":            {"kpi": "NATIVE_ACTION_KEY",  "table_name": "NATIVE", "datatype": "categorical"},

    # ─── PDC ────────────────────────────────────────────────────────────────
    "pdc status":                   {"kpi": "PDC_STATUS",     "table_name": "PDC", "datatype": "categorical"},
    "pdc msisdn":                   {"kpi": "PDC_MSISDN_NP",  "table_name": "PDC", "datatype": "categorical"},

    # ─── UTG_SEGMENT ────────────────────────────────────────────────────────
    "segment type":                 {"kpi": "UTG_Seg_Type",   "table_name": "UTG_SEGMENT", "datatype": "categorical"},
    "utg segment type":             {"kpi": "UTG_Seg_Type",   "table_name": "UTG_SEGMENT", "datatype": "categorical"},
    "gcg segment":                  {"kpi": "UTG_Seg_Type",   "table_name": "UTG_SEGMENT", "datatype": "categorical"},

    # ─── GEX ────────────────────────────────────────────────────────────────
    "gex revenue":                  {"kpi": "Gex_Revenue",    "table_name": "GEX", "datatype": "numeric"},
    "total gex revenue":            {"kpi": "Gex_Revenue",    "table_name": "GEX", "datatype": "numeric"},

    # ─── LMS_EXPIRY ─────────────────────────────────────────────────────────
    "loyalty reward points":        {"kpi": "LMS_EXPIRY_REWARD_POINTS",  "table_name": "LMS_EXPIRY", "datatype": "numeric"},
    "reward points expiring":       {"kpi": "LMS_EXPIRY_REWARD_POINTS",  "table_name": "LMS_EXPIRY", "datatype": "numeric"},
    "loyalty status points":        {"kpi": "LMS_EXPIRY_STATUS_POINTS",  "table_name": "LMS_EXPIRY", "datatype": "numeric"},
    "status points expiring":       {"kpi": "LMS_EXPIRY_STATUS_POINTS",  "table_name": "LMS_EXPIRY", "datatype": "numeric"},

    # ─── LMS_POINTS ─────────────────────────────────────────────────────────
    "makasb points":                {"kpi": "MAKASB_POINTS",  "table_name": "LMS_POINTS", "datatype": "numeric"},
    "makasb loyalty points":        {"kpi": "MAKASB_POINTS",  "table_name": "LMS_POINTS", "datatype": "numeric"},

    # ─── PREPAID_MIGRATION ──────────────────────────────────────────────────
    "prepaid migration plan":       {"kpi": "Prepaid_Migration_Next_Best_Plan_Internal", "table_name": "PREPAID_MIGRATION", "datatype": "categorical"},
    "next best plan":               {"kpi": "Prepaid_Migration_Next_Best_Plan_Internal", "table_name": "PREPAID_MIGRATION", "datatype": "categorical"},

    # ─── RPT_BONUS ──────────────────────────────────────────────────────────
    "bonus provision":              {"kpi": "RPT_BONUS_PROVISION_FULFILMENT_MESSAGE", "table_name": "RPT_BONUS", "datatype": "categorical"},
}


# =============================================================================
# FUZZY KEYWORD FALLBACK
# =============================================================================
# Ordered most-specific first. Checked only when exact match fails.

FUZZY_KEYWORDS = [
    # Revenue — specific first
    ("prepaid voice revenue",       "COMMON_Prepay_Voice_Revenue",       "COMMON_Seg_Fct"),
    ("prepaid data revenue",        "COMMON_Prepay_Data_Revenue",        "COMMON_Seg_Fct"),
    ("prepaid sms revenue",         "COMMON_Prepay_Sms_Revenue",         "COMMON_Seg_Fct"),
    ("prepaid vas revenue",         "COMMON_Prepay_Vas_Revenue",         "COMMON_Seg_Fct"),
    ("prepaid product revenue",     "COMMON_Prepay_Product_Revenue",     "COMMON_Seg_Fct"),
    ("prepaid gift revenue",        "COMMON_Prepay_Gft_Revenue",         "COMMON_Seg_Fct"),
    ("prepaid mt revenue",          "COMMON_Prepay_Mt_Revenue",          "COMMON_Seg_Fct"),
    ("prepaid omip revenue",        "COMMON_Prepay_Omip_Revenue",        "COMMON_Seg_Fct"),
    ("idd call revenue",            "COMMON_OG_IDD_Call_Revenue",        "COMMON_Seg_Fct"),
    ("idd sms revenue",             "COMMON_OG_IDD_Sms_Revenue",         "COMMON_Seg_Fct"),
    ("roaming call revenue",        "COMMON_OG_Roam_Call_Revenue",       "COMMON_Seg_Fct"),
    ("roam call revenue",           "COMMON_OG_Roam_Call_Revenue",       "COMMON_Seg_Fct"),
    ("outgoing call revenue",       "COMMON_OG_Call_Revenue",            "COMMON_Seg_Fct"),
    ("og call revenue",             "COMMON_OG_Call_Revenue",            "COMMON_Seg_Fct"),
    ("call revenue",                "COMMON_OG_Call_Revenue",            "COMMON_Seg_Fct"),
    ("sms revenue",                 "COMMON_OG_Sms_Revenue",             "COMMON_Seg_Fct"),
    ("data revenue",                "COMMON_Data_Revenue",               "COMMON_Seg_Fct"),
    # Volume
    ("data bundle volume",          "COMMON_Data_Bundle_Volume",         "COMMON_Seg_Fct"),
    ("free data volume",            "COMMON_Data_Free_Volume",           "COMMON_Seg_Fct"),
    ("streaming volume",            "COMMON_STREAMING_VOLUME",           "COMMON_Seg_Fct"),
    ("streaming data",              "COMMON_STREAMING_VOLUME",           "COMMON_Seg_Fct"),
    ("whatsapp volume",             "COMMON_WHATSAPP_VOLUME",            "COMMON_Seg_Fct"),
    ("tiktok volume",               "COMMON_TIKTOK_VOLUME",              "COMMON_Seg_Fct"),
    ("facebook volume",             "COMMON_FACEBOOK_VOLUME",            "COMMON_Seg_Fct"),
    ("instagram volume",            "COMMON_INSTAGRAM_VOLUME",           "COMMON_Seg_Fct"),
    ("web browsing",                "COMMON_WEB_BROWSING_VOLUME",        "COMMON_Seg_Fct"),
    ("email volume",                "COMMON_EMAIL_VOLUME",               "COMMON_Seg_Fct"),
    ("data volume",                 "COMMON_Data_Volume",                "COMMON_Seg_Fct"),
    # MOU / counts
    ("outgoing call minutes",       "Common_OG_Call_MOU",                "COMMON_Seg_Fct"),
    ("incoming call minutes",       "Common_IC_Call_MOU",                "COMMON_Seg_Fct"),
    ("call minutes",                "Common_OG_Call_MOU",                "COMMON_Seg_Fct"),
    ("call mou",                    "Common_OG_Call_MOU",                "COMMON_Seg_Fct"),
    ("sms count",                   "COMMON_OG_SMS_COUNT",               "COMMON_Seg_Fct"),
    # Recharge
    ("recharge amount",             "RECHARGE_Denomination",             "Recharge_Seg_Fact"),
    ("recharge denomination",       "RECHARGE_Denomination",             "Recharge_Seg_Fact"),
    ("recharge",                    "RECHARGE_Denomination",             "Recharge_Seg_Fact"),
    # Bill
    ("bill payment amount",         "BILL_PAYMENT_SUMMARY_AMOUNT",      "BILL_PAYMENT"),
    ("bill paid",                   "BILL_IS_PAID_MSISDN_PRO",          "BILL_EVENT"),
    ("bill payment",                "BILL_PAYMENT_SUMMARY_AMOUNT",      "BILL_PAYMENT"),
    # Subscription
    ("subscri",                     "SUBSCRIPTIONS_Product_Id",          "Subscriptions"),
    ("product",                     "SUBSCRIPTIONS_Product_Id",          "Subscriptions"),
    # Lifecycle
    ("bonus",                       "L_ACTION_KEY",                      "LIFECYCLE_BONUS"),
    ("promo",                       "L_ACTION_KEY",                      "LIFECYCLE_PROMO"),
    ("action key",                  "L_ACTION_KEY",                      "LIFECYCLE_PROMO"),
    # HBB
    ("hbb",                         "HBB_Fixed_Line_NRP",               "HBB"),
    ("fixed line",                  "HBB_Fixed_Line_NRP",               "HBB"),
    # DPI
    ("streaming",                   "dpi_app_usage_usage",              "DPI_App_Usage"),
    ("youtube",                     "dpi_app_usage_usage",              "DPI_App_Usage"),
    ("geo",                         "dpi_geo_location_region",          "DPI_Geo_Location"),
    # Points
    ("redeem",                      "REDEEMED_REDEEM_POINTS",           "REDEEMED_POINTS"),
    ("loyalty",                     "LMS_EXPIRY_REWARD_POINTS",         "LMS_EXPIRY"),
    ("makasb",                      "MAKASB_POINTS",                    "LMS_POINTS"),
    # Fallback
    ("revenue",                     "COMMON_Total_Revenue",             "COMMON_Seg_Fct"),
]


def _normalize(value: str) -> str:
    return value.strip().lower()


def _resolve_kpi(condition_text: str):
    """
    Resolve a natural-language KPI phrase to { kpi, table_name, datatype }.
    1. Exact match on normalized text.
    2. Fuzzy substring match (most-specific keyword first).
    3. None if unresolvable — caller will mark as unmatched.
    """
    normalized = _normalize(condition_text)

    # 1. Exact
    if normalized in MOCK_KPI_MAP:
        return MOCK_KPI_MAP[normalized]

    # 2. Fuzzy
    for keyword, kpi_col, table_name in FUZZY_KEYWORDS:
        if keyword in normalized:
            return {"kpi": kpi_col, "table_name": table_name, "datatype": "numeric"}

    return None


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": "2.0.0",
        "exact_entries": len(MOCK_KPI_MAP),
        "fuzzy_keywords": len(FUZZY_KEYWORDS),
    }


@app.post("/webhook/VP_verify")
def verify(request: VerifyRequest):
    matches = []
    unmatched = []

    for condition in request.conditions:
        result = _resolve_kpi(condition)
        if result:
            matches.append({
                "condition": condition,
                "kpi": result["kpi"],
                "table_name": result["table_name"],
                "datatype": result["datatype"],
            })
        else:
            unmatched.append(condition)
            logger.warning("UNMATCHED KPI: '%s'", condition)

    return {
        "output": {
            "matches": matches,
            "unmatched": unmatched,
            "mismatch_percentage": round(
                len(unmatched) / max(len(request.conditions), 1) * 100, 1
            ),
        }
    }


@app.get("/catalog")
def catalog():
    """Debug endpoint: list all known KPI mappings."""
    return {
        "exact_matches": {
            k: {"kpi": v["kpi"], "table_name": v["table_name"]}
            for k, v in sorted(MOCK_KPI_MAP.items())
        },
        "fuzzy_keywords": [
            {"keyword": kw, "kpi": col, "table_name": tbl}
            for kw, col, tbl in FUZZY_KEYWORDS
        ],
    }