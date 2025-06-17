# etl_dependency_graph_streamlit_app.py
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st
import streamlit.components.v1 as components
import tempfile
import json
import os


def main():
    st.set_page_config(layout="wide")
    st.title("ETL Dependency Graph Viewer")

    # ------------------------
    # Real Dataset
    # ------------------------
    data = [
    {"job": "12.Load_FACT_Ledger", "source": "ODS_P.DSSTG.STG_FACT_LEDGER_SOURCE_INFO", "target": "EDW_P.FIN.FACT_LEDGER"},
    {"job": "12.Load_FACT_Ledger", "source": "ODS_P.FIN_PSFT.PS_LEDGER", "target": "ODS_P.DSSTG.STG_FACT_LEDGER_SOURCE_INFO"},
    {"job": "12.Load_FACT_Ledger", "source": "EDW_P.FIN.FACT_LEDGER", "target": "ODS_P.DSSTG.STG_FACT_LEDGER_TARGET_INFO"},
    {"job": "12.Load_FACT_Ledger", "source": " ODS_P.DSSTG.STG_FACT_LEDGER_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_FACT_LEDGER_TARGET_INFO"},
    {"job": "13.Load_FACT_Ledger_Budget", "source": "ODS_P.FIN_PSFT.PS_LEDGER_BUDG", "target": "EDW_P.FIN.FACT_LEDGER_BUDGET"},
    {"job": "14.Load_FACT_Journal_Pending", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO", "target": "EDW_P.FIN.FACT_JOURNAL"},
    {"job": "14.Load_FACT_Journal_Pending", "source": "EDW_P.FIN.FACT_JOURNAL", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_TARGET_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": " ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_TARGET_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": " ODS_P.FIN_PSFT.PS_JRNL_HEADER", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": " ODS_P.FIN_PSFT.PS_JRNL_LN", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": " ODS_P.FIN_PSFT.PS_JRNL_HEADER", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO"},
    {"job": "14.Load_FACT_Journal_Pending", "source": " ODS_P.FIN_PSFT.PS_JRNL_LN", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO"},
    {"job": "2.Load_DIM_Business_Unit", "source": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT", "target": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT"},
    {"job": "2.Load_DIM_Business_Unit", "source": "ODS_P.FIN_PSFT.PS_BUS_UNIT_TBL_FS", "target": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT"},
    {"job": "3.Load_DIM_Operating_Unit", "source": "EDW_P.FIN.DIM_PS_OPERATING_UNIT", "target": "STG_P.FIN.DIM_PS_OPERATING_UNIT"},
    {"job": "3.Load_DIM_Operating_Unit", "source": "ODS_P.FIN_PSFT.PS_OPER_UNIT_TBL", "target": "STG_P.FIN.DIM_PS_OPERATING_UNIT"},
    {"job": "4.Load_DIM_Department", "source": "EDW_P.FIN.DIM_PS_DEPT", "target": "EDW_P.FIN.DIM_PS_DEPT"},
    {"job": "4.Load_DIM_Department", "source": " ODS_P.FIN_PSFT.PS_DEPT_TBL", "target": "EDW_P.FIN.DIM_PS_DEPT"},
    {"job": "5.Load_DIM_GL_Account", "source": "EDW_P.FIN.DIM_PS_GL_ACCOUNT", "target": "EDW_P.FIN.DIM_PS_GL_ACCOUNT"},
    {"job": "5.Load_DIM_GL_Account", "source": " ODS_P.FIN_PSFT.PS_GL_ACCOUNT_TBL", "target": "EDW_P.FIN.DIM_PS_GL_ACCOUNT"},
    {"job": "5.Load_DIM_GL_Account", "source": " ODS_P_BUSINESS_SUPPORT.FIN_SUPP.PS_GL_ACCOUNT_SUPP", "target": "EDW_P.FIN.DIM_PS_GL_ACCOUNT"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_ACCOUNT_HIER", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_DEPARTMENT_HIER", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_OPERATING_UNIT_HIER", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREELEAF_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_PS_GL_ACCOUNT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_FINAL_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_PS_DEPT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "EDW_P.FIN.DIM_PS_OPERATING_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "6.Load_DIM_GL_Account_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_ACCOUNT_HIER", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_DEPARTMENT_HIER", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_OPERATING_UNIT_HIER", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREELEAF_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_PS_GL_ACCOUNT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_FINAL_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_PS_DEPT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "7.Load_DIM_Department_Hierarchy", "source": "EDW_P.FIN.DIM_PS_OPERATING_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_ACCOUNT_HIER", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_DEPARTMENT_HIER", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_OPERATING_UNIT_HIER", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREELEAF_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_PS_GL_ACCOUNT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_FINAL_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_PS_DEPT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "8.Load_DIM_Business_Hierarchy", "source": "EDW_P.FIN.DIM_PS_OPERATING_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_ACCOUNT_HIER", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_ACCOUNT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_DEPARTMENT_HIER", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_DEPARTMENT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "EDW_P.FIN.DIM_BUSINESS_UNIT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_OPERATING_UNIT_HIER", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_FINAL_INFO", "target": "STG_P.FIN.DIM_OPERATING_UNIT_HIER"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREENODE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "ODS_P.FIN_PSFT.PSTREELEAF", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.FIN_PSFT.PSTREENODE", "target": "ODS_P.DSSTG.STG_PSTREELEAF_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREENODE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_PSTREELEAF_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_PS_GL_ACCOUNT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": " ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "ODS_P.DSSTG.STG_HIER_TREE_INFO", "target": "ODS_P.DSSTG.STG_HIER_FINAL_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_PS_DEPT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_PS_BUSINESS_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "9.Load_DIM_Operating_Hierarchy", "source": "EDW_P.FIN.DIM_PS_OPERATING_UNIT", "target": "ODS_P.DSSTG.STG_HIER_TREE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_INFO", "target": "EDW_P.FIN.FACT_VOUCHER"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VCHR_ACCTG_LINE", "target": "ODS_P.DSSTG.STG_PS_VCHR_ACCTG_LN_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VOUCHER", "target": "ODS_P.DSSTG.STG_PS_VCHR_ACCTG_LN_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_DISTRIB_LINE", "target": "ODS_P.DSSTG.STG_VOUCHER_FACT_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VCHR_ACCTG_LINE", "target": "ODS_P.DSSTG.STG_VOUCHER_FACT_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VOUCHER", "target": "ODS_P.DSSTG.STG_VOUCHER_FACT_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VOUCHER_LINE", "target": "ODS_P.DSSTG.STG_VOUCHER_FACT_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VENDOR", "target": "ODS_P.DSSTG.STG_PS_VENDOR_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.FIN_PSFT.PS_VENDOR", "target": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.DSSTG.STG_PS_VCHR_ACCTG_LN_INFO", "target": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.DSSTG.STG_VOUCHER_FACT_INFO", "target": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "EDW_P.FIN.FACT_VOUCHER", "target": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0100.Load_FACT_Voucher", "source": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_PS_VOUCHER_FACT_TARGET_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO", "target": "EDW_P.FIN.FACT_JOURNAL"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "EDW_P.FIN.FACT_JOURNAL", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_TARGET_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.FIN_PSFT.PS_JRNL_HEADER", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.FIN_PSFT.PS_JRNL_LN", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.FIN_PSFT.PS_JRNL_LN", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0150.Load_FACT_Journal", "source": "ODS_P.FIN_PSFT.PS_JRNL_HEADER", "target": "ODS_P.DSSTG.STG_PS_JOURNAL_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO", "target": "EDW_P.FIN.FACT_PAYMENT"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.FIN_PSFT.PS_PAYMENT_TBL", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.FIN_PSFT.PS_PYMNT_VCHR_XREF", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.FIN_PSFT.PS_VOUCHER", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_FAIL_INFO", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.FIN_PSFT.PS_PAYMENT_TBL", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.FIN_PSFT.PS_PYMNT_VCHR_XREF", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.FIN_PSFT.PS_VOUCHER", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "EDW_P.FIN.FACT_PAYMENT", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_TARGET_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0200.Load_FACT_Payment", "source": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_SOURCE_INFO", "target": "ODS_P.DSSTG.STG_PS_PAYMENT_FACT_TARGET_INFO"},
    {"job": "ODS_EDW.FIN_PSFT_REAL.0240.Load_MCV_Perform_Tables_FIN", "source": "EDW_P.OBJPERF.ETL_VW_CV_JRNL_VCHR_PYMNT_ALL_NCLH", "target": "EDW_P.OBJPERF.MCV_VW_CV_JRNL_VCHR_PYMNT_ALL_NCLH"}
]

    df = pd.DataFrame(data)
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    df = df.dropna(subset=["source", "target", "job"])
    df = df[(df['source'] != '') & (df['target'] != '') & (df['job'] != '')]
    edges = [(row["source"], row["target"], row["job"]) for _, row in df.iterrows()]
    nodes = sorted(set(df["source"]).union(set(df["target"])))

    @st.cache_data(show_spinner=False)
    def get_valid_nodes():
        return [n for n in nodes if isinstance(n, str) and n.strip() != '']

    def render_sidebar():
        st.sidebar.header("Explore Dependencies")
        selected_node = st.sidebar.selectbox("Select a table/job/report:", get_valid_nodes(), key="node_select")
        direction = st.sidebar.radio("Dependency Direction", ["Downstream (Impact)", "Upstream (Lineage)"], key="direction_radio")
        return selected_node.strip(), direction

    selected_node, direction = render_sidebar()

    g = nx.DiGraph()
    for src, tgt, job in edges:
        g.add_edge(src, tgt, label=job)

    def get_subgraph(graph, start_node, direction="downstream"):
        visited = set()
        to_visit = [start_node.strip()]
        sub_edges = []
        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)
            try:
                neighbors = graph.successors(current) if direction == "downstream" else graph.predecessors(current)
                for n in neighbors:
                    sub_edges.append((current, n)) if direction == "downstream" else sub_edges.append((n, current))
                    to_visit.append(n)
            except nx.NetworkXError:
                continue
        return sub_edges

    selected_raw_edges = get_subgraph(g, selected_node, direction="downstream" if direction.startswith("Down") else "upstream")

    filtered_nodes = set()
    for src, tgt in selected_raw_edges:
        filtered_nodes.add(src)
        filtered_nodes.add(tgt)

    selected_edges = [
        (src, tgt, job)
        for (src, tgt, job) in edges
        if (src, tgt) in selected_raw_edges or (tgt, src) in selected_raw_edges
    ]

    net = Network(height="700px", width="100%", directed=True, notebook=False)
    net.set_options(json.dumps({
        "nodes": {"size": 18, "font": {"size": 14, "multi": "html"}},
        "edges": {
            "arrows": {"to": {"enabled": True}},
            "font": {"size": 12, "align": "middle", "multi": "html"},
            "smooth": {
                "type": "cubicBezier",
                "forceDirection": "horizontal",
                "roundness": 0.5
            }
        },
        "layout": {
            "improvedLayout": True
        },
        "physics": {
            "enabled": True,
            "forceAtlas2Based": {
                "gravitationalConstant": -200,
                "springLength": 300,
                "springConstant": 0.03,
                "avoidOverlap": 1
            },
            "minVelocity": 0.75,
            "solver": "forceAtlas2Based",
            "stabilization": {"iterations": 250}
        },
        "interaction": {
            "navigationButtons": True,
            "keyboard": True,
            "dragNodes": True,
            "zoomView": True
        }
    }, indent=2)))

    for node in filtered_nodes:
        if node and node.lower() != 'none':
            net.add_node(node, label=node)

    for src, tgt, job in selected_edges:
        if all([src, tgt, job]) and all(x.lower() != 'none' for x in [src, tgt, job]):
            net.add_edge(src, tgt, label=job, color="red")

    html_content = ""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
        net.save_graph(tmp_file.name)
        with open(tmp_file.name, 'r', encoding='utf-8') as f:
            html_content = f.read()
        os.unlink(tmp_file.name)

    if html_content:
        components.html(html_content, height=750, scrolling=True)

    if not filtered_nodes:
        st.warning("No connected nodes found for the selected input.")
    else:
        filtered_df = df[df.apply(lambda row: row['source'] in filtered_nodes and row['target'] in filtered_nodes, axis=1)]
        st.subheader("Filtered ETL Mapping Table")
        st.dataframe(filtered_df)

    st.write("✅ App finished rendering.")


if __name__ == "__main__":
    main()
