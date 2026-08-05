@echo off
:: Launcher with no spaces in filename — place this file anywhere with a simple path.
:: Task Scheduler points to THIS file. It then calls the real script via a quoted path.
cmd /k "C:\Users\ryapeng\OneDrive - Vancity\DGO TEAM (internal) - General\Data Governance Office\BI_Automation_Project\run_extract.bat" %*
